// Package ipc implements the binary IPC server that the C FUSE daemon calls.
//
// The C daemon (etcfuse) opens a Unix domain socket to this service and
// sends binary-framed requests for each FUSE operation.  This service
// translates those requests into etcd operations and returns binary responses.
package ipc

import (
	"context"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/internal/history"
	"github.com/MHS-20/EtcFS/pkg/arena"
	"github.com/MHS-20/EtcFS/pkg/blockio"
	"github.com/MHS-20/EtcFS/pkg/fencing"
	"github.com/MHS-20/EtcFS/pkg/metadata"
	"github.com/MHS-20/EtcFS/pkg/metrics"
)

// Service handles FUSE operation requests from the C daemon.
type Service struct {
	store        *metadata.Store
	membership   *metadata.Membership
	watchdog     *fencing.Watchdog
	alloc        *arena.Allocator
	log          *config.Logger
	dev          *blockio.Device
	notifyServer *notifyServer

	// writeBarriers adds a device flush, a range sync and a readback to every
	// write, and a device flush to every read.  See writeRun.
	writeBarriers bool

	// flushInterval is how long an inode's extents may stay buffered in RAM
	// before they are published to etcd.  Zero disables deferral entirely, so
	// every write commits before it is acknowledged.  See delegate.go.
	flushInterval time.Duration
	// flushMaxBytes caps the write payload one buffer may stand for, so a hot
	// inode cannot turn an unbounded amount of acknowledged data into data that
	// a crash would lose.  With dataCache on it is also the bound on how much
	// RAM one inode's buffered payload may occupy, and the backpressure: a write
	// past the cap waits for the flush that makes room for it.
	flushMaxBytes uint64

	// dataCache buffers a deferred write's payload in RAM alongside its extents
	// instead of putting it on the device as the write is served, so a write
	// costs no device I/O either.  The flush writes the bytes before it
	// publishes the extents naming them, which is the ordering that keeps a lost
	// write a lost write rather than a read of garbage.
	dataCache bool

	// readOnly rejects every mutating opcode with EROFS before it reaches a
	// handler. Checked in dispatch rather than per-handler so a new mutating
	// operation is safe by default: it must be added to mutatingOps to be
	// servable at all, at which point read-only coverage is a one-line review
	// rather than a call site to remember.
	readOnly bool

	// history records every served operation for offline consistency checking.
	// Nil unless --history-log was given, and a nil recorder records nothing.
	history *history.Recorder

	// openFiles counts this node's open descriptors per inode, so an unlink of
	// the last name can keep the record alive until the last one closes.
	openMu    sync.Mutex
	openCount map[uint64]int
	// orphaned holds inodes this node unlinked while it still had them open:
	// the last release deletes the record.
	orphaned map[uint64]bool

	// locks caches this node's inode locks past the operations that took
	// them, so a repeat acquisition costs no etcd round trip. See lockcache.go.
	lockMu sync.Mutex
	locks  map[uint64]*lockEntry

	// Fencing generation this node started with.  Every data-path commit is
	// guarded against it, so once the fencing controller bumps gen:<node_id>
	// this node's commits stop being accepted by etcd.
	genMu    sync.Mutex
	genInit  bool
	startGen uint64
}

// NewService creates a Service.
func NewService(store *metadata.Store, membership *metadata.Membership,
	watchdog *fencing.Watchdog, log *config.Logger) *Service {
	return &Service{
		store:         store,
		membership:    membership,
		watchdog:      watchdog,
		alloc:         arena.NewAllocator(membership.NodeID(), store),
		log:           log,
		openCount:     make(map[uint64]int),
		orphaned:      make(map[uint64]bool),
		locks:         make(map[uint64]*lockEntry),
		flushInterval: defaultFlushInterval,
		flushMaxBytes: defaultFlushMaxBytes,
	}
}

// SetFlushInterval bounds how long an acknowledged write may stay unpublished.
// Zero commits every write before acknowledging it, which is the behaviour
// before write delegation: nothing is ever lost by a crash, and every write
// carries a Raft commit.
func (s *Service) SetFlushInterval(d time.Duration) {
	s.flushInterval = d
}

// SetDataCache buffers a deferred write's payload in RAM as well as its extents.
// Off restores a device write per write, which is the configuration that loses
// nothing beyond what deferring the commit already loses.
func (s *Service) SetDataCache(on bool) {
	s.dataCache = on
}

// SetBlockDevice attaches a block device for data I/O.
//
// barriers is forced on for a device opened without O_DIRECT: there the bytes
// really do sit in this node's page cache until something pushes them out.
func (s *Service) SetBlockDevice(dev *blockio.Device, barriers bool) {
	s.dev = dev
	s.writeBarriers = barriers || !dev.IsDirect()
	// The allocator hands out arenas by multiplying an ID by the arena size,
	// with nothing else to stop it running past the end of the device.
	s.alloc.SetDeviceSize(uint64(dev.TotalSize()))
}

// ReclaimOrphans deletes the inodes a previous incarnation of this node was
// keeping alive for open descriptors that died with it. Nothing names them and
// no descriptor survives a restart, so they are pure leak from here on.
func (s *Service) ReclaimOrphans(ctx context.Context) {
	inos, err := s.store.ListOrphans(ctx, s.membership.NodeID())
	if err != nil {
		s.log.Warn("cannot list orphaned inodes", "error", err)
		return
	}
	for _, ino := range inos {
		if err := s.store.DeleteOrphan(ctx, ino); err != nil {
			s.log.Warn("cannot delete orphaned inode", "ino", ino, "error", err)
			continue
		}
		s.log.Info("reclaimed an inode left open by a previous run", "ino", ino)
	}
}

// SetHistoryRecorder attaches a recorder that logs every served operation.
func (s *Service) SetHistoryRecorder(r *history.Recorder) {
	s.history = r
}

// SetReadOnly rejects every mutating opcode with EROFS. Intended for mounting
// a filesystem for backup or inspection while another node writes, and for
// giving fsck a safe way to run against a live volume.
func (s *Service) SetReadOnly(ro bool) {
	s.readOnly = ro
}

// ReconstructArenas rebuilds the arena free-list from existing extents in etcd.
func (s *Service) ReconstructArenas(ctx context.Context) error {
	return s.alloc.Reconstruct(ctx)
}

// Allocator returns this node's block allocator, so background passes (the
// scrubber's orphan reclamation) can return disk ranges to it.
func (s *Service) Allocator() *arena.Allocator {
	return s.alloc
}

func (s *Service) FreeBlock(diskOff, length uint64) {
	s.alloc.Free(diskOff, length)
}

// Store returns the underlying metadata store.
func (s *Service) Store() *metadata.Store {
	return s.store
}

// IsFenced returns true if self-fencing has triggered.
func (s *Service) IsFenced() bool {
	return s.watchdog != nil && s.watchdog.IsFenced()
}

// InitGeneration ensures this node's gen:<node_id> key exists and caches the
// generation the node starts with.  Idempotent, and safe to retry after a
// transient etcd failure.
func (s *Service) InitGeneration(ctx context.Context) error {
	s.genMu.Lock()
	defer s.genMu.Unlock()
	return s.initGenerationLocked(ctx)
}

// InstallStoreGuard makes every store transaction carry this node's fencing
// generation, so namespace mutations are rejected once the node is fenced —
// not just extent writes.
//
// The guard reports unavailable rather than falling back to generation 0 when
// initialisation has not happened yet: a wrong guard value is worse than no
// transaction, since generation 0 would compare successfully on a node that
// has never been fenced and mask the missing initialisation.
func (s *Service) InstallStoreGuard() {
	s.store.SetGuard(func() (clientv3.Cmp, uint64, bool) {
		s.genMu.Lock()
		defer s.genMu.Unlock()
		if !s.genInit {
			return clientv3.Cmp{}, 0, false
		}
		return metadata.WithGenerationGuard(s.membership.NodeID(), s.startGen), s.startGen, true
	})
}

func (s *Service) initGenerationLocked(ctx context.Context) error {
	if s.genInit {
		return nil
	}
	gen, err := s.store.EnsureGenerationKey(ctx, s.membership.NodeID())
	if err != nil {
		return err
	}
	s.startGen = gen
	s.genInit = true
	metrics.FencingGeneration.Set(float64(gen))
	return nil
}

// guardGeneration returns the generation that data-path transactions must be
// guarded against, initialising it on first use if startup initialisation was
// skipped or failed.
func (s *Service) guardGeneration(ctx context.Context) (uint64, error) {
	s.genMu.Lock()
	defer s.genMu.Unlock()
	if err := s.initGenerationLocked(ctx); err != nil {
		return 0, err
	}
	return s.startGen, nil
}
