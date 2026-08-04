// Package ipc implements the binary IPC server that the C FUSE daemon calls.
//
// The C daemon (etcfuse) opens a Unix domain socket to this service and
// sends binary-framed requests for each FUSE operation.  This service
// translates those requests into etcd operations and returns binary responses.
//
// Phase 2: read-only FUSE ops (LOOKUP, GETATTR, READDIR, READLINK, STATFS).
package ipc

import (
	"context"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/pkg/arena"
	"github.com/MHS-20/EtcFS/pkg/blockio"
	"github.com/MHS-20/EtcFS/pkg/fencing"
	"github.com/MHS-20/EtcFS/pkg/metadata"
	wal "github.com/MHS-20/EtcFS/pkg/walgo"
)

// Service handles FUSE operation requests from the C daemon.
type Service struct {
	store        *metadata.Store
	membership   *metadata.Membership
	watchdog     *fencing.Watchdog
	alloc        *arena.Allocator
	log          *config.Logger
	dev          *blockio.Device
	wal          *wal.WAL
	notifyServer *notifyServer

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
		store:      store,
		membership: membership,
		watchdog:   watchdog,
		alloc:      arena.NewAllocator(membership.NodeID(), store),
		log:        log,
	}
}

// SetBlockDevice attaches a block device for data I/O.
func (s *Service) SetBlockDevice(dev *blockio.Device) {
	s.dev = dev
}

// ReconstructArenas rebuilds the arena free-list from existing extents in etcd.
func (s *Service) ReconstructArenas(ctx context.Context) error {
	return s.alloc.Reconstruct(ctx)
}

func (s *Service) SetWAL(w *wal.WAL) {
	s.wal = w
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
