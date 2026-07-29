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
	return s.watchdog.IsFenced()
}
