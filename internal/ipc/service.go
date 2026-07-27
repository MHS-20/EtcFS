// Package ipc implements the binary IPC server that the C FUSE daemon calls.
//
// The C daemon (etcfuse) opens a Unix domain socket to this service and
// sends binary-framed requests for each FUSE operation.  This service
// translates those requests into etcd operations and returns binary responses.
//
// Phase 2: read-only FUSE ops (LOOKUP, GETATTR, READDIR, READLINK, STATFS).
package ipc

import (
	"github.com/anomalyco/etcfuse/internal/config"
	"github.com/anomalyco/etcfuse/pkg/fencing"
	"github.com/anomalyco/etcfuse/pkg/metadata"
)

// Service handles FUSE operation requests from the C daemon.
type Service struct {
	store      *metadata.Store
	membership *metadata.Membership
	watchdog   *fencing.Watchdog
	log        *config.Logger
}

// NewService creates a Service.
func NewService(store *metadata.Store, membership *metadata.Membership,
	watchdog *fencing.Watchdog, log *config.Logger) *Service {
	return &Service{
		store:      store,
		membership: membership,
		watchdog:   watchdog,
		log:        log,
	}
}

// Store returns the underlying metadata store.
func (s *Service) Store() *metadata.Store {
	return s.store
}

// IsFenced returns true if self-fencing has triggered.
func (s *Service) IsFenced() bool {
	return s.watchdog.IsFenced()
}
