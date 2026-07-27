// Package ipc implements the gRPC service that the C FUSE daemon calls.
//
// The C daemon (etcfuse) opens a Unix domain socket to this service and
// sends protobuf requests for each FUSE operation.  This service translates
// those requests into etcd operations and returns protobuf responses.
//
// Phase 0: stub implementation — returns minimal responses so the C daemon
//
//	can mount and serve a filesystem.  Full implementation follows
//	in Phase 1 (etcd schema) and Phase 2 (FUSE read ops).
package ipc

import (
	"context"

	"google.golang.org/grpc"

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

// Register registers the gRPC service on the given server.
func (s *Service) Register(srv *grpc.Server) {
	// In Phase 0: no protobuf-generated service registration yet.
	// The C daemon uses a raw binary protocol (see pkg/fuse/ops.c).
	// Phase 1: replace with generated gRPC service.
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "ipc.EtcFSMeta",
		HandlerType: (*interface{})(nil),
		Methods:     []grpc.MethodDesc{},
		Streams:     []grpc.StreamDesc{},
	}, nil)
}

// ---- Metadata operation stubs (Phase 1+) ----

// Lookup resolves a directory entry to an inode.
func (s *Service) Lookup(ctx context.Context, parent uint64, name string) (ino uint64, attrs *metadata.InodeRecord, err error) {
	// Phase 0: always return ENOENT — only the root inode (1) is valid.
	// Phase 1: query etcd for dirent:<parent>/<name>, then inode:<result>.
	if parent == 1 && name == "." {
		return 1, &metadata.InodeRecord{
			Ino:     1,
			Size:    4096,
			Mode:    0755 | uint32(1<<31), // S_IFDIR
			Nlink:   2,
			Blksize: 4096,
		}, nil
	}
	return 0, nil, nil
}

// Getattr returns attributes for an inode.
func (s *Service) Getattr(ctx context.Context, ino uint64) (*metadata.InodeRecord, error) {
	// Phase 0: only the root inode has attributes.
	if ino == 1 {
		return &metadata.InodeRecord{
			Ino:     1,
			Size:    4096,
			Mode:    0755 | uint32(1<<31),
			Nlink:   2,
			Blksize: 4096,
		}, nil
	}
	return nil, nil
}

// Store returns the underlying metadata store.
func (s *Service) Store() *metadata.Store {
	return s.store
}

// Logger returns the structured logger.
func (s *Service) Logger() *config.Logger {
	return s.log
}

// IsFenced returns true if self-fencing has triggered.
func (s *Service) IsFenced() bool {
	return s.watchdog.IsFenced()
}
