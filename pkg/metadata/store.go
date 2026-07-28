package metadata

import (
	"context"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// MetadataStore is the interface for metadata operations.
// Implemented by the real etcd-backed Store and the deterministic mock simulator.
type MetadataStore interface {
	// Key-value
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) (int64, error)
	Delete(ctx context.Context, key string) error
	GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)

	// Transactions
	Txn(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, error)

	// Leases
	GrantLease(ctx context.Context, ttl time.Duration) (clientv3.LeaseID, error)
	KeepAlive(ctx context.Context, leaseID clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	RevokeLease(ctx context.Context, leaseID clientv3.LeaseID) error

	// Watches
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}
