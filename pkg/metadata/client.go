package metadata

import (
	"context"
	"fmt"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Store is the primary metadata facade.  It wraps the etcd client and
// provides schema-aware operations (inode CRUD, dirent mutation, locking,
// fencing generation).  All structural mutations go through this type.
type Store struct {
	client *clientv3.Client
	nodeID string
}

// NewStore creates a Store backed by the given etcd client.
func NewStore(client *clientv3.Client, nodeID string) *Store {
	return &Store{
		client: client,
		nodeID: nodeID,
	}
}

// Client returns the underlying etcd client (for direct use by watch
// multiplexer, fencing watchdog, etc.).
func (s *Store) Client() *clientv3.Client {
	return s.client
}

// NodeID returns this node's identifier.
func (s *Store) NodeID() string {
	return s.nodeID
}

// Txn executes a transaction.  The caller provides comparison ops,
// success ops, and failure ops.  Returns true if the transaction succeeded
// (all comparisons matched and success ops were applied).
func (s *Store) Txn(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, error) {
	resp, err := s.client.Txn(ctx).If(ifs...).Then(thens...).Else(elses...).Commit()
	if err != nil {
		return false, fmt.Errorf("txn: %w", err)
	}
	return resp.Succeeded, nil
}

// Get reads a single key's value.  Returns nil if the key doesn't exist.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

// GetPrefix reads all keys with the given prefix.
func (s *Store) GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("get prefix %s: %w", prefix, err)
	}
	return resp.Kvs, nil
}

// GetRevision reads a key at a specific etcd revision (point-in-time snapshot).
// Useful for consistent paginated directory listings.
func (s *Store) GetRevision(ctx context.Context, key string, opts ...clientv3.OpOption) ([]*mvccpb.KeyValue, int64, error) {
	resp, err := s.client.Get(ctx, key, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("get rev %s: %w", key, err)
	}
	return resp.Kvs, resp.Header.Revision, nil
}

// Put writes a key-value pair.  Returns the new revision.
func (s *Store) Put(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error) {
	resp, err := s.client.Put(ctx, key, string(value), opts...)
	if err != nil {
		return 0, fmt.Errorf("put %s: %w", key, err)
	}
	return resp.Header.Revision, nil
}

// Delete removes a key.
func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

// DeletePrefix removes all keys with the given prefix.
func (s *Store) DeletePrefix(ctx context.Context, prefix string) (int64, error) {
	resp, err := s.client.Delete(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("delete prefix %s: %w", prefix, err)
	}
	return resp.Deleted, nil
}

// GrantLease creates a new lease with the given TTL.
func (s *Store) GrantLease(ctx context.Context, ttl time.Duration) (clientv3.LeaseID, error) {
	resp, err := s.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return 0, fmt.Errorf("grant lease: %w", err)
	}
	return resp.ID, nil
}

// KeepAlive starts a keepalive stream for the given lease.
// The caller must receive from the returned channel to keep the lease alive.
func (s *Store) KeepAlive(ctx context.Context, leaseID clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return s.client.KeepAlive(ctx, leaseID)
}

// RevokeLease immediately terminates a lease.
func (s *Store) RevokeLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	_, err := s.client.Revoke(ctx, leaseID)
	return err
}

// Watch creates a watch on the given key or prefix.
// The returned channel delivers WatchResponses until ctx is cancelled.
func (s *Store) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return s.client.Watch(ctx, key, opts...)
}

// MemberList returns the list of etcd cluster members.
func (s *Store) MemberList(ctx context.Context) (*clientv3.MemberListResponse, error) {
	return s.client.MemberList(ctx)
}

// Status returns the etcd endpoint status.
func (s *Store) Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
	return s.client.Status(ctx, endpoint)
}
