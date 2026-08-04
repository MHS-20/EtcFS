package metadata

import (
	"context"
	"fmt"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// GuardFunc supplies the fencing-generation comparison that every structural
// mutation must carry.  It returns the comparison, the generation that
// comparison encodes, and true once the node's generation is known; false
// means the guard is not yet available (generation not initialised) and the
// transaction must not proceed.
//
// It is a function rather than a stored Cmp because the generation is resolved
// lazily, on first use, by the owning service.  The generation is returned
// alongside the Cmp so a failed transaction can be classified without
// unpacking the comparison's protobuf representation.
type GuardFunc func() (cmp clientv3.Cmp, gen uint64, ok bool)

// Store is the primary metadata facade.  It wraps the etcd client and
// provides schema-aware operations (inode CRUD, dirent mutation, locking,
// fencing generation).  All structural mutations go through this type.
type Store struct {
	client *clientv3.Client
	nodeID string

	// guard, when set, is prepended to the comparisons of every Txn.  A nil
	// guard leaves transactions unguarded — correct only for control-plane
	// stores (see SetGuard).
	guard GuardFunc
}

// NewStore creates a Store backed by the given etcd client.
//
// The returned Store is unguarded.  Callers serving filesystem requests must
// call SetGuard before serving, or a fenced node's namespace mutations will be
// accepted.
func NewStore(client *clientv3.Client, nodeID string) *Store {
	return &Store{
		client: client,
		nodeID: nodeID,
	}
}

// SetGuard installs the fencing-generation guard applied to every Txn.
//
// The guard is deliberately opt-out rather than opt-in: the failure mode being
// designed against is a new mutation path forgetting to guard itself, which is
// exactly how namespace operations went unguarded while the helper existed.
// Paths that must bypass it call txnRaw explicitly — there are only three
// (see EnsureGenerationKey, BumpGeneration, and bootstrap membership
// registration), and each is unguarded for a reason documented at the call.
func (s *Store) SetGuard(g GuardFunc) {
	s.guard = g
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

// Txn executes a transaction guarded by this node's fencing generation.
// The caller provides comparison ops, success ops, and failure ops.  Returns
// true if the transaction succeeded (all comparisons matched and success ops
// were applied).
//
// When the transaction fails, the guard is re-checked to tell a fence apart
// from an ordinary CAS miss: a guard failure returns ErrFenced, because the
// two demand opposite responses.  A CAS miss is retryable contention; a fence
// is permanent and the caller must stop mutating metadata.
func (s *Store) Txn(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, error) {
	guarded := ifs
	if s.guard != nil {
		cmp, _, ok := s.guard()
		if !ok {
			return false, fmt.Errorf("txn: %w", ErrGuardUnavailable)
		}
		// Prepend so the guard is evaluated with the caller's comparisons in
		// one atomic evaluation, not as a separate round trip that could race
		// a fence landing in between.
		guarded = append([]clientv3.Cmp{cmp}, ifs...)
	}

	ok, err := s.txnRaw(ctx, guarded, thens, elses)
	if err != nil {
		return false, err
	}
	if !ok && s.guard != nil {
		if fenced, ferr := s.guardFailed(ctx); ferr == nil && fenced {
			return false, fmt.Errorf("txn: %w", ErrFenced)
		}
	}
	return ok, nil
}

// txnRaw executes a transaction without the fencing guard.
//
// Only for control-plane paths that cannot be guarded: creating the generation
// key the guard compares against, bumping a node's generation (which must not
// be guarded by the generation it is changing), and bootstrap membership
// registration that runs before the generation is known.  Everything else must
// use Txn.
func (s *Store) txnRaw(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, error) {
	resp, err := s.client.Txn(ctx).If(ifs...).Then(thens...).Else(elses...).Commit()
	if err != nil {
		return false, fmt.Errorf("txn: %w", err)
	}
	return resp.Succeeded, nil
}

// guardFailed reports whether the installed guard no longer matches the stored
// generation — that is, whether this node has been fenced since it started.
func (s *Store) guardFailed(ctx context.Context) (bool, error) {
	_, expected, ok := s.guard()
	if !ok {
		return false, nil
	}
	current, err := s.GetGeneration(ctx, s.nodeID)
	if err != nil {
		return false, err
	}
	return current != expected, nil
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
//
// When a guard is installed the write is issued as a guarded transaction, not
// a bare Put: a fenced node must not be able to mutate metadata through a call
// that happens to skip Txn.  Several namespace handlers (setattr, symlink,
// mknod) and the truncate path write inode records this way.
func (s *Store) Put(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error) {
	if s.guard != nil {
		return s.guardedWrite(ctx, clientv3.OpPut(key, string(value), opts...), "put", key)
	}
	return s.putRaw(ctx, key, value, opts...)
}

// putRaw writes without the fencing guard.  Control-plane use only — see
// txnRaw for the rules.
func (s *Store) putRaw(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error) {
	resp, err := s.client.Put(ctx, key, string(value), opts...)
	if err != nil {
		return 0, fmt.Errorf("put %s: %w", key, err)
	}
	return resp.Header.Revision, nil
}

// guardedWrite applies a single mutation inside a generation-guarded
// transaction, translating a guard rejection into ErrFenced.
func (s *Store) guardedWrite(ctx context.Context, op clientv3.Op, verb, key string) (int64, error) {
	cmp, _, ok := s.guard()
	if !ok {
		return 0, fmt.Errorf("%s %s: %w", verb, key, ErrGuardUnavailable)
	}
	resp, err := s.client.Txn(ctx).If(cmp).Then(op).Commit()
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", verb, key, err)
	}
	if !resp.Succeeded {
		return 0, fmt.Errorf("%s %s: %w", verb, key, ErrFenced)
	}
	return resp.Header.Revision, nil
}

// Delete removes a key.  Guarded when a guard is installed — see Put.
func (s *Store) Delete(ctx context.Context, key string) error {
	if s.guard != nil {
		_, err := s.guardedWrite(ctx, clientv3.OpDelete(key), "delete", key)
		return err
	}
	_, err := s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

// DeletePrefix removes all keys with the given prefix.  Guarded when a guard
// is installed — see Put.
func (s *Store) DeletePrefix(ctx context.Context, prefix string) (int64, error) {
	if s.guard != nil {
		cmp, _, ok := s.guard()
		if !ok {
			return 0, fmt.Errorf("delete prefix %s: %w", prefix, ErrGuardUnavailable)
		}
		resp, err := s.client.Txn(ctx).
			If(cmp).
			Then(clientv3.OpDelete(prefix, clientv3.WithPrefix())).
			Commit()
		if err != nil {
			return 0, fmt.Errorf("delete prefix %s: %w", prefix, err)
		}
		if !resp.Succeeded {
			return 0, fmt.Errorf("delete prefix %s: %w", prefix, ErrFenced)
		}
		return resp.Responses[0].GetResponseDeleteRange().Deleted, nil
	}
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
