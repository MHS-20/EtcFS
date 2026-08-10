package metadata

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Lock operations: acquire shared/exclusive file locks backed by etcd leases.
//
// A lock is acquired by CAS-inserting a lock:<ino> key with a lease binding.
// If the key already exists with a conflicting mode, the acquire fails.
// Lease expiry (crash, network partition) auto-deletes the lock key.

// LockMode represents the lock type.
type LockMode string

const (
	LockShared    LockMode = "shared"
	LockExclusive LockMode = "exclusive"
)

// ErrConflict is returned when a lock cannot be acquired due to a conflict.
var ErrConflict = fmt.Errorf("lock conflict")

// ErrExists is returned when a resource already exists.
var ErrExists = fmt.Errorf("already exists")

// ErrFenced is returned when a transaction was rejected because this node's
// fencing generation was bumped — the node has been fenced and must not mutate
// shared state again.  Callers must surface this as EIO, never as a
// contention or not-found error: a fence is permanent, and misreporting it
// makes a fencing bug look like ordinary contention.
var ErrFenced = fmt.Errorf("node is fenced")

// ErrGuardUnavailable is returned when a guarded transaction is attempted
// before the fencing generation is known.  Failing closed is deliberate — an
// unguarded mutation is exactly what the guard exists to prevent.
var ErrGuardUnavailable = fmt.Errorf("fencing guard unavailable")

// Namespace errors, each mapping to the errno POSIX names for the situation.
var (
	// ErrNotEmpty: a directory that still has entries cannot be replaced.
	ErrNotEmpty = fmt.Errorf("directory not empty")
	// ErrNotDir: a directory can only be renamed over another directory.
	ErrNotDir = fmt.Errorf("not a directory")
	// ErrIsDir: a non-directory cannot be renamed over a directory.
	ErrIsDir = fmt.Errorf("is a directory")
	// ErrInvalid: the operation is rejected on its own terms — an unsupported
	// flag, or a directory rename that would detach a subtree into a cycle.
	ErrInvalid = fmt.Errorf("invalid argument")
	// ErrNotFound: the name or inode the operation names does not exist.
	ErrNotFound = fmt.Errorf("not found")
	// ErrPerm: the operation is not permitted on this kind of inode, such as a
	// hard link to a directory.
	ErrPerm = fmt.Errorf("operation not permitted")
)

// AcquireLock takes a lock on an inode and returns the lease backing it.
//
// An exclusive lock is blocked by any holder.  A shared lock is blocked only by
// an exclusive one, so any number of readers hold it at once — each under its
// own key and its own lease, which is what lets one of them release without
// dropping the lock for the rest.
//
// The caller must receive from the returned keepalive channel to keep the lock
// alive.
//
// ctx bounds the acquisition RPCs only.  It deliberately does not bound the
// keepalive stream — see the comment at the KeepAlive call below — so a
// caller may pass a context with a deadline without the resulting lock
// quietly expiring once that deadline passes.
func (s *Store) AcquireLock(ctx context.Context, ino uint64, mode LockMode, ttl time.Duration) (clientv3.LeaseID, <-chan *clientv3.LeaseKeepAliveResponse, error) {
	if mode != LockShared && mode != LockExclusive {
		return 0, nil, fmt.Errorf("acquire lock ino %d: unknown mode %q (%w)", ino, mode, ErrInvalid)
	}

	leaseID, err := s.GrantLease(ctx, ttl)
	if err != nil {
		return 0, nil, fmt.Errorf("acquire lock ino %d: grant lease: %w", ino, err)
	}

	// The range that must be empty for this acquisition to be allowed.  Etcd
	// evaluates a comparison over a range as "true for every key in it", and an
	// empty range is vacuously true — so this reads as "no blocking holder
	// exists", decided atomically with the write below rather than in a
	// separate round trip that a competing acquire could slip through.
	blocked := LockPrefix(ino)
	if mode == LockShared {
		blocked = LockModePrefix(ino, LockExclusive)
	}
	cmp := clientv3.Compare(clientv3.CreateRevision(blocked), "=", 0).WithPrefix()

	op := clientv3.OpPut(LockKey(ino, mode, int64(leaseID)), s.nodeID, clientv3.WithLease(leaseID))

	ok, err := s.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{op}, nil)
	if err != nil {
		_ = s.RevokeLease(ctx, leaseID)
		return 0, nil, fmt.Errorf("acquire lock ino %d: %w", ino, err)
	}
	if !ok {
		_ = s.RevokeLease(ctx, leaseID)
		return 0, nil, fmt.Errorf("acquire lock ino %d: %w", ino, ErrConflict)
	}

	// Not ctx: clientv3 ties a keepalive stream's lifetime to the context it
	// is given.  Passing the caller's context here would stop renewing the
	// lease the moment that context is cancelled — the lock would silently
	// expire at its TTL while the holder still believed it held it, which is
	// precisely the stale-holder situation locking exists to prevent.  The
	// stream ends when the lease is revoked (ReleaseLock) or expires.
	keepCh, err := s.KeepAlive(context.Background(), leaseID)
	if err != nil {
		_ = s.ReleaseLock(ctx, ino, leaseID)
		return 0, nil, fmt.Errorf("acquire lock ino %d: keepalive: %w", ino, err)
	}

	return leaseID, keepCh, nil
}

// ReleaseLock drops one holder's lock.
//
// Revoking the lease deletes that holder's key and only that key, so a shared
// lock survives with its remaining holders.
func (s *Store) ReleaseLock(ctx context.Context, ino uint64, leaseID clientv3.LeaseID) error {
	if err := s.RevokeLease(ctx, leaseID); err != nil {
		return fmt.Errorf("release lock ino %d: %w", ino, err)
	}
	return nil
}

// GetLockInfo returns the current lock state for an inode, or nil if it is
// unlocked.  A single exclusive holder makes the whole lock exclusive.
func (s *Store) GetLockInfo(ctx context.Context, ino uint64) (*LockRecord, error) {
	kvs, err := s.GetPrefix(ctx, LockPrefix(ino))
	if err != nil {
		return nil, fmt.Errorf("get lock ino %d: %w", ino, err)
	}
	if len(kvs) == 0 {
		return nil, nil
	}

	rec := &LockRecord{Mode: string(LockShared), Holders: make([]string, 0, len(kvs))}
	for _, kv := range kvs {
		if mode, ok := ParseLockKey(string(kv.Key), ino); ok && mode == LockExclusive {
			rec.Mode = string(LockExclusive)
		}
		rec.Holders = append(rec.Holders, string(kv.Value))
	}
	return rec, nil
}

// IsLocked returns true if any lock is held on the inode.
func (s *Store) IsLocked(ctx context.Context, ino uint64) (bool, error) {
	kvs, err := s.GetPrefix(ctx, LockPrefix(ino))
	if err != nil {
		return false, fmt.Errorf("is locked ino %d: %w", ino, err)
	}
	return len(kvs) > 0, nil
}

// WatchLock watches every holder key for an inode.  The returned channel
// delivers on any change to the lock state: acquired, released, or expired.
func (s *Store) WatchLock(ctx context.Context, ino uint64) clientv3.WatchChan {
	return s.Watch(ctx, LockPrefix(ino), clientv3.WithPrefix())
}
