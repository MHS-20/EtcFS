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

// AcquireLock attempts to acquire a lock on an inode.
//
// For exclusive locks: CAS — succeed only if no lock key exists.
// For shared locks:   CAS — succeed if no exclusive lock exists.
//
// Returns the lease ID that backs the lock and a keepalive channel.
// The caller must receive from keepaliveCh to keep the lock alive.
//
// ctx bounds the acquisition RPCs only.  It deliberately does not bound the
// keepalive stream — see the comment at the KeepAlive call below — so a
// caller may pass a context with a deadline without the resulting lock
// quietly expiring once that deadline passes.
func (s *Store) AcquireLock(ctx context.Context, ino uint64, mode LockMode, ttl time.Duration) (clientv3.LeaseID, <-chan *clientv3.LeaseKeepAliveResponse, error) {
	leaseID, err := s.GrantLease(ctx, ttl)
	if err != nil {
		return 0, nil, fmt.Errorf("acquire lock ino %d: grant lease: %w", ino, err)
	}

	value := fmt.Sprintf(`{"mode":"%s","holders":["%s"]}`, mode, s.nodeID)

	var cmps []clientv3.Cmp

	switch mode {
	case LockExclusive:
		cmps = []clientv3.Cmp{
			clientv3.Compare(clientv3.CreateRevision(LockKey(ino)), "=", 0),
		}
	case LockShared:
		existing, _ := s.GetLockInfo(ctx, ino)
		if existing != nil && existing.Mode == string(LockShared) {
			existingVal, _ := s.Get(ctx, LockKey(ino))
			existing.Holders = append(existing.Holders, s.nodeID)
			holdersJSON := ""
			for j, h := range existing.Holders {
				if j > 0 {
					holdersJSON += ","
				}
				holdersJSON += `"` + h + `"`
			}
			value = fmt.Sprintf(`{"mode":"%s","holders":[%s]}`, LockShared, holdersJSON)
			if existingVal != nil {
				cmps = []clientv3.Cmp{
					clientv3.Compare(clientv3.Value(LockKey(ino)), "=", string(existingVal)),
				}
			}
		} else {
			cmps = []clientv3.Cmp{
				clientv3.Compare(clientv3.CreateRevision(LockKey(ino)), "=", 0),
			}
		}
	}

	op := clientv3.OpPut(LockKey(ino), value, clientv3.WithLease(leaseID))

	ok, err := s.Txn(ctx, cmps, []clientv3.Op{op}, nil)
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

// ReleaseLock releases a lock on an inode.
func (s *Store) ReleaseLock(ctx context.Context, ino uint64, leaseID clientv3.LeaseID) error {
	if err := s.RevokeLease(ctx, leaseID); err != nil {
		return fmt.Errorf("release lock ino %d: %w", ino, err)
	}
	return nil
}

// GetLockInfo returns the current lock state for an inode.
// Returns nil if no lock is held.
func (s *Store) GetLockInfo(ctx context.Context, ino uint64) (*LockRecord, error) {
	value, err := s.Get(ctx, LockKey(ino))
	if err != nil {
		return nil, fmt.Errorf("get lock ino %d: %w", ino, err)
	}
	if value == nil {
		return nil, nil
	}

	// Simple JSON parsing — full struct in Phase 3.
	var rec LockRecord
	_, _ = fmt.Sscanf(string(value), `{"mode":"%s"}`, &rec.Mode)
	if rec.Mode == "" {
		rec.Mode = "unknown"
	}
	return &rec, nil
}

// IsLocked returns true if any lock is held on the inode.
func (s *Store) IsLocked(ctx context.Context, ino uint64) (bool, error) {
	rec, err := s.GetLockInfo(ctx, ino)
	if err != nil {
		return false, err
	}
	return rec != nil, nil
}

// WatchLock watches the lock key for an inode.  Returns a channel that
// delivers a value when the lock state changes (acquired, released, expired).
func (s *Store) WatchLock(ctx context.Context, ino uint64) clientv3.WatchChan {
	return s.Watch(ctx, LockKey(ino))
}
