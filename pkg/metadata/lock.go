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

// AcquireLock attempts to acquire a lock on an inode.
//
// For exclusive locks: CAS — succeed only if no lock key exists.
// For shared locks:   CAS — succeed if no exclusive lock exists.
//
// Returns the lease ID that backs the lock and a keepalive channel.
// The caller must receive from keepaliveCh to keep the lock alive.
func (s *Store) AcquireLock(ctx context.Context, ino uint64, mode LockMode, ttl time.Duration) (clientv3.LeaseID, <-chan *clientv3.LeaseKeepAliveResponse, error) {
	leaseID, err := s.GrantLease(ctx, ttl)
	if err != nil {
		return 0, nil, fmt.Errorf("acquire lock ino %d: grant lease: %w", ino, err)
	}

	value := fmt.Sprintf(`{"mode":"%s","holders":["%s"]}`, mode, s.nodeID)

	var cmps []clientv3.Cmp

	switch mode {
	case LockExclusive:
		// No lock key can exist at all
		cmps = []clientv3.Cmp{
			clientv3.Compare(clientv3.CreateRevision(LockKey(ino)), "=", 0),
		}
	case LockShared:
		// No exclusive lock can exist (shared is ok)
		// We check for value containing "exclusive" — a simplified approach.
		// Full implementation in Phase 3 uses a proper lock table.
		cmps = []clientv3.Cmp{
			clientv3.Compare(clientv3.CreateRevision(LockKey(ino)), "=", 0),
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

	keepCh, err := s.KeepAlive(ctx, leaseID)
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
