package metadata

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
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

// lockSession returns the lease every lock on this node is written under,
// granting it on first use.
//
// A session that has ended — its lease expired while the node was partitioned,
// or it was revoked — is replaced rather than reused.  Expiry has already
// deleted every lock key written under it, which is the guarantee that matters:
// a node that stopped renewing holds nothing, exactly as when each lock carried
// its own lease.
//
// ttl fixes the session's TTL on the acquisition that creates it and is ignored
// afterwards, since one lease now backs every lock the store takes.
func (s *Store) lockSession(ttl time.Duration) (*concurrency.Session, error) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	if s.session != nil {
		select {
		case <-s.session.Done():
			s.session = nil
		default:
			return s.session, nil
		}
	}

	// Rounded up: etcd's lease TTL is whole seconds, and truncating a
	// sub-second TTL to zero would be rejected outright.
	seconds := int((ttl + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	// The session's keepalive deliberately runs on the client's own context
	// rather than any caller's: a keepalive bound to a request context would
	// stop renewing when that request ended, and the lock would silently expire
	// at its TTL while the holder still believed it held it — precisely the
	// stale-holder situation locking exists to prevent.
	sess, err := concurrency.NewSession(s.client, concurrency.WithTTL(seconds))
	if err != nil {
		return nil, err
	}
	s.session = sess
	return sess, nil
}

// AcquireLock takes a lock on an inode and returns the holder token that
// releases it.
//
// An exclusive lock is blocked by any holder.  A shared lock is blocked only by
// an exclusive one, so any number of readers hold it at once — each under its
// own key, which is what lets one of them release without dropping the lock for
// the rest.
//
// All of a node's locks share one session lease, so an acquisition is a single
// transaction: the per-lock GrantLease and RevokeLease that used to bracket
// every write were two Raft commits on the critical path, and the lease TTL is
// what releases a dead holder's lock whether that lease was granted once or
// once per write.  This is the shape of an NFSv4 write delegation, with the
// delegation scoped to the lock rather than to the open — a lock still spans a
// single operation, so no waiter can be made to wait on another node's close.
func (s *Store) AcquireLock(ctx context.Context, ino uint64, mode LockMode, ttl time.Duration) (string, error) {
	if mode != LockShared && mode != LockExclusive {
		return "", fmt.Errorf("acquire lock ino %d: unknown mode %q (%w)", ino, mode, ErrInvalid)
	}

	sess, err := s.lockSession(ttl)
	if err != nil {
		return "", fmt.Errorf("acquire lock ino %d: session: %w", ino, err)
	}

	// The lease is shared, so it cannot identify a holder on its own: two
	// concurrent readers on this node would write the same key and one
	// would release the other's lock.
	holder := fmt.Sprintf("%d-%d", sess.Lease(), s.lockSeq.Add(1))

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

	op := clientv3.OpPut(LockKey(ino, mode, holder), s.nodeID, clientv3.WithLease(sess.Lease()))

	ok, err := s.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{op}, nil)
	if err != nil {
		return "", fmt.Errorf("acquire lock ino %d: %w", ino, err)
	}
	if !ok {
		return "", fmt.Errorf("acquire lock ino %d: %w", ino, ErrConflict)
	}

	return holder, nil
}

// ReleaseLock drops one holder's lock.
//
// Deleting that holder's key and only that key leaves a shared lock standing
// with its remaining holders — and, now that the lease is shared by every lock
// the node holds, a delete is the only release that does not drop all of them.
func (s *Store) ReleaseLock(ctx context.Context, ino uint64, mode LockMode, holder string) error {
	if err := s.Delete(ctx, LockKey(ino, mode, holder)); err != nil {
		return fmt.Errorf("release lock ino %d: %w", ino, err)
	}
	return nil
}

// CloseLockSession ends the node's lock session, revoking the lease and with it
// any lock key still outstanding.  Idempotent.
func (s *Store) CloseLockSession() error {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	if s.session == nil {
		return nil
	}
	err := s.session.Close()
	s.session = nil
	return err
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
