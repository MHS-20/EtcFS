package ipc

import (
	"context"
	"errors"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// Synthetic history opcodes for events that never cross the IPC socket, kept
// far above the real wire opcodes (which top out at 35) so a decoder can never
// confuse one for the other. See test/verify/lock.go and generation.go for the
// matching decoders — deliberately separate code reading the same bytes, for
// the same reason the IPC history is decoded independently of the daemon that
// wrote it.
const (
	historyOpLockHold      = 1000
	historyOpGuardedCommit = 1001
)

const (
	lockEventAcquire = 0
	lockEventRelease = 1
)

// etcd operations on the data path are retried a few times before being
// reported to the kernel as an error, so that a leader election or a dropped
// connection during failover shows up as a brief stall rather than an EIO.
const (
	etcdAttempts   = 3
	etcdOpTimeout  = 2 * time.Second
	inodeLockTTL   = 2 * time.Second
	retryBaseDelay = 10 * time.Millisecond
	retryStep      = 40 * time.Millisecond

	// requestTimeout bounds the etcd work behind a single FUSE request.  It is
	// defined in the config package because it constrains what lease TTLs are
	// acceptable, and Parse rejects one that inverts the relationship.
	//
	// One value for every operation class, deliberately.  Metadata reads could
	// justify a tighter bound than lock acquisition, but splitting them is
	// speculative tuning until there is evidence a single ceiling is the wrong
	// shape; the property that matters here is that a bound exists at all.
	requestTimeout = config.RequestTimeout
)

// retryDelay is the pause before the attempt following the given one.
func retryDelay(attempt int) time.Duration {
	return retryBaseDelay + time.Duration(attempt)*retryStep
}

// retry runs fn until it succeeds or the attempt budget is spent, returning
// the last error.
//
// The pause between attempts honours ctx: a request whose deadline has already
// passed used to sit out the full backoff before noticing, holding the FUSE
// request open for a reply it was never going to give.
func retry(ctx context.Context, attempts int, fn func() error) error {
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if permanent(err) {
			return err
		}
		if werr := wait(ctx, retryDelay(attempt)); werr != nil {
			return err
		}
	}
	return err
}

// wait pauses for d, or returns early if ctx is done.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// permanent reports whether an error will still hold on a retry.  A fence is
// permanent by definition — retrying it only delays the EIO the caller is
// going to get anyway, while holding the FUSE request open.
func permanent(err error) bool {
	return errors.Is(err, metadata.ErrFenced) ||
		errors.Is(err, metadata.ErrGuardUnavailable)
}

// retryEtcd runs fn against a fresh bounded context on every attempt.  Each
// attempt gets its own context so that one timed-out call does not poison the
// retries that follow it.
func retryEtcd(ctx context.Context, fn func(context.Context) error) error {
	return retry(ctx, etcdAttempts, func() error {
		actx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
		defer cancel()
		return fn(actx)
	})
}

// retryKV is retryEtcd for callers that treat a total failure as "no data"
// rather than an error, and only want it logged.
func (s *Service) retryKV(ctx context.Context, fn func(context.Context) error) {
	if err := retryEtcd(ctx, fn); err != nil {
		s.log.Warn("etcd KV operation failed after retries", "error", err)
	}
}

// heldLock is an acquired inode lock.
//
// The lock it holds is node-local: the etcd key behind it is cached by the
// lock cache and outlives this hold, so Release is a mutex unlock and costs no
// round trip at all.  See lockcache.go for why the key stays.
type heldLock struct {
	s        *Service
	e        *lockEntry
	mode     metadata.LockMode
	released bool
}

// lockModeByte encodes a LockMode as the single byte the lock history payload
// carries.
func lockModeByte(mode metadata.LockMode) byte {
	if mode == metadata.LockExclusive {
		return 1
	}
	return 0
}

// recordLockEvent appends one endpoint of a lock's hold interval to the
// history, as the interval the operation actually spanned.
//
// The instant a lock changes hands is the revision of one etcd transaction,
// and nothing here observes that instant directly — only the call and the
// return around it.  Recording that surrounding interval says exactly as much
// as is known, and lets the checker place the linearization point anywhere
// inside it.
//
// An earlier version recorded a single point taken after the call returned,
// which claimed a precision this code does not have: it placed the event
// strictly later than it happened, and left no room at all for clock offset
// between hosts, so ordinary skew between two nodes read as two holders of
// one lock.
//
// The interval recorded is the operation's, not the cached etcd key's, which
// is longer at both ends.  A subset of the true hold interval is the safe
// direction to be wrong in for a mutual-exclusion checker: it can only ever
// report overlaps that really happened.
func (l *heldLock) recordLockEvent(kind byte, call, ret time.Time) {
	if l.s == nil || l.s.history == nil {
		return
	}
	var b buf
	b.b = append(b.b, kind, lockModeByte(l.mode))
	b.w64(l.e.ino)
	l.s.history.Record("lock_hold", historyOpLockHold, call, ret, b.b, nil)
}

// Release drops the node-local hold.  Idempotent, so a deferred Release is
// safe next to an explicit one.
func (l *heldLock) Release() {
	if l.released {
		return
	}
	l.released = true
	call := time.Now()
	if l.mode == metadata.LockExclusive {
		l.e.rw.Unlock()
	} else {
		l.e.rw.RUnlock()
	}
	l.recordLockEvent(lockEventRelease, call, time.Now())
}

// lockInode takes a lock on an inode: the node-local exclusion first, then the
// etcd key that excludes the other nodes — which is usually already there,
// cached from an earlier operation on the same inode.
//
// The local wait is bounded rather than blocking. Two threads on one node
// contending for the same inode used to collide in etcd and get EAGAIN after
// the retry budget; keeping that shape means a pathologically slow holder
// still cannot pin a FUSE request open indefinitely.
func (s *Service) lockInode(ctx context.Context, ino uint64, mode metadata.LockMode) (*heldLock, error) {
	e := s.lockEntryFor(ino)

	call := time.Now()
	if err := lockLocal(ctx, e, mode); err != nil {
		return nil, err
	}

	if err := s.ensureLockKey(ctx, e, mode); err != nil {
		unlockLocal(e, mode)
		return nil, err
	}

	held := &heldLock{s: s, e: e, mode: mode}
	held.recordLockEvent(lockEventAcquire, call, time.Now())
	return held, nil
}

// lockLocal takes the entry's node-local lock, giving up rather than waiting
// forever.  sync.RWMutex has no timed acquire, so the budget is spent as
// TryLock attempts on the same backoff every other contended operation uses.
func lockLocal(ctx context.Context, e *lockEntry, mode metadata.LockMode) error {
	return retry(ctx, lockAttempts, func() error {
		if mode == metadata.LockExclusive {
			if e.rw.TryLock() {
				return nil
			}
		} else if e.rw.TryRLock() {
			return nil
		}
		return metadata.ErrConflict
	})
}

func unlockLocal(e *lockEntry, mode metadata.LockMode) {
	if mode == metadata.LockExclusive {
		e.rw.Unlock()
	} else {
		e.rw.RUnlock()
	}
}

// ensureLockKey makes sure etcd carries a lock key for this inode that covers
// the requested mode, acquiring one only when the cache cannot answer.
func (s *Service) ensureLockKey(ctx context.Context, e *lockEntry, mode metadata.LockMode) error {
	e.keyMu.Lock()
	defer e.keyMu.Unlock()

	if e.holder != "" && covers(e.mode, mode) {
		return nil
	}
	// A cached shared key blocks this node's own exclusive acquisition — the
	// comparison behind it rejects any holder, including us — so it goes first.
	// The upgrade is not downgraded afterwards, so a read-modify-write sequence
	// pays this once rather than on every alternation.
	if e.holder != "" {
		s.releaseKeyLocked(e)
	}

	holder, err := s.acquireLockKey(ctx, e.ino, mode)
	if err != nil {
		return err
	}
	e.holder, e.mode = holder, mode
	return nil
}

// acquireLockKey takes the etcd lock key, asking the current holder to yield if
// one is in the way.  The want key is written once per acquisition, not once
// per attempt: a peer needs to be told once, and each repeat is a Raft commit
// against a node already working on the recall.
func (s *Service) acquireLockKey(ctx context.Context, ino uint64, mode metadata.LockMode) (string, error) {
	var holder string
	announced := false

	err := retry(ctx, lockAttempts, func() error {
		actx, cancel := context.WithTimeout(ctx, etcdOpTimeout)
		defer cancel()

		var aerr error
		holder, aerr = s.store.AcquireLock(actx, ino, mode, inodeLockTTL)
		if errors.Is(aerr, metadata.ErrConflict) && !announced {
			announced = true
			if werr := s.store.AnnounceLockWant(actx, ino); werr != nil {
				s.log.Warn("cannot announce a lock request; the holder will not be asked to yield",
					"ino", ino, "error", werr)
			}
		}
		return aerr
	})

	if announced {
		// Off the critical path: the lock is already ours or already lost, and
		// leaving the want key behind would have every peer yield this inode
		// for nothing from here on.
		go func() {
			cctx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
			defer cancel()
			if cerr := s.store.ClearLockWant(cctx, ino); cerr != nil {
				s.log.Warn("lock request not withdrawn; peers will keep yielding this inode",
					"ino", ino, "error", cerr)
			}
		}()
	}
	return holder, err
}

// commitGuarded applies ops in one transaction carrying the caller's
// comparisons plus this node's fencing generation.  It reports whether the
// transaction committed and, separately, whether a fence was the reason it did
// not — a fenced node must not mutate metadata again.  Transient etcd errors
// are retried; a failed guard is not, because a fence is permanent.
//
// The guard itself is applied by the store (see metadata.Store.SetGuard), which
// covers every mutation path rather than only the ones that remember to ask.
// This wrapper keeps the retry policy and the "was it a fence" contract its
// callers are written against.
//
// A caller with no comparisons of its own can treat the two the same way — the
// guard is then the only thing that can reject the transaction — but a caller
// that supplies them must not: a comparison miss is contention it can rebuild
// its proposal from, and a fence is permanent.
func (s *Service) commitGuarded(ctx context.Context, cmps []clientv3.Cmp, ops []clientv3.Op) (committed, fenced bool, err error) {
	call := time.Now()
	// Ensure the generation is resolved before committing, so a first write
	// fails with a real etcd error rather than ErrGuardUnavailable.
	gctx, gcancel := context.WithTimeout(context.Background(), etcdOpTimeout)
	gen, err := s.guardGeneration(gctx)
	gcancel()
	if err != nil {
		return false, false, err
	}

	err = retryEtcd(ctx, func(tctx context.Context) error {
		var terr error
		committed, terr = s.store.Txn(tctx, cmps, ops, nil)
		return terr
	})
	fenced = errors.Is(err, metadata.ErrFenced)
	s.recordGuardedCommit(gen, committed, fenced, call, time.Now())
	if fenced {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	return committed, false, nil
}

// recordGuardedCommit appends one guarded-commit attempt to the history: the
// generation this node believed it held, and whether the transaction
// committed. See test/verify/generation.go — the property it checks is that
// no commit succeeds once one has been rejected for a fence, which is the
// single most safety-critical invariant this codebase has.
func (s *Service) recordGuardedCommit(gen uint64, committed, fenced bool, call, ret time.Time) {
	if s.history == nil {
		return
	}
	var b buf
	b.w64(gen)
	flag := byte(0)
	if committed {
		flag |= 1
	}
	if fenced {
		flag |= 2
	}
	b.b = append(b.b, flag)
	s.history.Record("guarded_commit", historyOpGuardedCommit, call, ret, b.b, nil)
}
