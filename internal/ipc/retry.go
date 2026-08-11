package ipc

import (
	"context"
	"errors"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/pkg/metadata"
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
// Release drops it in a round trip of its own.  A caller that is already
// committing a transaction can instead put ReleaseOp into it and call Folded
// once that transaction commits, which releases the lock at the same revision
// as the work it was protecting and saves the round trip entirely.  Release
// stays safe to defer either way: after Folded it does nothing.
type heldLock struct {
	s        *Service
	ino      uint64
	mode     metadata.LockMode
	holder   string
	released bool
}

// ReleaseOp is the deletion that drops this lock, for folding into a caller's
// own transaction.
func (l *heldLock) ReleaseOp() clientv3.Op {
	return clientv3.OpDelete(metadata.LockKey(l.ino, l.mode, l.holder))
}

// Folded records that a transaction carrying ReleaseOp committed, so the
// deferred Release becomes a no-op.  Only call it once the transaction is
// known to have committed — a rejected one released nothing.
func (l *heldLock) Folded() { l.released = true }

// Release drops the lock, unless a committed transaction already carried it.
func (l *heldLock) Release() {
	if l.released {
		return
	}
	// Fresh context rather than the request's: releasing has to work even
	// when the request deadline has already expired, otherwise the lock
	// lingers and blocks the next writer for no reason.  Retried, because a
	// lock key now outlives a failed release for as long as the node does:
	// the session lease that would have expired it is the one this node
	// keeps renewing.
	rctx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
	defer cancel()
	err := retryEtcd(rctx, func(dctx context.Context) error {
		return l.s.store.ReleaseLock(dctx, l.ino, l.mode, l.holder)
	})
	if err != nil {
		l.s.log.Error("inode lock not released, it will block writers until this node exits",
			"ino", l.ino, "mode", l.mode, "error", err)
		return
	}
	l.released = true
}

// lockInode takes a lock on an inode.  The lock is written under the store's
// session lease, which is renewed for the life of the process, so nothing
// per-lock is left running past the release.
//
// Each acquisition attempt runs against its own bounded context.  This is the
// first etcd call on both the read and the write path, so an unbounded one
// stalls I/O here, before any generation guard is consulted; the caller's ctx
// still caps the total, so a request that has already run out of time does not
// start another attempt.
func (s *Service) lockInode(ctx context.Context, ino uint64, mode metadata.LockMode) (*heldLock, error) {
	var holder string

	err := retry(ctx, etcdAttempts, func() error {
		actx, cancel := context.WithTimeout(ctx, etcdOpTimeout)
		defer cancel()
		var aerr error
		holder, aerr = s.store.AcquireLock(actx, ino, mode, inodeLockTTL)
		return aerr
	})
	if err != nil {
		return nil, err
	}

	return &heldLock{s: s, ino: ino, mode: mode, holder: holder}, nil
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
	// Ensure the generation is resolved before committing, so a first write
	// fails with a real etcd error rather than ErrGuardUnavailable.
	gctx, gcancel := context.WithTimeout(context.Background(), etcdOpTimeout)
	_, err = s.guardGeneration(gctx)
	gcancel()
	if err != nil {
		return false, false, err
	}

	err = retryEtcd(ctx, func(tctx context.Context) error {
		var terr error
		committed, terr = s.store.Txn(tctx, cmps, ops, nil)
		return terr
	})
	if errors.Is(err, metadata.ErrFenced) {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	return committed, false, nil
}
