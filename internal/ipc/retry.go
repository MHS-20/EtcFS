package ipc

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

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
)

// retryDelay is the pause before the attempt following the given one.
func retryDelay(attempt int) time.Duration {
	return retryBaseDelay + time.Duration(attempt)*retryStep
}

// retry runs fn until it succeeds or the attempt budget is spent, returning
// the last error.
func retry(attempts int, fn func() error) error {
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		time.Sleep(retryDelay(attempt))
	}
	return err
}

// retryEtcd runs fn against a fresh bounded context on every attempt.  Each
// attempt gets its own context so that one timed-out call does not poison the
// retries that follow it.
func retryEtcd(fn func(context.Context) error) error {
	return retry(etcdAttempts, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
		defer cancel()
		return fn(ctx)
	})
}

// retryKV is retryEtcd for callers that treat a total failure as "no data"
// rather than an error, and only want it logged.
func (s *Service) retryKV(fn func(context.Context) error) {
	if err := retryEtcd(fn); err != nil {
		s.log.Warn("etcd KV operation failed after retries", "error", err)
	}
}

// lockInode takes a lease-backed lock on an inode and returns the release
// function.  The lease keepalive stream is drained for the lifetime of the
// lock — the lease expires on its own once the lock is released, so nothing
// is left running past the release.
func (s *Service) lockInode(ctx context.Context, ino uint64, mode metadata.LockMode) (release func(), err error) {
	var leaseID clientv3.LeaseID
	var keepCh <-chan *clientv3.LeaseKeepAliveResponse

	err = retry(etcdAttempts, func() error {
		var aerr error
		leaseID, keepCh, aerr = s.store.AcquireLock(ctx, ino, mode, inodeLockTTL)
		return aerr
	})
	if err != nil {
		return nil, err
	}

	go func() {
		for range keepCh {
		}
	}()

	return func() { _ = s.store.ReleaseLock(ctx, ino, leaseID) }, nil
}

// commitGuarded applies ops in one transaction guarded by this node's fencing
// generation.  Returns (false, nil) when the guard rejected the commit — the
// node has been fenced and must not mutate metadata again.  Transient etcd
// errors are retried; a failed guard is not, because a fence is permanent.
func (s *Service) commitGuarded(ops []clientv3.Op) (bool, error) {
	gctx, gcancel := context.WithTimeout(context.Background(), etcdOpTimeout)
	gen, err := s.guardGeneration(gctx)
	gcancel()
	if err != nil {
		return false, err
	}

	guard := []clientv3.Cmp{metadata.WithGenerationGuard(s.membership.NodeID(), gen)}

	committed := false
	err = retryEtcd(func(ctx context.Context) error {
		var terr error
		committed, terr = s.store.Txn(ctx, guard, ops, nil)
		return terr
	})
	if err != nil {
		return false, err
	}
	return committed, nil
}
