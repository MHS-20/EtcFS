package ipc

import (
	"context"
	"sync"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// Inode lock caching.
//
// An inode lock used to be acquired and released in etcd around every single
// operation, which put two Raft commits on the critical path of a write and
// one on the critical path of a read.  At etcd's measured ~2.2ms per commit
// that alone set the filesystem's IOPS ceiling, and no amount of provisioned
// device IOPS moved it — a serial chain of commits is latency-bound, and
// provisioning buys parallelism.
//
// So a lock key now outlives the operation that took it.  The node keeps it in
// etcd and reuses it for every later operation on the same inode, which costs
// nothing: a repeat acquisition is a map lookup.  What the operation still
// takes, every time, is a node-local lock — the exclusion between this node's
// own threads that the etcd key used to provide as a side effect.
//
// A cached key is under no lease that will expire while the node lives, so a
// peer blocked on it cannot simply wait.  It writes a want key instead
// (metadata.AnnounceLockWant), and StartLockRevocation below drops it in
// response.  This is a write delegation in the NFSv4 sense, and it has that
// design's trade: uncontended access is free, and contention costs a round
// trip plus the recall latency.

const (
	// lockCacheMax bounds the number of inodes whose locks are kept.  Every
	// cached lock is a key held in etcd and a peer's potential recall, so the
	// cache is not free to leave unbounded even ignoring memory.
	//
	// ponytail: eviction is a linear sweep of the map, which is fine at this
	// size and against how rarely it runs.  An LRU list is the upgrade if the
	// inode fan-out of a real workload ever makes the sweep hot.
	lockCacheMax = 4096

	// lockAttempts is the acquisition budget for a lock that a peer holds.  It
	// is larger than the general etcd retry budget because the wait is now for
	// another node to notice a want key and yield, not merely for an operation
	// to finish.
	lockAttempts = 6

	// minHoldTime is how long a freshly acquired lock is kept before a peer's
	// recall is honoured.  Without it, sustained contention on one inode costs a
	// recall and a want key per operation — two extra commits where the
	// per-operation acquire this cache replaced cost one, so the cache would
	// make the contended case worse than the case it was built to fix.
	//
	// This is GFS2's gl_hold_time and it makes the same trade: a bounded extra
	// wait for the peer, in exchange for a bound on how often a lock can change
	// hands.  It costs nothing when uncontended, since nothing recalls.
	minHoldTime = 10 * time.Millisecond
)

// lockEntry is one inode's cached lock.
//
// rw is the node-local exclusion the operation actually takes: RLock for a
// shared lock, Lock for an exclusive one.  keyMu guards the etcd side, which
// concurrent readers holding rw.RLock may both find missing and race to
// create.
type lockEntry struct {
	ino uint64
	rw  sync.RWMutex

	keyMu      sync.Mutex
	mode       metadata.LockMode // meaningful only while holder is set
	holder     string
	acquiredAt time.Time // when holder was taken, for minHoldTime

	lastUsed time.Time // guarded by Service.lockMu
}

// covers reports whether a lock already held in mode satisfies a request for
// want.  An exclusive lock covers a shared request: it excludes every peer the
// shared lock would have, so a read under it is at least as safe.  Keeping it
// rather than downgrading is also what stops a read-modify-write workload
// flapping the key between modes, one commit each way.
func covers(held, want metadata.LockMode) bool {
	return held == metadata.LockExclusive || held == want
}

// lockEntryFor returns the cache entry for an inode, creating it if needed.
func (s *Service) lockEntryFor(ino uint64) *lockEntry {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	e := s.locks[ino]
	if e == nil {
		s.evictLocksLocked()
		e = &lockEntry{ino: ino}
		s.locks[ino] = e
	}
	e.lastUsed = time.Now()
	return e
}

// isCurrent reports whether e is still the cache's entry for its inode.  An
// operation that has taken e's local lock must check this before proceeding:
// an entry evicted out from under it excludes nothing, because the next caller
// builds a fresh entry and takes a different mutex.
func (s *Service) isCurrent(e *lockEntry) bool {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	return s.locks[e.ino] == e
}

// evictLocksLocked drops cached locks until the cache has room, oldest first.
// Only an entry no operation currently holds can go, so a full cache of busy
// inodes grows past the bound rather than blocking; the bound is a target, not
// an invariant.
func (s *Service) evictLocksLocked() {
	for len(s.locks) >= lockCacheMax {
		var oldest uint64
		var victim *lockEntry
		for ino, e := range s.locks {
			// TryLock rather than Lock: an entry with an operation in flight is
			// skipped, never waited for.  Evicting it would let the next caller
			// build a second entry for the same inode and run alongside the
			// operation this one is excluding.
			if !e.rw.TryLock() {
				continue
			}
			if victim == nil || e.lastUsed.Before(victim.lastUsed) {
				if victim != nil {
					victim.rw.Unlock()
				}
				oldest, victim = ino, e
				continue
			}
			e.rw.Unlock()
		}
		if victim == nil {
			return
		}
		delete(s.locks, oldest)
		s.dropCachedLock(victim)
		victim.rw.Unlock()
	}
}

// dropCachedLock deletes an entry's etcd key.  The caller must hold the
// entry's write lock, so no operation is running under the lock being dropped.
func (s *Service) dropCachedLock(e *lockEntry) {
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	s.releaseKeyLocked(e)
}

// releaseKeyLocked deletes an entry's etcd key with keyMu already held.
func (s *Service) releaseKeyLocked(e *lockEntry) {
	holder, mode := e.holder, e.mode
	e.holder = ""
	if holder == "" {
		return
	}

	// A context of its own: a release has to happen even when the request that
	// last used the lock has run out of time, or the key stands until the node
	// exits and every peer stalls on it.
	ctx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
	defer cancel()
	if err := retryEtcd(ctx, func(rctx context.Context) error {
		return s.store.ReleaseLock(rctx, e.ino, mode, holder)
	}); err != nil {
		s.log.Error("cached inode lock not released, it will block peers until this node exits",
			"ino", e.ino, "mode", mode, "error", err)
	}
}

// StartLockRevocation serves peers' requests for locks this node has cached.
//
// One watch for the whole cluster: an event names an inode, and a node with no
// cached lock on it ignores the event.  Recalls are handled one at a time,
// which is bounded by how long an operation can hold an inode (requestTimeout)
// and keeps the loop free of a goroutine per event.
func (s *Service) StartLockRevocation(ctx context.Context) {
	ch := s.store.WatchLockWants(ctx)
	go func() {
		for resp := range ch {
			for _, ev := range resp.Events {
				if ev.Type != mvccpb.PUT { // a withdrawn want recalls nothing
					continue
				}
				ino, node, ok := metadata.ParseLockWantKey(string(ev.Kv.Key))
				if !ok || node == s.store.NodeID() {
					continue
				}
				s.recallLock(ino)
			}
		}
	}()
}

// recallLock yields a cached lock to a peer that has asked for it — the
// blocking-AST half of the delegation.
//
// The entry stays in the cache: only the etcd key is given up, the way a GFS2
// glock is demoted rather than destroyed.  Removing the entry would leave any
// operation currently running under it holding a mutex the next caller no
// longer looks at, and the node-local exclusion that the cached key no longer
// provides would be gone with it.
func (s *Service) recallLock(ino uint64) {
	s.lockMu.Lock()
	e := s.locks[ino]
	s.lockMu.Unlock()
	if e == nil {
		return
	}

	// A lock taken moments ago is held to its minimum before being given up, so
	// that contention on one inode cannot turn every single operation into a
	// recall.  The holder keeps making progress during the wait.
	e.keyMu.Lock()
	held := time.Since(e.acquiredAt)
	e.keyMu.Unlock()
	if held < minHoldTime {
		time.Sleep(minHoldTime - held)
	}

	e.rw.Lock()
	defer e.rw.Unlock()
	s.dropCachedLock(e)
	s.log.Debug("yielded a cached inode lock to a peer", "ino", ino)
}

// ReleaseCachedLocks drops every lock this node is holding on to.  Called on
// shutdown, ahead of ending the lock session: closing the session revokes the
// lease and would clear the keys anyway, but only after this node has stopped
// answering, and a peer blocked on one of them should not have to wait for
// that.
func (s *Service) ReleaseCachedLocks() {
	s.lockMu.Lock()
	entries := make([]*lockEntry, 0, len(s.locks))
	for ino, e := range s.locks {
		entries = append(entries, e)
		delete(s.locks, ino)
	}
	s.lockMu.Unlock()

	for _, e := range entries {
		e.rw.Lock()
		s.dropCachedLock(e)
		e.rw.Unlock()
	}
}
