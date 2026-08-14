package ipc

import (
	"context"
	"sync"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/arena"
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
	lease      clientv3.LeaseID // the session lease holder was written under
	acquiredAt time.Time        // when holder was taken, for minHoldTime

	// meta is the inode's record and extent list as last read or written by
	// this node, and metaFor is the holder token they were read under.  A
	// cached lock takes the etcd round trip off an operation; without this the
	// metadata read is what is left on the data path, and a read that owns the
	// lock would still cost one.
	//
	// Validity is exactly "this node has held the key continuously since the
	// read": while the key is held no peer can write the inode, so nothing the
	// snapshot describes can have changed underneath.  Every path that gives
	// the key up clears metaFor with it (releaseKeyLocked), and a re-acquired
	// key carries a new holder token, so a snapshot from before a recall can
	// never be mistaken for a current one.
	//
	// Guarded by keyMu, which every operation already takes once through
	// ensureLockKey.  The value is shared with concurrent shared-lock holders
	// and must be treated as immutable — a mutation publishes a new one.
	meta    *inodeMeta
	metaFor string

	// dataStreakEnd is the device offset the buffered write data currently runs
	// up to, so the next write can tell whether it continues that range.  See
	// streakContinues.  Guarded by keyMu.
	dataStreakEnd uint64

	// pending is the metadata this node's writes have produced and not yet
	// published.  It lives beside meta, under the same mutex and the same
	// validity rule, because it is the other half of one statement about the
	// inode: meta says what this node believes the file is, and pending says
	// which part of that belief etcd has not been told yet.  See delegate.go.
	pending *pending

	lastUsed time.Time // guarded by Service.lockMu
}

// hasPending reports whether an inode has writes this node has acknowledged
// and not yet published.  Takes no lock on the entry itself, so it is safe on
// a path that must not block behind an operation in flight.
func (s *Service) hasPending(ino uint64) bool {
	s.lockMu.Lock()
	e := s.locks[ino]
	s.lockMu.Unlock()
	if e == nil {
		return false
	}
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	return !e.pending.empty()
}

// pendingSize returns an inode's size as this node's own unpublished writes
// have left it.
//
// Only consulted when writes are actually deferred: without it a write
// followed by a stat on the same node reports the size etcd still carries,
// which is the size before the write.  A peer's stat still reads etcd and lags
// by up to the flush interval, which is the delegation's cost.
func (s *Service) pendingSize(ino uint64) (uint64, bool) {
	s.lockMu.Lock()
	e := s.locks[ino]
	s.lockMu.Unlock()
	if e == nil {
		return 0, false
	}
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	if e.pending.empty() || e.holder == "" || e.metaFor != e.holder || e.meta == nil || e.meta.rec == nil {
		return 0, false
	}
	return e.meta.rec.Size, true
}

// streakContinues reports whether runs continue the contiguous device range the
// buffer has been accumulating, and records where they end either way.
//
// Buffering a write's *data* only pays when the flush can merge it with its
// neighbours into a larger device I/O.  A provisioned volume rate-limits I/O
// operations rather than capping how many may be outstanding, so a batch of
// scattered 4 K writes costs the device exactly what the same writes cost issued
// one at a time — deferring them converts steady latency into a burst and buys
// nothing back.  Merging is what actually reduces the count, and merging needs
// adjacency, which is what sequential allocation gives and a random overwrite
// workload — reallocating from the scattered holes its own reclaims leave —
// never does.
//
// So the cheapest possible test decides it: does this write's first run start
// where the last one ended.  A write that fails it is written through, exactly
// as it was before data buffering existed; its extent is still deferred, which
// is where the Raft commit was saved and that saving is unconditional.
func (e *lockEntry) streakContinues(runs []arena.Run) bool {
	if len(runs) == 0 {
		return false
	}
	e.keyMu.Lock()
	defer e.keyMu.Unlock()

	continues := runs[0].DiskOff == e.dataStreakEnd
	last := runs[len(runs)-1]
	e.dataStreakEnd = last.DiskOff + last.Length
	return continues
}

// bufferedReadAt serves a device range from this node's own unpublished write
// data, reporting false when the range is not wholly buffered.
func (s *Service) bufferedReadAt(e *lockEntry, dst []byte, diskOff uint64) bool {
	if !s.dataCache {
		return false
	}
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	return e.pending.readAt(dst, diskOff)
}

// inodeMeta is an inode's metadata as of one revision: the record and the
// extent list, which every data-path operation needs together.  Immutable once
// published into a lockEntry.
type inodeMeta struct {
	rec     *metadata.InodeRecord
	extents []metadata.Extent
}

// cachedMeta returns the metadata cached under the currently held key, or nil
// when there is none to trust.
func (e *lockEntry) cachedMeta() *inodeMeta {
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	if e.holder == "" || e.metaFor != e.holder {
		return nil
	}
	return e.meta
}

// setMeta publishes metadata read or written under the currently held key.
// A no-op when no key is held: there would be nothing keeping it true.
func (e *lockEntry) setMeta(m *inodeMeta) {
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	if e.holder == "" {
		return
	}
	e.meta, e.metaFor = m, e.holder
}

// dropMeta forgets the cached metadata, leaving the lock itself alone.
func (e *lockEntry) dropMeta() {
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	e.meta, e.metaFor = nil, ""
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
		if err := s.dropCachedLock(victim, "eviction"); err != nil {
			// Its writes are not published, so its lock cannot be given up.
			// Leaving it in the cache keeps the buffer reachable by the flush
			// interval; the cache runs over its bound until then, which the
			// bound already tolerates.
			victim.rw.Unlock()
			return
		}
		delete(s.locks, oldest)
		victim.rw.Unlock()
	}
}

// dropCachedLock publishes anything this node has buffered for the inode and
// then deletes its etcd key.  The caller must hold the entry's write lock, so
// no operation is running under the lock being dropped.
//
// The flush comes first and its failure aborts the drop.  Yielding the key with
// writes still buffered would let a peer read a file missing the writes this
// node has already acknowledged, and the flush can never succeed afterwards —
// its own comparison on the lock key would reject it.  Refusing to yield makes
// the peer wait, which is the safe direction to fail in.
func (s *Service) dropCachedLock(e *lockEntry, trigger string) error {
	e.keyMu.Lock()
	defer e.keyMu.Unlock()

	if !e.pending.empty() {
		// A context of its own, for the same reason the release below has one:
		// this runs off whatever request last touched the inode, and may run
		// with none at all.
		ctx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
		err := s.flushLocked(ctx, e, trigger)
		cancel()
		if err != nil && !e.pending.empty() {
			return err
		}
	}
	return s.releaseKeyLocked(e)
}

// releaseKeyLocked deletes an entry's etcd key with keyMu already held.
//
// Every cached copy of the inode goes with that key, and this is the single
// place the obligation is discharged: recall, eviction, an upgrade from shared
// to exclusive, a lost session and shutdown all release the key through here.
// There are three such copies — the metadata snapshot, anything still buffered,
// and the kernel's data pages — and the last of those is the only one this
// process cannot simply drop, so it is done first and its failure aborts the
// release.  Yielding the key with pages still cached would hide the next
// holder's writes behind them for good, since a page cache has no timeout.
func (s *Service) releaseKeyLocked(e *lockEntry) error {
	if err := s.invalidatePages(e.ino); err != nil {
		s.log.Error("kernel pages not invalidated, so this node's lock is not yielded",
			"ino", e.ino, "error", err)
		return err
	}

	holder, mode := e.holder, e.mode
	e.holder, e.lease = "", 0
	e.meta, e.metaFor = nil, ""
	// Every caller is expected to have published or discarded its buffer before
	// reaching here.  Reaching it with one still standing is a bug, and the only
	// thing left to do about it is give the blocks back rather than leak them.
	if !e.pending.empty() {
		s.discardPending(e, "the lock key was released with writes still buffered")
	}
	if holder == "" {
		return nil
	}

	// A context of its own: a release has to happen even when the request that
	// last used the lock has run out of time, or the key stands until the node
	// exits and every peer stalls on it.
	ctx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
	defer cancel()
	call := time.Now()
	err := retryEtcd(ctx, func(rctx context.Context) error {
		return s.store.ReleaseLock(rctx, e.ino, mode, holder)
	})
	s.recordKeyEvent(e.ino, mode, lockEventRelease, call, time.Now())
	if err != nil {
		s.log.Error("cached inode lock not released, it will block peers until this node exits",
			"ino", e.ino, "mode", mode, "error", err)
	}
	return nil
}

// keyLostLocked drops everything cached under a key etcd has already deleted:
// the session went, or a flush found the key gone.  Nothing can be refused here
// — the key is not ours to hold on to any more, and a peer may already own the
// inode — so the kernel pages are dropped on a best-effort basis and a failure
// is reported rather than acted on.  This is the one window the page cache
// cannot fully close, and it is the same window the metadata snapshot has: the
// gap between a lease expiring in etcd and this node observing it.
func (s *Service) keyLostLocked(e *lockEntry, why string) {
	if err := s.invalidatePages(e.ino); err != nil {
		s.log.Error("kernel pages not invalidated for an inode whose lock key is gone; "+
			"a reader on this node may see stale data until the file is reopened",
			"ino", e.ino, "error", err)
	}
	if e.holder != "" {
		// etcd dropped the key at some unobserved instant between this node
		// taking it and noticing it was gone, so that whole span is what the
		// history is told.  Claiming the release happened now would place it
		// after a peer's acquisition that legitimately came first.
		s.recordKeyEvent(e.ino, e.mode, lockEventRelease, e.acquiredAt, time.Now())
	}
	e.holder, e.lease, e.meta, e.metaFor = "", 0, nil, ""
	if !e.pending.empty() {
		s.discardPending(e, why)
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
	if err := s.dropCachedLock(e, "recall"); err != nil {
		s.log.Error("cached inode lock not yielded: this node's writes to it are not published",
			"ino", ino, "error", err)
		return
	}
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
		if err := s.dropCachedLock(e, "shutdown"); err != nil {
			// Last chance: the process is exiting, so a buffer kept here dies
			// with it either way, and the blocks are better returned to the
			// arena than left allocated for the next incarnation to reconstruct.
			e.keyMu.Lock()
			s.discardPending(e, "this node is shutting down and the writes could not be published")
			// Last chance, so a failure here is reported and not obeyed: the
			// process is going away and its kernel pages with it.
			if rerr := s.releaseKeyLocked(e); rerr != nil {
				s.log.Error("cached inode lock not released on shutdown",
					"ino", e.ino, "error", rerr)
			}
			e.keyMu.Unlock()
		}
		e.rw.Unlock()
	}
}
