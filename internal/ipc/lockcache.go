package ipc

import (
	"context"
	"sync"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/arena"
	"github.com/MHS-20/EtcFS/pkg/metadata"
	"github.com/MHS-20/EtcFS/pkg/metrics"
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
	// cache is not free to leave unbounded even ignoring memory.  What happens
	// when it is reached is lockMap.evictLocked.
	lockCacheMax = 4096

	// entryRetries bounds how many times an acquisition restarts because the
	// cache entry was evicted from under it.  This is a race with the evictor
	// and not a wait for anyone, so a handful of attempts is plenty: each one
	// re-reads the map and takes a different mutex.
	entryRetries = 6

	// contendedAttempts is the budget for waiting out a contended inode,
	// whether the holder is a thread on this node or a peer that has to be
	// recalled first.  Both are the same wait in the end — this node cannot
	// have the inode until whoever holds it lets go — so both get the same
	// budget.
	//
	// It was six attempts, which is 450ms, and a contended chaos run showed why
	// that is the wrong number: a plain cross-node `cat > file` failed with EIO
	// while requestTimeout still had 95% of its budget left.  Contention is
	// supposed to make a write wait, not fail.  The real bound is the request
	// deadline — retry aborts on it — and this only has to be large enough to
	// reach it: the delays are 10ms + 40ms per attempt, so 22 of them sum past
	// requestTimeout.
	contendedAttempts = 22

	// minHoldTime is how long a freshly acquired lock is kept before a peer's
	// recall is honoured, and the floor the adaptive hold below decays back to.
	// Without it, sustained contention on one inode costs a recall and a want
	// key per operation — two extra commits where the per-operation acquire this
	// cache replaced cost one, so the cache would make the contended case worse
	// than the case it was built to fix.
	//
	// This is GFS2's gl_hold_time and it makes the same trade: a bounded extra
	// wait for the peer, in exchange for a bound on how often a lock can change
	// hands.  It costs nothing when uncontended, since nothing recalls.
	minHoldTime = 10 * time.Millisecond

	// maxHoldTime caps how far that hold can grow under sustained contention.
	//
	// A fixed floor answers a recall that arrives moments after an acquisition,
	// but not a workload where every handover does — six nodes writing disjoint
	// ranges of one file, say, where the inode changes hands continuously and
	// each turn buys one operation before the next recall.  There the hold has
	// to grow, so a node amortises the handover over several operations instead
	// of paying a round trip per turn.
	//
	// What the growth spends is the waiter's latency, so the ceiling is what
	// keeps that honest: a peer blocked on an inode waits at most this long per
	// handover, against an acquisition budget that runs to the request deadline.
	// It is also the flush interval, which a recall already has to wait out.
	maxHoldTime = 100 * time.Millisecond
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
	acquiredAt time.Time        // when holder was taken, for the hold below

	// holdTime is how long this inode's lock is kept before a recall is
	// honoured.  It grows while handovers keep arriving inside the current hold
	// and decays when they stop, so an inode nobody contends for costs nothing
	// and one that is fought over stops changing hands on every operation.
	// Zero means it has never been contended, and reads as minHoldTime.
	holdTime time.Duration

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

	lastUsed time.Time // guarded by lockMap.mu
}

// hasPending reports whether an inode has writes this node has acknowledged
// and not yet published.  Takes no lock on the entry itself, so it is safe on
// a path that must not block behind an operation in flight.
func (s *Service) hasPending(ino uint64) bool {
	e := s.locks.lookup(ino)
	if e == nil {
		return false
	}
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	return !e.pending.empty()
}

// holdsLockKey reports whether this node currently holds an inode's lock key in
// etcd, which is the only case in which a change to that inode's record cannot
// have come from a peer.
//
// Distinct from Service.Holds, which asks the weaker question the scrubber
// needs — whether an entry exists at all.  Here the entry existing is not
// enough: a released or recalled entry stays in the cache, and an inode this
// node no longer holds is exactly one a peer may be writing.
func (s *Service) holdsLockKey(ino uint64) bool {
	e := s.locks.lookup(ino)
	if e == nil {
		return false
	}
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	return e.holder != ""
}

// pendingSize returns an inode's size as this node's own unpublished writes
// have left it.
//
// Only consulted when writes are actually deferred: without it a write
// followed by a stat on the same node reports the size etcd still carries,
// which is the size before the write.  A peer's stat still reads etcd and lags
// by up to the flush interval, which is the delegation's cost.
func (s *Service) pendingSize(ino uint64) (uint64, bool) {
	e := s.locks.lookup(ino)
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
	var wasHeld bool
	err := retryEtcd(ctx, func(rctx context.Context) error {
		var rerr error
		wasHeld, rerr = s.store.ReleaseLock(rctx, e.ino, mode, holder)
		return rerr
	})
	// A key that was already gone was dropped by the lease at an instant this
	// node never saw, so the honest statement is that the hold ended somewhere
	// between acquiring it and noticing — the same span keyLostLocked records,
	// and for the same reason.  Timing the release from now instead would
	// claim the inode right through the window a peer legitimately owned it,
	// which is a mutual-exclusion violation in the history and not in the
	// filesystem.  A lost response on a retry lands here too, and widening the
	// interval is the safe direction to be wrong in: it weakens this node's
	// claim rather than inventing one.
	widened := call
	if !wasHeld {
		widened = e.acquiredAt
	}
	s.recordKeyEvent(e.ino, mode, lockEventRelease, widened, time.Now(), call)
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
		s.recordKeyEvent(e.ino, e.mode, lockEventRelease, e.acquiredAt, time.Now(), time.Now())
	}
	e.holder, e.lease, e.meta, e.metaFor = "", 0, nil, ""
	if !e.pending.empty() {
		s.discardPending(e, why)
	}
}

// StartLockRevocation serves peers' requests for locks this node has cached.
//
// One watch for the whole cluster: an event names an inode, and a node with no
// cached lock on it ignores the event.
//
// Recalls run one *per inode*, not one at a time.  Serialising the whole loop
// was simpler and is what a recall storm showed to be wrong: each recall waits
// out minHoldTime and then a flush, so a queue of them made unrelated inodes
// wait on each other, and peers blocked on inodes nobody was slow about
// exhausted their acquisition budget and took an EIO.  Concurrency is bounded
// by the number of distinct contended inodes rather than by the event rate —
// an inode already being recalled ignores further events, since the one in
// flight is what the peer is waiting for and starting a second would race it
// for the same key.
func (s *Service) StartLockRevocation(ctx context.Context) {
	go s.runWatch(ctx, watcher{
		what:   "lock request",
		prefix: metadata.PrefixLockWant,
		event:  s.lockWanted,
	})
}

// lockWanted starts a recall for a peer's request, if one is not already
// running for that inode.
func (s *Service) lockWanted(ev *clientv3.Event) {
	if ev.Type != mvccpb.PUT { // a withdrawn want recalls nothing
		return
	}
	ino, node, ok := metadata.ParseLockWantKey(string(ev.Kv.Key))
	if !ok || node == s.store.NodeID() {
		return
	}
	if !s.recalls.begin(ino) {
		return
	}
	go func() {
		defer s.recalls.end(ino)
		s.recallLock(ino)
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
	e := s.locks.lookup(ino)
	if e == nil {
		return
	}

	// A lock taken moments ago is held to its minimum before being given up, so
	// that contention on one inode cannot turn every single operation into a
	// recall.  The holder keeps making progress during the wait.
	held, hold := e.handoverHold()
	if held < hold {
		time.Sleep(hold - held)
	}
	metrics.LockHandoverHold.Observe(hold.Seconds())

	if err := s.yieldCachedLock(ino, "recall"); err != nil {
		s.log.Error("cached inode lock not yielded: this node's writes to it are not published",
			"ino", ino, "error", err)
		return
	}
	s.log.Debug("yielded a cached inode lock to a peer", "ino", ino)
}

// handoverHold records a peer's request for this inode and returns how long the
// lock has been held and how long it must be held before it is given up.
//
// The hold doubles when the request arrives inside the current one and halves
// when it arrives after it, between the floor and the ceiling above. That is
// the whole of the adaptation, and each half of it answers a real workload:
// doubling is what stops six writers to one file paying a round trip per
// operation, and halving is what stops an inode that was contended once from
// making every later peer wait for a contention that has ended.
//
// Measured from the acquisition rather than from the last recall, because that
// is the quantity the hold is trading against — how many of this node's own
// operations one turn of the lock is worth.
func (e *lockEntry) handoverHold() (held, hold time.Duration) {
	e.keyMu.Lock()
	defer e.keyMu.Unlock()

	held = time.Since(e.acquiredAt)
	hold = max(e.holdTime, minHoldTime)
	if held < hold {
		hold = min(hold*2, maxHoldTime)
	} else {
		hold = max(hold/2, minHoldTime)
	}
	e.holdTime = hold
	return held, hold
}

// yieldCachedLock publishes whatever this node has buffered for an inode and
// gives up its cached lock key, so the next node to want the inode finds it
// free instead of having to ask for it back.
//
// Holding the entry's write lock is what makes the drop safe: no operation of
// this node's can be running under the key while it is being given up.  An
// inode this node holds no key for is nothing to yield, and succeeds.
func (s *Service) yieldCachedLock(ino uint64, trigger string) error {
	e := s.locks.lookup(ino)
	if e == nil {
		return nil
	}
	e.rw.Lock()
	defer e.rw.Unlock()
	return s.dropCachedLock(e, trigger)
}

// sessionPollInterval is how often the session watcher looks for a session to
// watch when this node has taken no lock yet.  Nothing is at stake while there
// is no session — there are no keys and no caches under one — so this only has
// to be short enough that the watch is in place well before the first lock's
// lease could expire.
const sessionPollInterval = 500 * time.Millisecond

// StartSessionWatch drops every cache standing behind this node's lock keys as
// soon as the session those keys were written under ends.
//
// Without it the loss is noticed only by the next operation to touch each
// inode (ensureLockKey), and until then the node serves reads from a metadata
// snapshot and kernel pages whose lock a peer may already hold, and keeps
// acknowledging writes into a buffer whose flush is certain to be rejected.
// The keys are gone the instant the lease expires, so the caches behind them
// are worthless from that instant too, and every millisecond of late detection
// is more acknowledged data that will have to be discarded.
func (s *Service) StartSessionWatch(ctx context.Context) {
	go func() {
		for {
			lease, done, ok := s.store.LockSessionWatch()
			if !ok {
				select {
				case <-ctx.Done():
					return
				case <-time.After(sessionPollInterval):
				}
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-done:
				s.dropCachesForLease(lease)
			}
		}
	}()
}

// dropCachesForLease discards everything cached under one dead session lease.
//
// Scoped by lease rather than applied to the whole cache: by the time this
// runs, an operation may already have granted a fresh session and re-acquired
// an inode under it, and that entry's key is live.  Entries are left in the
// map, as a recall leaves them — only what the key vouched for is dropped.
func (s *Service) dropCachesForLease(lease clientv3.LeaseID) {
	entries := s.locks.all()

	dropped := 0
	for _, e := range entries {
		e.keyMu.Lock()
		if e.holder != "" && e.lease == lease {
			s.keyLostLocked(e, "this node's lock session ended")
			dropped++
		}
		e.keyMu.Unlock()
	}
	if dropped > 0 {
		s.log.Error("lock session ended, so every cache behind this node's locks was dropped",
			"lease", lease, "inodes", dropped)
	}
}

// ReleaseCachedLocks drops every lock this node is holding on to.  Called on
// shutdown, ahead of ending the lock session: closing the session revokes the
// lease and would clear the keys anyway, but only after this node has stopped
// answering, and a peer blocked on one of them should not have to wait for
// that.
func (s *Service) ReleaseCachedLocks() {
	entries := s.locks.drain()

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
