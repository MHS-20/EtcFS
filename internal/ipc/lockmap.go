package ipc

import (
	"sort"
	"sync"
	"time"
)

// lockMap is the set of inodes this node has a cached lock entry for.
//
// Only the membership of that set lives here — which inode has which entry,
// which entry is the current one, and which entry to evict when the set is
// full. What a cached lock *means* (when its key may be given up, what must be
// published first, what a recall has to assert) stays on the Service in
// lockcache.go: that is the protocol, and it is not a map operation.
//
// The eviction victims are dropped through the callback rather than here,
// because dropping a lock publishes whatever the entry has buffered and can
// fail — and a failure has to leave the entry in the set.
type lockMap struct {
	mu      sync.Mutex
	entries map[uint64]*lockEntry

	// drop publishes the entries' buffers and gives up their etcd keys, in one
	// transaction, returning those whose keys are gone. It runs with each
	// entry's write lock held and the set's mutex held.
	drop func(es []*lockEntry, trigger string) []*lockEntry
}

func newLockMap(drop func([]*lockEntry, string) []*lockEntry) *lockMap {
	return &lockMap{entries: make(map[uint64]*lockEntry), drop: drop}
}

// entryFor returns the entry for an inode, creating it if needed.
func (m *lockMap) entryFor(ino uint64) *lockEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := m.entries[ino]
	if e == nil {
		m.evictLocked()
		e = &lockEntry{ino: ino}
		m.entries[ino] = e
	}
	e.lastUsed = time.Now()
	return e
}

// lookup returns the entry for an inode, or nil if it has none.
func (m *lockMap) lookup(ino uint64) *lockEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries[ino]
}

// isCurrent reports whether e is still the set's entry for its inode.  An
// operation that has taken e's local lock must check this before proceeding:
// an entry evicted out from under it excludes nothing, because the next caller
// builds a fresh entry and takes a different mutex.
func (m *lockMap) isCurrent(e *lockEntry) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries[e.ino] == e
}

// all returns the current entries, for the sweeps that walk every one of them.
// A snapshot rather than a held mutex: the sweeps take entry locks, and holding
// the set's mutex across that would block every acquisition behind them.
func (m *lockMap) all() []*lockEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := make([]*lockEntry, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	return entries
}

// drain empties the set and returns what it held.
func (m *lockMap) drain() []*lockEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := make([]*lockEntry, 0, len(m.entries))
	for ino, e := range m.entries {
		entries = append(entries, e)
		delete(m.entries, ino)
	}
	return entries
}

// evictLocked makes room in the set by giving up its least recently used
// locks, a batch at a time.
//
// A batch rather than one victim, because giving a key up is a Raft commit and
// one transaction of lockEvictBatch deletes costs what one delete costs.  A
// workload with a far larger working set than the cache — an unpacking archive
// touches 80,000 inodes against 4,096 entries — otherwise evicts one inode per
// new one and pays that commit per file.  Between sweeps the set sits under its
// bound by up to a batch, which is what the bound already tolerates in the
// other direction.
//
// Only an entry no operation currently holds can go, so a full set of busy
// inodes grows past the bound rather than blocking; the bound is a target, not
// an invariant.
func (m *lockMap) evictLocked() {
	if len(m.entries) < lockCacheMax {
		return
	}
	victims := m.claimVictimsLocked(lockEvictBatch)
	if len(victims) == 0 {
		return
	}
	// Entries whose keys could not be given up — their writes are unpublished,
	// or their pages are not invalidated — are not returned and stay in the set
	// with their buffers reachable by the flush interval.
	for _, e := range m.drop(victims, "eviction") {
		delete(m.entries, e.ino)
	}
	for _, e := range victims {
		e.rw.Unlock()
	}
}

// claimVictimsLocked takes the write lock on up to n of the least recently used
// entries and returns them, oldest first.
//
// The candidates are ordered before any of them is locked, so the sweep costs
// one pass and one sort of the set rather than a pass per victim — which is
// what a batch of evictions used to cost, one linear scan each.
//
// TryLock rather than Lock: an entry with an operation in flight is skipped,
// never waited for.  Evicting it would let the next caller build a second entry
// for the same inode and run alongside the operation this one is excluding.
func (m *lockMap) claimVictimsLocked(n int) []*lockEntry {
	candidates := make([]*lockEntry, 0, len(m.entries))
	for _, e := range m.entries {
		candidates = append(candidates, e)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastUsed.Before(candidates[j].lastUsed)
	})

	victims := make([]*lockEntry, 0, n)
	for _, e := range candidates {
		if len(victims) == n {
			break
		}
		if e.rw.TryLock() {
			victims = append(victims, e)
		}
	}
	return victims
}

// recallSet names the inodes with a recall already in flight, so that a burst
// of want events for one inode starts one yield rather than a race between
// several for the same key.
type recallSet struct {
	mu  sync.Mutex
	set map[uint64]bool
}

func newRecallSet() *recallSet { return &recallSet{set: make(map[uint64]bool)} }

// begin claims the right to recall an inode, reporting false if one is already
// in flight.
func (r *recallSet) begin(ino uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.set[ino] {
		return false
	}
	r.set[ino] = true
	return true
}

func (r *recallSet) end(ino uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.set, ino)
}
