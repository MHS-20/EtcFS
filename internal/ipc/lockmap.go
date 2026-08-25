package ipc

import (
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
// The eviction victim is dropped through the callback rather than here, because
// dropping a lock publishes whatever the entry has buffered and can fail — and
// a failure has to leave the entry in the set.
type lockMap struct {
	mu      sync.Mutex
	entries map[uint64]*lockEntry

	// drop publishes an entry's buffer and gives up its etcd key. It runs with
	// the entry's write lock held and the set's mutex held.
	drop func(e *lockEntry, trigger string) error
}

func newLockMap(drop func(*lockEntry, string) error) *lockMap {
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

// evictLocked drops cached locks until the set has room, oldest first.
// Only an entry no operation currently holds can go, so a full set of busy
// inodes grows past the bound rather than blocking; the bound is a target, not
// an invariant.
//
// ponytail: eviction is a linear sweep of the map, which is fine at this size
// and against how rarely it runs.  An LRU list is the upgrade if the inode
// fan-out of a real workload ever makes the sweep hot.
func (m *lockMap) evictLocked() {
	for len(m.entries) >= lockCacheMax {
		var oldest uint64
		var victim *lockEntry
		for ino, e := range m.entries {
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
		if err := m.drop(victim, "eviction"); err != nil {
			// Its writes are not published, so its lock cannot be given up.
			// Leaving it in the set keeps the buffer reachable by the flush
			// interval; the set runs over its bound until then, which the bound
			// already tolerates.
			victim.rw.Unlock()
			return
		}
		delete(m.entries, oldest)
		victim.rw.Unlock()
	}
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
