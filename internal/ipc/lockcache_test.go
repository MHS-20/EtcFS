package ipc

import (
	"testing"
	"time"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// The whole point of caching a lock is that a second acquisition of a mode the
// cached one already satisfies costs no etcd round trip — and that an exclusive
// lock satisfies a read, so a read-modify-write sequence does not flap the key
// between modes at a Raft commit each way.
func TestCoveredLockModes(t *testing.T) {
	cases := []struct {
		held, want metadata.LockMode
		covered    bool
	}{
		{metadata.LockExclusive, metadata.LockExclusive, true},
		{metadata.LockExclusive, metadata.LockShared, true},
		{metadata.LockShared, metadata.LockShared, true},
		{metadata.LockShared, metadata.LockExclusive, false},
	}
	for _, c := range cases {
		if got := covers(c.held, c.want); got != c.covered {
			t.Errorf("covers(%s, %s) = %v, want %v", c.held, c.want, got, c.covered)
		}
	}
}

// A released lock has to leave the entry acquirable again, in either mode.  The
// hold is now a node-local RWMutex rather than an etcd key, so a leaked unlock
// deadlocks every later operation on that inode instead of merely costing a
// round trip.
func TestLocalLockReleaseIsIdempotent(t *testing.T) {
	e := &lockEntry{ino: 7}

	lk := &heldLock{e: e, mode: metadata.LockExclusive}
	e.rw.Lock()

	lk.Release()
	lk.Release() // a deferred Release next to an explicit one must be harmless

	if !e.rw.TryLock() {
		t.Fatal("exclusive lock still held after release")
	}
	e.rw.Unlock()
}

// A shared hold must release as a reader: unlocking it as a writer panics, and
// releasing only one of two concurrent readers must leave the other's hold
// standing.
func TestSharedLocksReleaseIndependently(t *testing.T) {
	e := &lockEntry{ino: 7}

	first := &heldLock{e: e, mode: metadata.LockShared}
	second := &heldLock{e: e, mode: metadata.LockShared}
	e.rw.RLock()
	e.rw.RLock()

	first.Release()
	if e.rw.TryLock() {
		t.Fatal("exclusive lock taken while a reader still holds the inode")
	}

	second.Release()
	if !e.rw.TryLock() {
		t.Fatal("inode still locked after the last reader released")
	}
	e.rw.Unlock()
}

// An eviction must not take a lock out of the cache while an operation is using
// it: the entry is the node-local exclusion, so dropping a busy one would let a
// second operation acquire a fresh entry for the same inode and run alongside.
func TestEvictionSkipsBusyEntries(t *testing.T) {
	s := &Service{locks: make(map[uint64]*lockEntry)}

	// The oldest entry is the one eviction would pick first, and it is busy.
	busy := &lockEntry{ino: 1, lastUsed: time.Unix(0, 0)}
	busy.rw.Lock()
	s.locks[1] = busy
	for ino := uint64(2); ino <= lockCacheMax; ino++ {
		s.locks[ino] = &lockEntry{ino: ino, lastUsed: time.Unix(int64(ino), 0)}
	}

	s.lockMu.Lock()
	s.evictLocksLocked()
	s.lockMu.Unlock()

	if _, ok := s.locks[1]; !ok {
		t.Fatal("an inode with an operation in flight was evicted from the lock cache")
	}
	if len(s.locks) >= lockCacheMax {
		t.Fatalf("cache still at %d entries, eviction made no room", len(s.locks))
	}
	busy.rw.Unlock()
}
