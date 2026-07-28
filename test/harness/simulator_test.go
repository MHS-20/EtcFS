package harness

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeterministicReplay(t *testing.T) {
	s1 := NewSimulator(42)
	v1 := s1.Run(100, 1)

	s2 := NewSimulator(42)
	v2 := s2.Run(100, 1)

	assert.Equal(t, v1, v2, "same seed should produce identical results")
	assert.Zero(t, v1, "no faults injected, should have zero violations")
}

func TestNlinkInvariant(t *testing.T) {
	s := NewSimulator(123)

	// Create 10 files, then unlink 5 — nlink should be consistent
	for i := 0; i < 10; i++ {
		s.executeRandomOp()
	}
	s.store.Tick()

	v := s.checkNlinkConsistency()
	assert.Zero(t, v, "nlink should be consistent")
}

func TestCrashRecovery(t *testing.T) {
	s := NewSimulator(456)

	// Create some state
	s.createFile(t.Context(), 1, "test.txt", 100, 0100644)
	s.createFile(t.Context(), 1, "other.txt", 101, 0100644)
	s.createDir(t.Context(), 1, "mydir", 200)

	// Crash and restore
	s.simulateCrash()

	// All 3 inodes should be reloaded
	assert.NotNil(t, s.inodes[100])
	assert.NotNil(t, s.inodes[101])
	assert.NotNil(t, s.inodes[200])

	// All 3 dirents should be reloaded
	assert.Equal(t, uint64(100), s.lookup(1, "test.txt"))
	assert.Equal(t, uint64(101), s.lookup(1, "other.txt"))
	assert.Equal(t, uint64(200), s.lookup(1, "mydir"))

	v := s.checkInvariants()
	assert.Zero(t, v)
}

func TestNlinkConsistencyAfterUnlink(t *testing.T) {
	s := NewSimulator(789)

	s.createFile(t.Context(), 1, "f1", 301, 0100644)
	s.createFile(t.Context(), 1, "f2", 302, 0100644)
	s.createFile(t.Context(), 1, "f3", 303, 0100644)

	// Unlink f2
	s.unlinkFile(t.Context(), 1, "f2")

	v := s.checkNlinkConsistency()
	assert.Zero(t, v, "nlink should match after unlink")

	// f2 should be gone
	assert.Nil(t, s.inodes[302])
	assert.Zero(t, s.lookup(1, "f2"))
}

func TestRenamePreservesInode(t *testing.T) {
	s := NewSimulator(111)

	s.createFile(t.Context(), 1, "old", 401, 0100644)
	s.renameFile(t.Context(), 1, "old", 1, "new", 401)

	assert.Zero(t, s.lookup(1, "old"))
	assert.Equal(t, uint64(401), s.lookup(1, "new"))
	assert.NotNil(t, s.inodes[401])
}

func TestSeedZeroViolations(t *testing.T) {
	s := NewSimulator(42)
	v := s.Run(200, 1)
	assert.Zero(t, v, "random operations without faults should not violate invariants")
}
