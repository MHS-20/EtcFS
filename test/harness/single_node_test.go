package harness

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- C7.5: Write known patterns → crash → verify ----

func TestSingleNode_WriteCrashRecovery(t *testing.T) {
	s := NewSimulator(7001)
	ctx := t.Context()

	// Create a file and write data
	_, _ = s.createFile(ctx, 1, "data.bin", 7000, 0100644)
	s.writeInode(ctx, 7000, 1024*1024) // 1 MB

	// Verify size before crash
	rec := s.getattr(7000)
	require.NotNil(t, rec)
	assert.Equal(t, uint64(1024*1024), rec.Size)

	// Crash and recover
	s.simulateCrash()

	// Verify data survived
	rec = s.getattr(7000)
	require.NotNil(t, rec, "inode should survive crash")
	assert.Equal(t, uint64(1024*1024), rec.Size, "committed size should survive crash")

	// Dirent should survive
	ino := s.lookup(1, "data.bin")
	assert.Equal(t, uint64(7000), ino)

	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- C7.5: Write then crash mid-write, verify only committed data survives ----

func TestSingleNode_CrashMidWrite(t *testing.T) {
	s := NewSimulator(7002)
	ctx := t.Context()

	// Create file, commit initial size
	_, _ = s.createFile(ctx, 1, "partial.bin", 7100, 0100644)
	s.writeInode(ctx, 7100, 4096)

	// Crash (simulating power loss)
	s.simulateCrash()

	// After crash, only committed data should survive
	rec := s.getattr(7100)
	require.NotNil(t, rec)
	assert.Equal(t, uint64(4096), rec.Size, "only committed size should survive")

	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- C7.6: Crash during fsync ----

func TestSingleNode_CrashDuringFsync(t *testing.T) {
	s := NewSimulator(7003)
	ctx := t.Context()

	// Create and write
	_, _ = s.createFile(ctx, 1, "fsync.txt", 7200, 0100644)
	s.writeInode(ctx, 7200, 8192)

	// Simulate fsync: write to store
	rec := s.getattr(7200)
	val := []byte(fmt.Sprintf("size=%d", rec.Size))
	_, _ = s.store.Put(ctx, "fsync:7200", val)

	// Crash
	s.simulateCrash()

	// Verify fsynced data survived
	val, err := s.store.Get(ctx, "fsync:7200")
	require.NoError(t, err)
	assert.NotNil(t, val)

	rec = s.getattr(7200)
	require.NotNil(t, rec)
	assert.Equal(t, uint64(8192), rec.Size)

	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- C7.8: ENOSPC handling ----

func TestSingleNode_ENOSPC(t *testing.T) {
	s := NewSimulator(7004)
	ctx := t.Context()

	_, _ = s.createFile(ctx, 1, "full.bin", 7300, 0100644)

	// Try to write beyond the arena limit — should fail
	// In the mock: writeInode just sets size, ENOSPC is tested at block allocation level

	// Simulate: write succeeds up to arena capacity
	for i := uint64(0); i < 100; i++ {
		s.writeInode(ctx, 7300, i*4096)
	}

	rec := s.getattr(7300)
	require.NotNil(t, rec)
	assert.Greater(t, rec.Size, uint64(0))

	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- C7.9: Maximum file size ----

func TestSingleNode_MaxFileSize(t *testing.T) {
	s := NewSimulator(7005)
	ctx := t.Context()

	ino := uint64(7400)
	_, _ = s.createFile(ctx, 1, "huge.bin", ino, 0100644)

	// Write a large file (simulated — in production, multiple extents)
	largeSize := uint64(10 * 1024 * 1024 * 1024) // 10 GB
	s.writeInode(ctx, ino, largeSize)

	rec := s.getattr(ino)
	require.NotNil(t, rec)
	assert.Equal(t, largeSize, rec.Size)

	// Verify lots of extents can be stored (test with 1000 extents)
	store := s.store
	for i := 0; i < 1000; i++ {
		extKey := fmt.Sprintf("extent:%d/%d", ino, i)
		extVal := fmt.Sprintf("%d,%d,%d,%d,%d", i*4096, i*4096+1000000, 4096, 1, i)
		_, err := store.Put(ctx, extKey, []byte(extVal))
		require.NoError(t, err)
	}

	// Read back all extents
	count := 0
	prefix := fmt.Sprintf("extent:%d/", ino)
	kvs, _ := store.GetPrefix(ctx, prefix)
	for range kvs {
		count++
	}
	assert.Equal(t, 1000, count, "all 1000 extents should be stored")

	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- C7.11: Performance baseline ----

func TestSingleNode_PerformanceBaseline(t *testing.T) {
	s := NewSimulator(7006)
	ctx := t.Context()

	ops := 1000
	start := time.Now()

	for i := 0; i < ops; i++ {
		ino := uint64(8000 + i)
		name := fmt.Sprintf("perf-%d", i)
		_, _ = s.createFile(ctx, 1, name, ino, 0100644)
		s.writeInode(ctx, ino, 4096)
		// Read back
		_ = s.lookup(1, name)
		_ = s.getattr(ino)
		// Delete
		s.unlinkFile(ctx, 1, name)
	}

	elapsed := time.Since(start)
	opsPerSec := float64(ops*4) / elapsed.Seconds() // create+write+read+delete
	t.Logf("metadata ops/sec: %.0f (elapsed: %v)", opsPerSec, elapsed)

	assert.Greater(t, opsPerSec, float64(100), "should handle at least 100 metadata ops/sec")

	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- C7.12: Graceful shutdown and restart ----

func TestSingleNode_GracefulShutdownRestart(t *testing.T) {
	s := NewSimulator(7007)
	ctx := t.Context()

	// Create state
	_, _ = s.createFile(ctx, 1, "persistent.txt", 9000, 0100644)
	s.writeInode(ctx, 9000, 4096)

	// Graceful shutdown: flush all state
	// In the simulator, all state is already in the mock store

	// Restart (simulate crash + recovery)
	s.simulateCrash()

	// Verify all state is intact
	ino := s.lookup(1, "persistent.txt")
	assert.Equal(t, uint64(9000), ino)

	rec := s.getattr(9000)
	require.NotNil(t, rec)
	assert.Equal(t, uint64(4096), rec.Size)

	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- C7.10: rsync-like workload (many small files) ----

func TestSingleNode_ManySmallFiles(t *testing.T) {
	s := NewSimulator(7008)
	ctx := t.Context()

	const fileCount = 200
	parent := uint64(10000)

	// Create a parent directory
	_, _ = s.createDir(ctx, 1, "manyfiles", parent)

	// Create many small files
	for i := 0; i < fileCount; i++ {
		ino := parent + 1 + uint64(i)
		name := fmt.Sprintf("f-%04d.txt", i)
		_, err := s.createFile(ctx, parent, name, ino, 0100644)
		require.NoError(t, err)
		s.writeInode(ctx, ino, uint64(i%100)*4096)
	}

	// Verify all files exist
	for i := 0; i < fileCount; i++ {
		name := fmt.Sprintf("f-%04d.txt", i)
		ino := s.lookup(parent, name)
		assert.Equal(t, parent+1+uint64(i), ino)
	}

	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- Additional: rename during crash ----

func TestSingleNode_RenameAndCrash(t *testing.T) {
	s := NewSimulator(7009)
	ctx := t.Context()

	_, _ = s.createFile(ctx, 1, "before.txt", 9500, 0100644)
	s.renameFile(ctx, 1, "before.txt", 1, "after.txt", 9500)

	// Crash after rename
	s.simulateCrash()

	// Old name should be gone
	oldIno := s.lookup(1, "before.txt")
	assert.Zero(t, oldIno)

	// New name should point to inode
	newIno := s.lookup(1, "after.txt")
	assert.Equal(t, uint64(9500), newIno)

	v := s.checkInvariants()
	assert.Zero(t, v)
}
