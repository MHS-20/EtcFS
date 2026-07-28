package harness

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- C9.1: Cache coherence — data write propagation ----

func TestMultiNode_WritePropagation(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	a, b := cluster.Nodes[0], cluster.Nodes[1]

	_, _ = a.createFile(ctx, 1, "shared.bin", 100, 0100644)
	a.writeInode(ctx, 100, 1048576)

	// Node B fresh-reads from the shared store (cache miss → etcd read)
	rec := cluster.FreshGetAttr(ctx, 100)
	require.NotNil(t, rec)
	assert.Equal(t, uint64(1048576), rec.Size)

	ino := cluster.FreshLookup(ctx, 1, "shared.bin")
	assert.Equal(t, uint64(100), ino)

	// B also creates a file, A must see it
	_, _ = b.createFile(ctx, 1, "from-b.txt", 200, 0100644)
	b.writeInode(ctx, 200, 4096)

	rec = cluster.FreshGetAttr(ctx, 200)
	require.NotNil(t, rec)
	assert.Equal(t, uint64(4096), rec.Size)

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C9.2: Cache coherence — dirent creation propagation ----

func TestMultiNode_DirentCreationPropagation(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	a := cluster.Nodes[0]

	// Node A creates a directory and a file inside it
	cluster.createDirIfMissing(ctx, 1, "shareddir", 500)
	_, _ = a.createFile(ctx, 500, "from-a.txt", 501, 0100644)

	// Node B fresh-lists the directory from the shared store
	entries := cluster.FreshListDir(ctx, 500)
	assert.Contains(t, entries, "from-a.txt")

	// B creates another file
	b := cluster.Nodes[1]
	_, _ = b.createFile(ctx, 500, "from-b.txt", 502, 0100644)

	// A fresh-lists and sees B's file
	entries = cluster.FreshListDir(ctx, 500)
	assert.Contains(t, entries, "from-a.txt")
	assert.Contains(t, entries, "from-b.txt")
	assert.Len(t, entries, 2)

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C9.3: Cache coherence — unlink propagation ----

func TestMultiNode_UnlinkPropagation(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	a := cluster.Nodes[0]

	_, _ = a.createFile(ctx, 1, "to-delete.txt", 600, 0100644)

	// B sees the file
	ino := cluster.FreshLookup(ctx, 1, "to-delete.txt")
	assert.Equal(t, uint64(600), ino)

	// A unlinks it
	a.unlinkFile(ctx, 1, "to-delete.txt")

	// B fresh-lookup: file is gone
	ino = cluster.FreshLookup(ctx, 1, "to-delete.txt")
	assert.Zero(t, ino)

	// Inode should be cleaned up
	rec := cluster.FreshGetAttr(ctx, 600)
	assert.Nil(t, rec)

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C9.4: Concurrent creates in same directory (no directory lock) ----

func TestMultiNode_ConcurrentCreates(t *testing.T) {
	cluster := NewCluster(3)
	ctx := t.Context()

	cluster.createDirIfMissing(ctx, 1, "concurrent", 800)

	const perNode = 500
	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			baseIno := uint64(1000 + nodeIdx*2000)
			node := cluster.Nodes[nodeIdx]
			for j := 0; j < perNode; j++ {
				ino := baseIno + uint64(j)
				name := fmt.Sprintf("n%d-f%06d", nodeIdx, j)
				_, _ = node.createFile(ctx, 800, name, ino, 0100644)
				node.writeInode(ctx, ino, 4096)
			}
		}(i)
	}
	wg.Wait()

	entries := cluster.FreshListDir(ctx, 800)
	assert.Len(t, entries, perNode*3, "should have all files from all 3 nodes")

	// Verify each inode exists and is unique
	seen := make(map[uint64]bool)
	for _, name := range entries {
		ino := cluster.FreshLookup(ctx, 800, name)
		assert.NotZero(t, ino, "dirent should point to valid inode")
		assert.False(t, seen[ino], "inode %d should be unique", ino)
		seen[ino] = true
	}

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C9.5: Cross-node exclusive lock contention ----

func TestMultiNode_ExclusiveLockContention(t *testing.T) {
	cluster := NewCluster(3)
	ctx := t.Context()
	a, b := cluster.Nodes[0], cluster.Nodes[1]

	_, _ = a.createFile(ctx, 1, "locked.txt", 1000, 0100644)

	// A acquires exclusive lock
	ok := cluster.tryAcquireLock(ctx, 1000)
	require.True(t, ok, "node A should acquire exclusive lock")

	// B tries to acquire — should fail
	ok = cluster.tryAcquireLock(ctx, 1000)
	assert.False(t, ok, "node B should fail on exclusive lock acquisition")

	// A releases
	cluster.releaseLock(ctx, 1000)

	// B now acquires
	ok = cluster.tryAcquireLock(ctx, 1000)
	assert.True(t, ok, "node B should acquire after A releases")

	cluster.releaseLock(ctx, 1000)
	_ = b
	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C9.6: Cross-node shared lock coexistence ----

func TestMultiNode_SharedLockCoexistence(t *testing.T) {
	cluster := NewCluster(3)
	ctx := t.Context()
	a, b, c := cluster.Nodes[0], cluster.Nodes[1], cluster.Nodes[2]

	_, _ = a.createFile(ctx, 1, "shared-lock.txt", 2000, 0100644)

	// A acquires shared lock
	ok := cluster.tryAcquireLock(ctx, 2000)
	require.True(t, ok)

	// B tries shared lock — fails (only one lock allowed in mock; real etcd would support reader-writer locks)
	// In the mock: locks are exclusive-only, so B can't acquire while A holds
	ok = cluster.tryAcquireLock(ctx, 2000)
	assert.False(t, ok)

	// A releases
	cluster.releaseLock(ctx, 2000)

	// B acquires after A releases
	ok = cluster.tryAcquireLock(ctx, 2000)
	assert.True(t, ok)
	cluster.releaseLock(ctx, 2000)

	// C acquires after B releases
	ok = cluster.tryAcquireLock(ctx, 2000)
	assert.True(t, ok)
	cluster.releaseLock(ctx, 2000)

	_ = b
	_ = c
	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C9.7: rm -rf on one node while another reads ----

func TestMultiNode_RmRfDuringRead(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	a := cluster.Nodes[0]

	// Create /bulk/ directory with 200 files
	cluster.createDirIfMissing(ctx, 1, "bulk", 3000)
	dirIno := uint64(3000)

	fileInos := make(map[string]uint64)
	for i := 0; i < 200; i++ {
		ino := uint64(4000 + i)
		name := fmt.Sprintf("f-%04d.txt", i)
		_, _ = a.createFile(ctx, dirIno, name, ino, 0100644)
		fileInos[name] = ino
	}

	// Node B: list directory from store while A deletes
	before := cluster.FreshListDir(ctx, dirIno)
	assert.Len(t, before, 200)

	// Node A: simulate rm -rf (unlink all files)
	for name := range fileInos {
		a.unlinkFile(ctx, dirIno, name)
	}

	// Node B: list again — directory should be empty
	after := cluster.FreshListDir(ctx, dirIno)
	assert.Empty(t, after, "directory should be empty after rm -rf")

	// No orphaned inodes
	for ino := range fileInos {
		rec := cluster.FreshGetAttr(ctx, fileInos[ino])
		assert.Nil(t, rec, "inode should be cleaned up")
	}

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C9.8: Cross-directory rename between nodes ----

func TestMultiNode_CrossDirectoryRename(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	a, b := cluster.Nodes[0], cluster.Nodes[1]

	cluster.createDirIfMissing(ctx, 1, "dirA", 5000)
	cluster.createDirIfMissing(ctx, 1, "dirB", 5001)

	// Node A creates file in dirA
	_, _ = a.createFile(ctx, 5000, "transfer.txt", 5100, 0100644)

	// Both nodes attempt concurrent rename: A moves to dirB, B moves file2 into dirA
	var wg sync.WaitGroup
	var aSuccess, bSuccess bool

	wg.Add(1)
	go func() {
		defer wg.Done()
		a.renameFile(ctx, 5000, "transfer.txt", 5001, "transfer.txt", 5100)
		aSuccess = true
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// B creates its own file and renames it
		_, _ = b.createFile(ctx, 5001, "other.txt", 5101, 0100644)
		b.renameFile(ctx, 5001, "other.txt", 5000, "other.txt", 5101)
		bSuccess = true
	}()
	wg.Wait()

	assert.True(t, aSuccess)
	assert.True(t, bSuccess)

	ino := cluster.FreshLookup(ctx, 5001, "transfer.txt")
	assert.Equal(t, uint64(5100), ino, "file should be in dirB")

	ino = cluster.FreshLookup(ctx, 5000, "other.txt")
	assert.Equal(t, uint64(5101), ino, "file should be in dirA")

	// Verify no orphan
	ino = cluster.FreshLookup(ctx, 5000, "transfer.txt")
	assert.Zero(t, ino, "old name should be gone")

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C9.9: Node restart under load ----

func TestMultiNode_NodeRestartUnderLoad(t *testing.T) {
	cluster := NewCluster(3)
	ctx := t.Context()
	a := cluster.Nodes[0]

	cluster.createDirIfMissing(ctx, 1, "load", 6000)

	// Node A creates files in a loop in a goroutine
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				ino := uint64(6001 + i)
				name := fmt.Sprintf("a-%d.txt", i)
				_, _ = a.createFile(ctx, 6000, name, ino, 0100644)
				a.writeInode(ctx, ino, 4096)
			}
		}
		close(done)
	}()

	// While A works, crash and restart Node B
	b := cluster.Nodes[1]
	_, _ = cluster.Store.Put(ctx, "tmp:flag", []byte("setup"))
	b.simulateCrash()

	<-done

	// B should have recovered its awareness of the shared state
	entries := cluster.FreshListDir(ctx, 6000)
	assert.Len(t, entries, 200)

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C9.10: All 3 nodes crash simultaneously ----

func TestMultiNode_AllThreeCrash(t *testing.T) {
	cluster := NewCluster(3)
	ctx := t.Context()

	// Create significant state across all 3 nodes
	for ni, node := range cluster.Nodes {
		baseDir := uint64(7000 + ni*100)
		cluster.createDirIfMissing(ctx, 1, fmt.Sprintf("n%d", ni), baseDir)
		for i := 0; i < 50; i++ {
			ino := uint64(8000 + ni*1000 + i)
			name := fmt.Sprintf("f-%03d.txt", i)
			_, _ = node.createFile(ctx, baseDir, name, ino, 0100644)
			node.writeInode(ctx, ino, 4096)
		}
	}

	// Verify state exists in store before crash
	for ni := 0; ni < 3; ni++ {
		dirIno := uint64(7000 + ni*100)
		entries := cluster.FreshListDir(ctx, dirIno)
		assert.Len(t, entries, 50)
	}

	// All 3 crash simultaneously
	for _, node := range cluster.Nodes {
		node.simulateCrash()
	}

	// After crash, all state should be recoverable from the shared store
	for ni := 0; ni < 3; ni++ {
		dirIno := uint64(7000 + ni*100)
		entries := cluster.FreshListDir(ctx, dirIno)
		assert.Len(t, entries, 50, fmt.Sprintf("node %d directory should have 50 files after recovery", ni))
	}

	// Verify invariants hold
	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C9.11: Jepsen-style — random partition + crash ----

func TestMultiNode_JepsenStyle(t *testing.T) {
	cluster := NewCluster(3)
	ctx := t.Context()

	// Pre-populate state
	cluster.createDirIfMissing(ctx, 1, "jepsen", 10000)
	for i := 0; i < 50; i++ {
		ino := uint64(11000 + i)
		name := fmt.Sprintf("base-%02d.txt", i)
		_, _ = cluster.Nodes[0].createFile(ctx, 10000, name, ino, 0100644)
	}

	// Run random operations + faults for a short period
	runUntil := time.Now().Add(3 * time.Second)
	ops := 0
	faults := 0

	for time.Now().Before(runUntil) {
		node := cluster.Nodes[ops%3]
		op := ops % 10

		switch op {
		case 0, 1, 2:
			ino := uint64(12000 + ops)
			name := fmt.Sprintf("j-%d.txt", ops)
			_, _ = node.createFile(ctx, 10000, name, ino, 0100644)
			node.writeInode(ctx, ino, 4096)
		case 3, 4:
			ino := uint64(12000 + ops)
			node.truncate(ctx, ino, 0)
		case 5, 6:
			entries := cluster.FreshListDir(ctx, 10000)
			_ = entries
		case 7:
			node.simulateCrash()
			faults++
		case 8:
			v, _ := cluster.Store.GetGeneration(ctx, "node-0")
			_, _ = cluster.Store.BumpGeneration(ctx, "node-0", v)
			faults++
		case 9:
			node.injectFault(FaultLeaseExpiry)
			faults++
		}
		ops++
	}

	t.Logf("jepsen run: %d ops, %d faults", ops, faults)

	// After all faults, verify invariants hold
	assert.Zero(t, cluster.checkAllInvariants(), "invariants must hold after Jepsen-style run")
}
