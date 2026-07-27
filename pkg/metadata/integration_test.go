//go:build integration
// +build integration

// Integration tests for the metadata layer against a real etcd cluster.
//
// Requires a running etcd cluster.  Start one with:
//
//	docker compose -f deploy/docker/docker-compose.yml up -d etcd1 etcd2 etcd3
//
// Run tests with:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration -count=1 -v ./pkg/metadata/ -run Integration
package metadata

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3"
)

// testStore connects to etcd and returns a Store for testing.
func testStore(t *testing.T, nodeID string) *Store {
	t.Helper()

	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "http://localhost:2379"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err, "cannot connect to etcd at %s — is Docker Compose running?", endpoints)

	t.Cleanup(func() {
		// Clean up test keys
		cli.Delete(context.Background(), PrefixInode, clientv3.WithPrefix())
		cli.Delete(context.Background(), PrefixDirent, clientv3.WithPrefix())
		cli.Delete(context.Background(), PrefixLock, clientv3.WithPrefix())
		cli.Delete(context.Background(), PrefixGen, clientv3.WithPrefix())
		cli.Delete(context.Background(), PrefixArena, clientv3.WithPrefix())
		cli.Delete(context.Background(), KeyInodeAllocCounter)
		cli.Close()
	})

	return NewStore(cli, nodeID)
}

// ---- C1.1: Schema validation ----

func TestIntegration_SchemaKeyFormats(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()

	// Write a key in each family and verify we can read it back
	_, err := store.Put(ctx, InodeKey(1), []byte("test-inode"))
	require.NoError(t, err)

	_, err = store.Put(ctx, DirentKey(1, "hello"), EncodeUint64(42))
	require.NoError(t, err)

	_, err = store.Put(ctx, LockKey(1), []byte(`{"mode":"shared","holders":["test"]}`))
	require.NoError(t, err)

	_, err = store.Put(ctx, GenKey("node-1"), []byte("5"))
	require.NoError(t, err)

	// Verify reads
	v, err := store.Get(ctx, InodeKey(1))
	require.NoError(t, err)
	assert.Equal(t, "test-inode", string(v))

	v, err = store.Get(ctx, DirentKey(1, "hello"))
	require.NoError(t, err)
	assert.Equal(t, uint64(42), DecodeUint64(v))
}

// ---- C1.2: Atomic dirent create (concurrent) ----

func TestIntegration_AtomicDirentCreate(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	const concurrent = 20
	errCh := make(chan error, concurrent)
	var created int32

	// Create parent inode
	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			name := fmt.Sprintf("file-%d", id)
			err := store.CreateDirent(ctx, parent, name, uint64(id+10))
			if err == nil {
				atomic.AddInt32(&created, 1)
			}
			errCh <- err
		}(i)
	}
	wg.Wait()
	close(errCh)

	// Each file should have been created exactly once
	assert.Equal(t, int32(concurrent), created, "all %d concurrent creates should succeed", concurrent)

	// Verify all entries exist
	entries, err := store.ListDirents(ctx, parent)
	require.NoError(t, err)
	assert.Len(t, entries, concurrent, "should have %d directory entries", concurrent)
}

// ---- C1.3: Atomic cross-directory rename ----

func TestIntegration_AtomicRename(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()

	parent := uint64(1)
	ino := uint64(42)

	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	// Create source file
	err = store.CreateDirent(ctx, parent, "old-name", ino)
	require.NoError(t, err)

	// Rename
	err = store.AtomicRename(ctx, parent, "old-name", parent, "new-name", ino, 0)
	require.NoError(t, err)

	// Old name should not exist
	oldIno, err := store.LookupDirent(ctx, parent, "old-name")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), oldIno, "old name should not exist after rename")

	// New name should point to the inode
	newIno, err := store.LookupDirent(ctx, parent, "new-name")
	require.NoError(t, err)
	assert.Equal(t, ino, newIno, "new name should point to same inode")
}

// ---- C1.4: Atomic rm -rf (DeleteRange) ----

func TestIntegration_AtomicRmRf(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	// Create 100 files
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("file-%04d", i)
		err := store.CreateDirent(ctx, parent, name, uint64(i+100))
		require.NoError(t, err)
	}

	entries, err := store.ListDirents(ctx, parent)
	require.NoError(t, err)
	assert.Len(t, entries, 100)

	// Bulk delete via DeleteRange on prefix
	deleted, err := store.AtomicRmRf(ctx, parent)
	require.NoError(t, err)
	assert.Equal(t, int64(100), deleted)

	entries, err = store.ListDirents(ctx, parent)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// ---- C1.5: Lease-backed lock acquire/release ----

func TestIntegration_LockAcquireRelease(t *testing.T) {
	store := testStore(t, "node-a")
	ctx := context.Background()
	ino := uint64(100)

	// Acquire exclusive lock
	leaseID, keepCh, err := store.AcquireLock(ctx, ino, LockExclusive, 10*time.Second)
	require.NoError(t, err)
	require.NotZero(t, leaseID)

	// Consume one keepalive to verify it works
	select {
	case resp := <-keepCh:
		require.NotNil(t, resp)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for keepalive response")
	}

	// Verify lock exists
	locked, err := store.IsLocked(ctx, ino)
	require.NoError(t, err)
	assert.True(t, locked)

	// Release
	err = store.ReleaseLock(ctx, ino, leaseID)
	require.NoError(t, err)

	// Verify lock is gone (lease revoked)
	time.Sleep(1 * time.Second)
	locked, err = store.IsLocked(ctx, ino)
	require.NoError(t, err)
	assert.False(t, locked)
}

// ---- C1.6: Lease expiry releases lock ----

func TestIntegration_LockLeaseExpiry(t *testing.T) {
	store := testStore(t, "node-b")
	ctx, cancel := context.WithCancel(context.Background())
	ino := uint64(200)

	// Acquire lock with short TTL
	leaseID, keepCh, err := store.AcquireLock(ctx, ino, LockExclusive, 3*time.Second)
	require.NoError(t, err)

	// Stop keepalive by cancelling context
	cancel()
	_ = leaseID

	// Drain keepalive channel
	go func() {
		for range keepCh {
		}
	}()

	// Wait for TTL expiry
	t.Log("waiting for lease expiry (3s TTL + 2s grace)...")
	time.Sleep(6 * time.Second)

	// Lock should be auto-deleted by etcd
	locked, err := store.IsLocked(context.Background(), ino)
	require.NoError(t, err)
	assert.False(t, locked, "lock should be auto-deleted after lease expiry")
}

// ---- C1.7: Fencing generation CAS ----

func TestIntegration_GenerationBump(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	nodeID := "fenced-node-1"

	// Initialise generation
	err := store.EnsureGeneration(ctx, nodeID, 0)
	require.NoError(t, err)

	gen, err := store.GetGeneration(ctx, nodeID)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), gen)

	// Bump generation
	newGen, err := store.BumpGeneration(ctx, nodeID, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), newGen)

	// Verify
	gen, err = store.GetGeneration(ctx, nodeID)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen)

	// Attempt to bump with stale expected value should fail
	_, err = store.BumpGeneration(ctx, nodeID, 0) // expected 0, actual 1
	assert.Error(t, err, "stale generation bump should fail")

	// Correct bump should succeed
	newGen, err = store.BumpGeneration(ctx, nodeID, 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), newGen)
}

// ---- C1.8: Inode range reservation ----

func TestIntegration_InodeRangeAllocation(t *testing.T) {
	store := testStore(t, "node-1")
	ctx := context.Background()

	// Reserve a range
	start, end, err := store.AllocInodeRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), start)
	assert.Equal(t, uint64(AllocRangeSize), end)

	// Reserve another range (should get next block)
	start2, end2, err := store.AllocInodeRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(AllocRangeSize), start2)
	assert.Equal(t, uint64(2*AllocRangeSize), end2)

	// Local allocator
	alloc := NewNodeInodeAlloc("node-1")

	err = alloc.Reserve(ctx, store)
	require.NoError(t, err)

	ino, err := alloc.Allocate()
	require.NoError(t, err)
	assert.Equal(t, uint64(2*AllocRangeSize), ino)

	ino2, err := alloc.Allocate()
	require.NoError(t, err)
	assert.Equal(t, uint64(2*AllocRangeSize+1), ino2)
}

// ---- C1.9: Watch delivery ----

func TestIntegration_WatchDelivery(t *testing.T) {
	store := testStore(t, "test-node")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	parent := uint64(99)
	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	// Start watching the directory prefix
	watchCh := store.Watch(ctx, DirentPrefix(parent), clientv3.WithPrefix())

	// Create files in a separate goroutine
	go func() {
		for i := 0; i < 10; i++ {
			_ = store.CreateDirent(context.Background(), parent, fmt.Sprintf("watch-%d", i), uint64(i+500))
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Collect at least one batch of events
	eventCount := 0
	timeout := time.After(15 * time.Second)
loop:
	for {
		select {
		case resp, ok := <-watchCh:
			if !ok {
				break loop
			}
			eventCount += len(resp.Events)
			t.Logf("received %d watch events (total: %d)", len(resp.Events), eventCount)
			if eventCount >= 10 {
				break loop
			}
		case <-timeout:
			break loop
		case <-ctx.Done():
			break loop
		}
	}

	assert.GreaterOrEqual(t, eventCount, 1, "should receive at least one watch event")
}

// ---- C1.10: Paginated readdir ----

func TestIntegration_PaginatedReaddir(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(5000)

	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	const totalFiles = 100
	for i := 0; i < totalFiles; i++ {
		err := store.CreateDirent(ctx, parent, fmt.Sprintf("pg-file-%04d", i), uint64(i+50000))
		require.NoError(t, err)
	}

	// Read in pages of 30
	const pageSize int64 = 30
	var allEntries []DirentEntry
	cursor := ""

	for {
		entries, nextCursor, _, err := store.ListDirentsPaginated(ctx, parent, cursor, pageSize)
		require.NoError(t, err)
		allEntries = append(allEntries, entries...)

		if len(entries) < int(pageSize) || nextCursor == "" {
			break
		}
		cursor = nextCursor
	}

	assert.GreaterOrEqual(t, len(allEntries), totalFiles, "paginated readdir should return at least %d entries", totalFiles)
}

// ---- C1.11: Transaction conflict storm ----

func TestIntegration_TransactionConflictStorm(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()

	const concurrent = 50
	const key = "storm/counter"
	var successes int32

	// Initialise counter
	_, err := store.Put(ctx, key, EncodeUint64(0))
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for attempt := 0; attempt < 20; attempt++ {
				value, err := store.Get(ctx, key)
				if err != nil {
					continue
				}
				current := DecodeUint64(value)
				next := current + 1

				cmp := clientv3.Compare(clientv3.Value(key), "=", string(EncodeUint64(current)))
				op := clientv3.OpPut(key, string(EncodeUint64(next)))

				ok, err := store.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{op}, nil)
				if err == nil && ok {
					atomic.AddInt32(&successes, 1)
					return
				}
				time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	// Every goroutine should eventually succeed
	assert.Equal(t, int32(concurrent), successes, "all concurrent increments should eventually succeed")

	// Verify final value
	final, err := store.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, uint64(concurrent), DecodeUint64(final), "counter should equal number of successes")
}

// ---- C1.12: Large extent map ----

func TestIntegration_LargeExtentMap(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	ino := uint64(77777)

	const totalExtents = 200
	for i := 0; i < totalExtents; i++ {
		err := store.AppendExtent(ctx, ino,
			uint64(i)*4096,          // logical offset
			uint64(i)*4096+1000000,  // disk offset
			4096,                    // length
			1)                       // generation
		require.NoError(t, err, "append extent %d", i)
	}

	// Read all extents back
	extents, err := store.GetExtents(ctx, ino)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(extents), totalExtents, "should have at least %d extents", totalExtents)

	// Verify first and last
	assert.Equal(t, uint64(0), extents[0][0])                       // logical_off
	assert.Equal(t, uint64(1000000), extents[0][1])                 // disk_off
	assert.Equal(t, uint64((totalExtents-1)*4096), extents[totalExtents-1][0])
}

// ---- Additional: Inode CRUD ----

func TestIntegration_InodeCRUD(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()

	// Create
	rec, err := store.CreateInode(ctx, 42, 0644, 1000, 1000)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), rec.Ino)
	assert.Equal(t, uint32(0644), rec.Mode)

	// Get
	rec, err = store.GetInode(ctx, 42)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, uint32(0644), rec.Mode)

	// Create duplicate should fail
	_, err = store.CreateInode(ctx, 42, 0644, 1000, 1000)
	assert.Error(t, err)

	// Delete
	err = store.DeleteInode(ctx, 42)
	require.NoError(t, err)

	// Verify deleted
	rec, err = store.GetInode(ctx, 42)
	require.NoError(t, err)
	assert.Nil(t, rec)
}

func TestIntegration_AtomicCreateFile(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	rec, err := store.AtomicCreateFile(ctx, parent, "test.txt", 100, 0644, 1000, 1000)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), rec.Ino)
	assert.Equal(t, uint32(0644), rec.Mode)

	// Verify dirent exists
	ino, err := store.LookupDirent(ctx, parent, "test.txt")
	require.NoError(t, err)
	assert.Equal(t, uint64(100), ino)

	// Verify inode exists
	rec, err = store.GetInode(ctx, 100)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, uint32(1), rec.Nlink)
}

func TestIntegration_AtomicUnlinkFile(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	_, err = store.AtomicCreateFile(ctx, parent, "to-delete.txt", 200, 0644, 1000, 1000)
	require.NoError(t, err)

	err = store.AtomicUnlink(ctx, parent, "to-delete.txt")
	require.NoError(t, err)

	// Dirent should be gone
	ino, err := store.LookupDirent(ctx, parent, "to-delete.txt")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), ino)

	// Inode should be deleted (nlink reached 0)
	rec, err := store.GetInode(ctx, 200)
	require.NoError(t, err)
	assert.Nil(t, rec, "inode should be deleted when nlink reaches 0")
}

// ---- Concurrent test helper ----

func TestIntegration_ConcurrentCreatesNoCollision(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	const workers = 32
	const filesPerWorker = 10
	errCh := make(chan error, workers*filesPerWorker)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for f := 0; f < filesPerWorker; f++ {
				name := fmt.Sprintf("w%d-f%d", workerID, f)
				ino := uint64(workerID*1000 + f + 10000)
				_, err := store.AtomicCreateFile(ctx, parent, name, ino, 0644, 1000, 1000)
				errCh <- err
			}
		}(w)
	}
	wg.Wait()
	close(errCh)

	errors := 0
	for err := range errCh {
		if err != nil {
			t.Logf("create error: %v", err)
			errors++
		}
	}

	assert.Equal(t, 0, errors, "all concurrent creates should succeed without collisions")

	entries, err := store.ListDirents(ctx, parent)
	require.NoError(t, err)
	assert.Len(t, entries, workers*filesPerWorker)
}
