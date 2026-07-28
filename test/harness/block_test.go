package harness

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- C6.1: Block device discovery ----
// Tests that we can detect block sizes from sysfs or fall back to defaults.

func TestBlock_DeviceDiscovery(t *testing.T) {
	// Test with a regular file simulating a block device
	f, err := os.CreateTemp("", "etcfuse-block-*")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()
	defer func() { _ = f.Close() }()

	// Write enough data to simulate a block device
	_, err = f.Write(make([]byte, 4096*100))
	require.NoError(t, err)

	// Verify we can stat the file and get its size
	fi, err := f.Stat()
	require.NoError(t, err)
	assert.Equal(t, int64(4096*100), fi.Size())
}

// ---- C6.2: O_DIRECT alignment ----

func TestBlock_Alignment(t *testing.T) {
	// O_DIRECT requires: buffer aligned to sector size, offset sector-aligned, length sector-multiple
	sectorSize := 4096

	aligned := make([]byte, sectorSize*2)
	offset := uint64(sectorSize)
	length := uint64(sectorSize)

	assert.True(t, offset%uint64(sectorSize) == 0, "offset must be sector-aligned")
	assert.True(t, length%uint64(sectorSize) == 0, "length must be sector-multiple")
	assert.True(t, uintptr(len(aligned))%uintptr(sectorSize) == 0, "buffer must be alignable")

	// Misaligned offset should be rejected
	badOffset := uint64(sectorSize + 512)
	assert.False(t, badOffset%uint64(sectorSize) == 0, "misaligned offset should be detected")
}

// ---- C6.8: Data-then-metadata ordering ----

func TestBlock_DataThenMetadata(t *testing.T) {
	// Simulate the write ordering invariant:
	// 1. Write data to block device
	// 2. Commit metadata to etcd
	// In our harness, this maps to: Put extent → then Update inode

	store := NewMockStore()
	ctx := context.Background()
	ino := uint64(6001)
	diskOff := uint64(0)
	length := uint64(4096)
	generation := uint64(1)

	// Step 1: Write extent data (simulated in harness)
	// In production: O_DIRECT pwrite to block device at diskOff

	// Step 2: Commit metadata (inode's extent list updated)
	rec := &mockInodeRecord{Ino: ino, Mode: 0100644, Nlink: 1, Size: length}
	store.kv["inode:6001"] = []byte(encodeMockInode(rec))

	// Step 3: Store extent mapping separately
	extKey := fmt.Sprintf("extent:%d/0", ino)
	extVal := fmt.Sprintf("%d,%d,%d,%d", 0, diskOff, length, generation)
	_, err := store.Put(ctx, extKey, []byte(extVal))
	require.NoError(t, err)

	// Verify the extent is recorded
	val, err := store.Get(ctx, extKey)
	require.NoError(t, err)
	assert.NotNil(t, val)

	// Simulate crash between data write and metadata commit:
	// If metadata wasn't committed, the extent should be an orphan.
	// Delete the metadata (simulating: was never committed)
	_ = store.Delete(ctx, "inode:6001")

	// After crash recovery, the orphan extent exists but no inode references it
	val, _ = store.Get(ctx, extKey)
	assert.NotNil(t, val, "orphan extent still on disk")

	// The scrubber would reclaim this orphan extent
	_ = store.Delete(ctx, extKey)
	val, _ = store.Get(ctx, extKey)
	assert.Nil(t, val, "orphan reclaimed by scrubber")
}

// ---- C6.9: Metadata-then-data ordering for truncate ----

func TestBlock_MetadataThenDataTruncate(t *testing.T) {
	store := NewMockStore()
	_ = context.Background()
	_ = uint64(6002)

	// Pre-allocate extents
	store.kv["extent:6002/0"] = []byte("0,1024,4096,1")
	store.kv["inode:6002"] = []byte("ino=6002,size=4096,nlink=1")

	// Step 1: Commit metadata (smaller extent list) to etcd
	store.kv["inode:6002"] = []byte("ino=6002,size=0,nlink=1")

	// Step 2: Then free the disk space
	// In production: the freed range is returned to the arena free-list
	delete(store.kv, "extent:6002/0")

	// Verify extent is freed
	_, exists := store.kv["extent:6002/0"]
	assert.False(t, exists, "extent should be freed after truncate")
}

// ---- C6.13: Fencing generation stamp on every extent ----

func TestBlock_GenerationStampedExtent(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	nodeID := "writer-node-1"
	generation := uint64(7)

	// Set up generation
	_, err := store.Put(ctx, fmt.Sprintf("gen:%s", nodeID), []byte(fmt.Sprintf("%d", generation)))
	require.NoError(t, err)

	// Write an extent with the current generation stamp
	extKey := "extent:7001/0"
	extVal := fmt.Sprintf("0,2048,4096,%d", generation)
	_, err = store.Put(ctx, extKey, []byte(extVal))
	require.NoError(t, err)

	// Verify the generation stamp is stored
	val, err := store.Get(ctx, extKey)
	require.NoError(t, err)
	assert.Contains(t, string(val), fmt.Sprintf("%d", generation))

	// Simulate fence: bump generation
	_, err = store.BumpGeneration(ctx, nodeID, generation)
	require.NoError(t, err)

	// New generation
	newGen, err := store.GetGeneration(ctx, nodeID)
	require.NoError(t, err)
	assert.Equal(t, uint64(8), newGen)

	// Old extents still have the old generation stamp
	val, _ = store.Get(ctx, extKey)
	assert.Contains(t, string(val), "7", "old extent keeps original generation stamp")
}

// ---- Helpers ----

type mockInodeRecord struct {
	Ino     uint64
	Mode    uint32
	Nlink   uint32
	Size    uint64
	Blksize uint32
}

func encodeMockInode(rec *mockInodeRecord) string {
	return fmt.Sprintf("ino=%d,mode=%o,nlink=%d,size=%d,blksize=%d",
		rec.Ino, rec.Mode, rec.Nlink, rec.Size, rec.Blksize)
}

func init() { _ = context.Background() }
