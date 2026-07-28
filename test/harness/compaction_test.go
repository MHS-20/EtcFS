package harness

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MHS-20/EtcFS/pkg/compaction"
	"github.com/MHS-20/EtcFS/pkg/metadata"
)

func setupArenaFiles(t *testing.T, c *Cluster, arenaID uint64, dirIno uint64, count int) []uint64 {
	ctx := t.Context()
	c.createDirIfMissing(ctx, 1, fmt.Sprintf("arena-%d", arenaID), dirIno)

	inos := make([]uint64, count)
	diskStart := c.ArenaDiskStart(arenaID)
	for i := 0; i < count; i++ {
		ino := 100000 + arenaID*10000 + uint64(i)
		name := fmt.Sprintf("a%d-f%03d", arenaID, i)
		inos[i] = ino

		diskOff := diskStart + uint64(i)*arenaBlockSize
		extKey := fmt.Sprintf("extent:%d/0", ino)
		extVal := fmt.Sprintf("0,%d,%d,1", diskOff, arenaBlockSize)
		_, _ = c.Store.Put(ctx, extKey, []byte(extVal))

		rec := &metadata.InodeRecord{
			Ino: ino, Mode: 0100644, Nlink: 1,
			Size: arenaBlockSize, Blksize: arenaBlockSize,
		}
		_, _ = c.Store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
		_, _ = c.Store.Put(ctx, metadata.DirentKey(dirIno, name), metadata.EncodeUint64(ino))
	}

	arenaKey := fmt.Sprintf("arena:%d", arenaID)
	_, _ = c.Store.Put(ctx, arenaKey, metadata.EncodeUint64(arenaID))
	return inos
}

func deleteArenaFile(t *testing.T, c *Cluster, dirIno uint64, name string, ino uint64) {
	ctx := t.Context()
	_ = c.Store.Delete(ctx, metadata.DirentKey(dirIno, name))
	extKey := fmt.Sprintf("extent:%d/0", ino)
	_ = c.Store.Delete(ctx, extKey)
}

// ---- C10.1: Compaction correctness ----

func TestCompaction_Correctness(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	arenaID := uint64(10)
	dirIno := uint64(2000)
	inos := setupArenaFiles(t, cluster, arenaID, dirIno, 20)

	extBefore := cluster.CountExtentsInArena(ctx, arenaID)
	assert.Equal(t, 20, extBefore)

	// Delete 14 of 20 files (70%) to drive utilization < 50%
	for i := 0; i < 14; i++ {
		name := fmt.Sprintf("a%d-f%03d", arenaID, i)
		deleteArenaFile(t, cluster, dirIno, name, inos[i])
	}

	extAfterDelete := cluster.CountExtentsInArena(ctx, arenaID)
	assert.LessOrEqual(t, extAfterDelete, 6)

	comp := compaction.New(store, "node-0")

	needs, candidates := comp.NeedsCompaction(ctx)
	if needs && len(candidates) > 0 {
		dstArena := uint64(11)
		count, err := comp.CompactArena(ctx, candidates[0], dstArena)
		require.NoError(t, err)
		assert.Greater(t, count, 0, "should remap some extents")
		t.Logf("compact: %d extents remapped", count)
	}

	// Surviving files should still be readable
	for i := 14; i < 20; i++ {
		name := fmt.Sprintf("a%d-f%03d", arenaID, i)
		ino := cluster.FreshLookup(ctx, dirIno, name)
		assert.Equal(t, inos[i], ino, "surviving file should still exist")
	}

	assert.Zero(t, cluster.checkAllInvariants())
	_ = arenaID
}

// ---- C10.4: Compaction batching (many files in one arena) ----

func TestCompaction_Batching(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	arenaID := uint64(20)
	dirIno := uint64(3000)

	// Create 1000 files in one arena
	diskStart := cluster.ArenaDiskStart(arenaID)
	for i := 0; i < 1000; i++ {
		ino := 200000 + uint64(i)
		name := fmt.Sprintf("bf-%04d", i)
		diskOff := diskStart + uint64(i)*arenaBlockSize

		extKey := fmt.Sprintf("extent:%d/0", ino)
		extVal := fmt.Sprintf("0,%d,%d,1", diskOff, arenaBlockSize)
		_, _ = store.Put(ctx, extKey, []byte(extVal))

		rec := &metadata.InodeRecord{
			Ino: ino, Mode: 0100644, Nlink: 1,
			Size: arenaBlockSize, Blksize: arenaBlockSize,
		}
		_, _ = store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
		_, _ = store.Put(ctx, metadata.DirentKey(dirIno, name), metadata.EncodeUint64(ino))
	}

	extBefore := cluster.CountExtentsInArena(ctx, arenaID)
	assert.Equal(t, 1000, extBefore)

	// Delete 600 files (60%)
	for i := 0; i < 600; i++ {
		name := fmt.Sprintf("bf-%04d", i)
		ino := 200000 + uint64(i)
		_ = store.Delete(ctx, metadata.DirentKey(dirIno, name))
		_ = store.Delete(ctx, fmt.Sprintf("extent:%d/0", ino))
	}

	// Compact: 400 files remapped in batched Txns (max 128 ops each)
	comp := compaction.New(store, "node-0")
	dstArena := uint64(21)
	count, err := comp.CompactArena(ctx, arenaID, dstArena)
	require.NoError(t, err)
	t.Logf("batching: %d extents remapped across batch Txns", count)

	// Verify 400 surviving files
	surviving := 0
	for i := 600; i < 1000; i++ {
		name := fmt.Sprintf("bf-%04d", i)
		ino := cluster.FreshLookup(ctx, dirIno, name)
		if ino != 0 {
			surviving++
		}
	}
	assert.Equal(t, 400, surviving, "all 400 surviving files should remain")
	assert.Zero(t, cluster.checkAllInvariants())
	_ = store
}
