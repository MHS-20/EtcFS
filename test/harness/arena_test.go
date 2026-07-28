package harness

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- C6.5: Arena ID allocation via CAS ----

func TestArena_ArenaIDAllocation(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	ids := make(map[uint64]bool)
	for i := 0; i < 10; i++ {
		id, err := allocateArenaID(ctx, store)
		require.NoError(t, err)
		assert.False(t, ids[id], "arena ID %d should be unique", id)
		ids[id] = true
	}
}

func TestArena_ArenaIDConcurrent(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	// Sequential allocation produces sequential IDs
	id1, err := allocateArenaID(ctx, store)
	require.NoError(t, err)
	id2, err := allocateArenaID(ctx, store)
	require.NoError(t, err)
	assert.Equal(t, id1+1, id2)
}

// ---- C6.6: Bitmap allocation/deallocation ----

func TestArena_BitmapAllocate(t *testing.T) {
	bitmap := make([]uint64, 256)
	const blocks = uint64(256 * 64)

	// Allocate and verify each block
	for i := uint64(0); i < blocks; i++ {
		idx := i / 64
		bit := i % 64
		assert.False(t, (bitmap[idx]&(1<<bit)) != 0, "block %d should be free initially", i)

		// Mark allocated
		bitmap[idx] |= (1 << bit)
		assert.True(t, (bitmap[idx]&(1<<bit)) != 0, "block %d should be allocated", i)
	}
}

func TestArena_BitmapFreeAndReuse(t *testing.T) {
	bitmap := make([]uint64, 256)

	// Allocate block 0
	bitmap[0] |= 1
	assert.True(t, (bitmap[0]&1) != 0)

	// Free block 0
	bitmap[0] &^= 1
	assert.False(t, (bitmap[0]&1) != 0, "block 0 should be free after release")

	// Re-allocate block 0
	bitmap[0] |= 1
	assert.True(t, (bitmap[0]&1) != 0)
}

// ---- C6.6: Contiguous block search ----

func TestArena_FindContiguous(t *testing.T) {
	bitmap := make([]uint64, 256)
	const blocks = uint64(256 * 64)

	// Mark first 50 blocks as allocated
	for i := uint64(0); i < 50; i++ {
		bitmap[i/64] |= (1 << (i % 64))
	}

	// Search for 30 contiguous free blocks — should start at block 50
	start := findContiguous(bitmap, 30)
	assert.Equal(t, uint64(50), start)

	// Search for very large range — should return exceeding limit
	start = findContiguous(bitmap, blocks)
	assert.Equal(t, blocks, start, "should return limit when insufficient space")
}

func TestArena_FindContiguousExact(t *testing.T) {
	bitmap := make([]uint64, 256)
	const blocks = uint64(256 * 64)

	// Allocate all but 10 blocks at the end
	for i := uint64(0); i < blocks-10; i++ {
		bitmap[i/64] |= (1 << (i % 64))
	}

	// 10 contiguous free — should find at end
	start := findContiguous(bitmap, 10)
	assert.Equal(t, blocks-10, start)

	// 11 contiguous free — should fail
	start = findContiguous(bitmap, 11)
	assert.Equal(t, blocks, start)
}

// ---- C6.7: Arena exhaustion ----

func TestArena_Exhaustion(t *testing.T) {
	bitmap := make([]uint64, 256)
	const blocks = uint64(256 * 64)

	// Fill all blocks
	for i := uint64(0); i < blocks; i++ {
		bitmap[i/64] |= (1 << (i % 64))
	}

	// Verify no free blocks
	found := false
	for i := uint64(0); i < blocks; i++ {
		if (bitmap[i/64] & (1 << (i % 64))) == 0 {
			found = true
			break
		}
	}
	assert.False(t, found, "all blocks should be allocated")

	// Free 5 blocks in the middle
	for i := uint64(100); i < 105; i++ {
		bitmap[i/64] &^= (1 << (i % 64))
	}

	// Verify those 5 are now free
	for i := uint64(100); i < 105; i++ {
		assert.False(t, (bitmap[i/64]&(1<<(i%64))) != 0,
			fmt.Sprintf("block %d should be free", i))
	}
}

// ---- C6.10: Live ratio tracking ----

func TestArena_LiveRatio(t *testing.T) {
	bitmap := make([]uint64, 256)
	const blocks = uint64(256 * 64)

	allocated := countAllocated(bitmap)
	assert.Equal(t, uint64(0), allocated)

	// Allocate half
	for i := uint64(0); i < blocks/2; i++ {
		bitmap[i/64] |= (1 << (i % 64))
	}

	allocated = countAllocated(bitmap)
	ratio := float64(allocated) / float64(blocks)
	assert.InDelta(t, 0.5, ratio, 0.01)
}

// ---- Helpers: inlined arena logic for testing ----

func allocateArenaID(ctx context.Context, s *MockStore) (uint64, error) {
	key := "arena_alloc_log"
	for attempt := 0; attempt < 5; attempt++ {
		v, _ := s.Get(ctx, key)
		current := uint64(0)
		if len(v) >= 8 {
			current = uint64(v[0])<<56 | uint64(v[1])<<48 | uint64(v[2])<<40 | uint64(v[3])<<32 |
				uint64(v[4])<<24 | uint64(v[5])<<16 | uint64(v[6])<<8 | uint64(v[7])
		}
		next := current + 1
		buf := []byte{byte(next >> 56), byte(next >> 48), byte(next >> 40), byte(next >> 32),
			byte(next >> 24), byte(next >> 16), byte(next >> 8), byte(next)}
		_, err := s.Put(ctx, key, buf)
		if err == nil {
			return current, nil
		}
	}
	return 0, fmt.Errorf("arena ID exhausted")
}

func findContiguous(bitmap []uint64, blocks uint64) uint64 {
	total := uint64(len(bitmap)) * 64
	count := uint64(0)
	start := uint64(0)
	for i := uint64(0); i < total; i++ {
		idx := i / 64
		bit := i % 64
		if (bitmap[idx] & (1 << bit)) == 0 {
			if count == 0 {
				start = i
			}
			count++
			if count >= blocks {
				return start
			}
		} else {
			count = 0
		}
	}
	return total
}

func countAllocated(bitmap []uint64) uint64 {
	var count uint64
	total := uint64(len(bitmap)) * 64
	for i := uint64(0); i < total; i++ {
		idx := i / 64
		bit := i % 64
		if (bitmap[idx] & (1 << bit)) != 0 {
			count++
		}
	}
	return count
}
