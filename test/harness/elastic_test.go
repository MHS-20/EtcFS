package harness

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MHS-20/EtcFS/pkg/membership"
	"github.com/MHS-20/EtcFS/pkg/metadata"
)

func registerArena(ctx context.Context, store *MockStore, arenaID uint64) {
	key := fmt.Sprintf("arena:%d", arenaID)
	_, _ = store.Put(ctx, key, metadata.EncodeUint64(arenaID))
}

// ---- C10.5: Elastic join — new node ----

func TestElastic_JoinNewNode(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	store := cluster.Store

	mgr := membership.New(store, "node-3")

	// Join: registers membership, acquires arena
	err := mgr.Join(ctx)
	require.NoError(t, err)

	// Verify membership key exists
	memVal, _ := store.Get(ctx, metadata.MembershipKey("node-3"))
	assert.NotNil(t, memVal)

	// Verify arena was acquired for node-3
	arenaKey := metadata.ArenaKey("node-3")
	arenaVal, _ := store.Get(ctx, arenaKey)
	assert.NotNil(t, arenaVal)

	// Existing nodes should be unaffected
	assert.True(t, mgr.IsMember(ctx, "node-3"))
	assert.Zero(t, cluster.checkAllInvariants())
	_ = cluster
}

// ---- C10.6: Elastic join — warm cache time ----

func TestElastic_WarmCacheTime(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	cluster.createDirIfMissing(ctx, 1, "existing-dir", 50000)
	registerArena(ctx, store, 30)

	mgr := membership.New(store, "node-4")
	err := mgr.Join(ctx)
	require.NoError(t, err)

	assert.True(t, mgr.IsMember(ctx, "node-4"))

	entries := cluster.FreshListDir(ctx, 50000)
	assert.NotNil(t, entries)

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C10.7: Elastic leave — graceful ----

func TestElastic_LeaveGraceful(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	store := cluster.Store

	mgr := membership.New(store, "node-exit")
	_ = mgr.Join(ctx)
	require.True(t, mgr.IsMember(ctx, "node-exit"))

	arenaKey := metadata.ArenaKey("node-exit")
	arenaVal, _ := store.Get(ctx, arenaKey)
	assert.NotNil(t, arenaVal)

	err := mgr.LeaveGraceful(ctx)
	require.NoError(t, err)

	assert.False(t, mgr.IsMember(ctx, "node-exit"))

	arenaVal, _ = store.Get(ctx, arenaKey)
	assert.Nil(t, arenaVal, "arena should be released")

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C10.8: Elastic leave — ungraceful (SIGKILL) ----

func TestElastic_LeaveUngraceful(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	store := cluster.Store

	mgr := membership.New(store, "node-killed")
	_ = mgr.Join(ctx)
	require.True(t, mgr.IsMember(ctx, "node-killed"))

	arenaKey := metadata.ArenaKey("node-killed")
	arenaVal, _ := store.Get(ctx, arenaKey)
	assert.NotNil(t, arenaVal)

	mgr.LeaveUngraceful(ctx)

	assert.False(t, mgr.IsMember(ctx, "node-killed"))
	arenaVal, _ = store.Get(ctx, arenaKey)
	assert.Nil(t, arenaVal)

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C10.9: Arena rebalancing — manual advisory ----

func TestElastic_RebalanceArena(t *testing.T) {
	cluster := NewCluster(3)
	ctx := t.Context()
	store := cluster.Store

	mgrA := membership.New(store, "node-A")
	_ = mgrA.Join(ctx)

	mgrB := membership.New(store, "node-B")
	_ = mgrB.Join(ctx)

	arenaAVal, _ := store.Get(ctx, metadata.ArenaKey("node-A"))
	require.NotNil(t, arenaAVal)
	arenaAID := metadata.DecodeUint64(arenaAVal)

	arenaBVal, _ := store.Get(ctx, metadata.ArenaKey("node-B"))
	require.NotNil(t, arenaBVal)

	err := mgrA.RebalanceArena(ctx, "node-A", "node-B", arenaAID)
	require.NoError(t, err)

	arenaAValAfter, _ := store.Get(ctx, metadata.ArenaKey("node-A"))
	assert.Nil(t, arenaAValAfter)

	arenaBValAfter, _ := store.Get(ctx, metadata.ArenaKey("node-B"))
	assert.Equal(t, arenaAID, metadata.DecodeUint64(arenaBValAfter))

	assert.Zero(t, cluster.checkAllInvariants())
	_ = arenaBVal
}

// ---- C10.10: Inode range exhaustion + re-reservation ----

func TestElastic_InodeRangeExhaustion(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	mgr := membership.New(store, "node-inodes")

	// Reserve first range
	err := mgr.ReserveInodeRange(ctx, 0, 100000)
	require.NoError(t, err)

	lo, hi := mgr.InodeRange(ctx, "node-inodes")
	assert.Equal(t, uint64(0), lo)
	assert.Equal(t, uint64(99999), hi)

	// Reserve second range (exhaustion triggers re-reservation) — same global counter
	err = mgr.ReserveInodeRange(ctx, 0, 100000)
	require.NoError(t, err)

	lo, hi = mgr.InodeRange(ctx, "node-inodes")
	assert.Equal(t, uint64(100000), lo)
	assert.Equal(t, uint64(199999), hi)

	// Verify no overlap in reserved ranges
	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C10.11: Global arena pool under contention ----

func TestElastic_ArenaPoolContention(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	const nodes = 4
	var wg sync.WaitGroup
	arenas := make(map[string]uint64)
	var mu sync.Mutex

	for i := 0; i < nodes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("contend-%d", idx)
			mgr := membership.New(store, nodeID)
			_ = mgr.Join(ctx)

			arenaVal, _ := store.Get(ctx, metadata.ArenaKey(nodeID))
			if arenaVal != nil {
				mu.Lock()
				arenas[nodeID] = metadata.DecodeUint64(arenaVal)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	assert.Len(t, arenas, nodes, "all nodes should get an arena")
	// Ensure no duplicate arenas
	seen := make(map[uint64]bool)
	for _, id := range arenas {
		assert.False(t, seen[id], "arena %d should be unique", id)
		seen[id] = true
	}

	assert.Zero(t, cluster.checkAllInvariants())
	_ = cluster
}

// ---- Additional Tests ----

func TestElastic_MultipleJoinLeaveCycles(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	store := cluster.Store

	for cycle := 0; cycle < 5; cycle++ {
		nodeID := fmt.Sprintf("cycle-%d", cycle)
		mgr := membership.New(store, nodeID)

		_ = mgr.Join(ctx)
		assert.True(t, mgr.IsMember(ctx, nodeID))

		_ = mgr.LeaveGraceful(ctx)
		assert.False(t, mgr.IsMember(ctx, nodeID))
	}

	assert.Zero(t, cluster.checkAllInvariants())
	_ = cluster
}

func TestElastic_RebalanceIdempotent(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	mgrA := membership.New(store, "src")
	_ = mgrA.Join(ctx)
	mgrB := membership.New(store, "dst")
	_ = mgrB.Join(ctx)

	arenaAVal, _ := store.Get(ctx, metadata.ArenaKey("src"))
	arenaAID := metadata.DecodeUint64(arenaAVal)

	_ = mgrA.RebalanceArena(ctx, "src", "dst", arenaAID)

	// Rebalancing again should fail (src no longer has the arena)
	err := mgrA.RebalanceArena(ctx, "src", "dst", arenaAID)
	assert.Error(t, err)

	assert.Zero(t, cluster.checkAllInvariants())
}
