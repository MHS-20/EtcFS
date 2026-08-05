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

// TestElastic_ConcurrentJoin is the in-memory equivalent of
// scripts/test/chaos-elastic-concurrent.sh: several nodes join at the same
// time, against the same shared store, instead of one after another. The
// chaos script proves this against real containers/EC2 in minutes; this
// proves the same property against MockStore in milliseconds, so a
// regression here is caught by `go test ./...` rather than only by someone
// remembering to run the chaos script.
//
// Covers what TestElastic_ArenaPoolContention does not: concurrent inode
// allocation. Both membership.Manager.AcquireArena (arenas) and
// ReserveInodeRange (inodes) CAS against the same shared counter pattern
// under concurrency here.
//
// Note: ReserveInodeRange CASes against the same inode_alloc_counter key
// that the real request path's Service.allocInode -> Store.NextCounter
// uses, so this exercises the identical etcd primitive under contention —
// but ReserveInodeRange's own retry budget (5 attempts, no jitter) is looser
// than NextCounter's (20 attempts, backoff+jitter, added after a documented
// near-miss under load — see the comment on NextCounter in
// pkg/metadata/alloc.go). This test can't fail for a NextCounter-specific
// retry regression; it can only prove the shared counter never hands out
// the same value twice under concurrent CAS pressure.
func TestElastic_ConcurrentJoin(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	const nodes = 5
	const inodesPerNode = 1000
	var wg sync.WaitGroup

	type joinResult struct {
		nodeID   string
		arena    uint64
		lo, hi   uint64
		joinErr  error
		rangeErr error
	}
	results := make([]joinResult, nodes)

	for i := 0; i < nodes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("concurrent-join-%d", idx)
			mgr := membership.New(store, nodeID)

			joinErr := mgr.Join(ctx)
			rangeErr := mgr.ReserveInodeRange(ctx, 0, inodesPerNode)
			lo, hi := mgr.InodeRange(ctx, nodeID)

			arenaVal, _ := store.Get(ctx, metadata.ArenaKey(nodeID))
			var arena uint64
			if arenaVal != nil {
				arena = metadata.DecodeUint64(arenaVal)
			}

			results[idx] = joinResult{nodeID: nodeID, arena: arena, lo: lo, hi: hi, joinErr: joinErr, rangeErr: rangeErr}
		}(i)
	}
	wg.Wait()

	seenArenas := make(map[uint64]bool)
	type ivl struct{ lo, hi uint64 }
	ranges := make([]ivl, 0, nodes)

	for _, r := range results {
		require.NoError(t, r.joinErr, "join must succeed for %s", r.nodeID)
		require.NoError(t, r.rangeErr, "inode range reservation must succeed for %s", r.nodeID)

		assert.False(t, seenArenas[r.arena], "arena %d handed to more than one node", r.arena)
		seenArenas[r.arena] = true

		assert.Less(t, r.lo, r.hi, "%s got an empty or inverted range [%d,%d)", r.nodeID, r.lo, r.hi)
		ranges = append(ranges, ivl{r.lo, r.hi})
	}

	// No two nodes' inode ranges may overlap — that's the actual hazard a
	// broken CAS retry produces (two nodes creating files with the same
	// inode number).
	for i := 0; i < len(ranges); i++ {
		for j := i + 1; j < len(ranges); j++ {
			overlap := ranges[i].lo <= ranges[j].hi && ranges[j].lo <= ranges[i].hi
			assert.False(t, overlap, "inode ranges overlap: [%d,%d] and [%d,%d]",
				ranges[i].lo, ranges[i].hi, ranges[j].lo, ranges[j].hi)
		}
	}

	assert.Zero(t, cluster.checkAllInvariants())
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

// ---- Fault injection during join/leave (TODO-hardening.md item 3) ----
//
// These are the cheap, deterministic equivalent of two of the four
// scripts/test/chaos-elastic-fault-injection.sh scenarios (FJ1, FJ3). The
// other two (FJ2: partition mid-join, FJ4: kill a survivor while a
// different node is mid-join) have no meaningful equivalent against
// MockStore/membership.Manager — FJ2 needs a real self-fencing watchdog
// process to observe, and FJ4 needs a real daemon crash affecting a real
// FUSE mount, neither of which the harness's Join()/LeaveGraceful()
// abstraction models. Both are chaos-script-only; see the report.

// TestElastic_JoinInterruptedBeforeArena is the harness equivalent of FJ1:
// a node's daemon dies after registering membership but before its first
// write (arena acquisition is lazy — see pkg/arena/allocator.go,
// AcquireArena is only called from handleWriteBlock, not at startup — so
// "before FUSE mount" and "before any write" are the same instant from the
// allocator's perspective). Manager.Join() bundles membership registration
// and arena acquisition in one call with no way to stop between them from
// outside the package, so this simulates the interruption the same way
// registerMembership itself would: writing the membership key directly via
// the public Store, then simply never calling AcquireArena.
func TestElastic_JoinInterruptedBeforeArena(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	store := cluster.Store

	const nodeID = "half-joined"
	_, err := store.Put(ctx, metadata.MembershipKey(nodeID), []byte(`{"joined":1}`))
	require.NoError(t, err)

	// The crash happens here — no AcquireArena call ever happens.

	arenaVal, _ := store.Get(ctx, metadata.ArenaKey(nodeID))
	assert.Nil(t, arenaVal, "a node that never reached its first write must not hold an arena")

	// The rest of the cluster must be completely unaffected: another node
	// joining and creating files does not observe the half-joined node at
	// all (it has no arena, so nothing depends on reclaiming from it).
	mgr := membership.New(store, "node-other")
	require.NoError(t, mgr.Join(ctx))
	assert.True(t, mgr.IsMember(ctx, "node-other"))

	assert.Zero(t, cluster.checkAllInvariants())
}

// TestElastic_GenerationBumpDuringGracefulLeave is the harness equivalent
// of FJ3: a node's fencing generation is bumped (as the fencing controller
// would on a lease expiry) while it is in the middle of leaving gracefully.
//
// Caveat, stated plainly: MockStore has no SetGuard/guard-enforcement
// concept — unlike the production metadata.Store, LeaveGraceful's Delete/Put
// calls here are never rejected by a generation mismatch, because nothing in
// the harness checks one. This test can only verify the structural
// invariant (no orphaned arena, membership key gone, cluster consistent
// afterward), not that the bump actually blocks the leaving node's own
// writes — that guard-rejection behavior is real in production
// (verified by scripts/test/chaos-fencing-namespace.sh) but is not modeled
// by this harness type at all.
func TestElastic_GenerationBumpDuringGracefulLeave(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	const nodeID = "leaving-node"
	mgr := membership.New(store, nodeID)
	require.NoError(t, mgr.Join(ctx))

	arenaKey := metadata.ArenaKey(nodeID)
	arenaVal, _ := store.Get(ctx, arenaKey)
	require.NotNil(t, arenaVal, "node must hold an arena before it can leave")

	// Fencing controller bumps the generation mid-leave.
	_, err := store.BumpGeneration(ctx, nodeID, 0)
	require.NoError(t, err)

	err = mgr.LeaveGraceful(ctx)
	require.NoError(t, err)

	assert.False(t, mgr.IsMember(ctx, nodeID), "membership key must be gone after leave")
	arenaVal, _ = store.Get(ctx, arenaKey)
	assert.Nil(t, arenaVal, "arena must not be orphaned — released back to the free pool")

	assert.Zero(t, cluster.checkAllInvariants())
}
