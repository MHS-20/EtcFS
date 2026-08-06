//go:build integration
// +build integration

// Fencing controller integration tests.
//
// These need real etcd because the property under test is the *ordering* of
// two effects — the volume detach and the generation bump — and the second is
// only observable as committed etcd state.
//
// Run with:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration -count=1 -v ./pkg/fencing/
package fencing

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/pkg/metadata"
)

func testController(t *testing.T, nodeID string) (*Controller, *metadata.Store, context.Context) {
	t.Helper()

	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "http://localhost:2379"
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err, "cannot connect to etcd at %s", endpoints)

	t.Cleanup(func() {
		cli.Delete(context.Background(), metadata.PrefixGen, clientv3.WithPrefix())
		cli.Delete(context.Background(), metadata.PrefixMembership, clientv3.WithPrefix())
		cli.Delete(context.Background(), metadata.PrefixFencePending, clientv3.WithPrefix())
		cli.Delete(context.Background(), metadata.PrefixFenceClaim, clientv3.WithPrefix())
		cli.Close()
	})

	store := metadata.NewStore(cli, nodeID)
	mem := metadata.NewMembership(cli, nodeID, "test-cluster", 10*time.Second)
	return NewController(store, mem, config.NewLogger(0)), store, context.Background()
}

// stubFencer records what it was asked to do and fails on demand.
type stubFencer struct {
	called     int
	nodeID     string
	instanceID string
	err        error
}

func (s *stubFencer) Fence(_ context.Context, nodeID, instanceID string) error {
	s.called++
	s.nodeID = nodeID
	s.instanceID = instanceID
	return s.err
}

// The safety property: a confirmed detach is a precondition of the bump, not
// a parallel action. Bumping first would tell the cluster it may reclaim the
// node's arenas while the node might still be writing to them.
func TestController_BumpsOnlyAfterConfirmedDetach(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	stub := &stubFencer{}
	c.SetFencer(stub)

	c.fenceNode(ctx, "dead-node", "i-0123456789", false)

	assert.Equal(t, 1, stub.called, "the fence must be attempted")
	assert.Equal(t, "dead-node", stub.nodeID)
	assert.Equal(t, "i-0123456789", stub.instanceID)

	gen, err := store.GetGeneration(ctx, "dead-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen, "generation must be bumped once the detach is confirmed")
}

// The failure direction that matters. An unconfirmed detach means the node may
// still be writing; advertising it as fenced is worse than admitting the fence
// did not happen, because the fenced flag is what authorises peers to reclaim
// its arenas and locks.
func TestController_DoesNotBumpWhenDetachFails(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	stub := &stubFencer{err: errors.New("still attached after 60s")}
	c.SetFencer(stub)

	c.fenceNode(ctx, "wedged-node", "i-0123456789", false)

	assert.Equal(t, 1, stub.called)

	gen, err := store.GetGeneration(ctx, "wedged-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), gen,
		"generation must stay put when the volume was not confirmed detached")
}

// A node whose membership key predates instance-ID recording (rolling
// upgrade) cannot be detached, so it must not be reported as fenced either.
// The instance ID is the EBS path's requirement, not the controller's — an
// NVMeFencer needs no instance at all — so the refusal now comes from the
// detacher, and this test drives the real one to prove the controller honours
// it.
func TestController_DoesNotBumpWithoutInstanceID(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	c.SetFencer(&EBSDetacher{api: &fakeEC2{}, volumeID: "vol-test"})

	c.fenceNode(ctx, "legacy-node", "", false)

	gen, err := store.GetGeneration(ctx, "legacy-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), gen)
}

// Without a fencer configured (Docker, bare metal) the controller keeps its
// previous single-signal behaviour rather than refusing to fence at all.
func TestController_SingleSignalWhenNoFencer(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")

	c.fenceNode(ctx, "plain-node", "", false)

	gen, err := store.GetGeneration(ctx, "plain-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen,
		"no detacher means fence on lease expiry alone, as before")
}

func TestController_InstanceIDRoundTripsThroughMembershipValue(t *testing.T) {
	// The controller reads the instance ID out of the deleted key's previous
	// value, so the encoding written by Membership must be readable by the
	// extractor. A silent mismatch here would disable detaching entirely
	// while every test that stubs the value directly still passed.
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "http://localhost:2379"
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer cli.Close()
	defer cli.Delete(context.Background(), metadata.PrefixMembership, clientv3.WithPrefix())

	ctx := context.Background()
	mem := metadata.NewMembership(cli, "rt-node", "test-cluster", 10*time.Second)
	mem.SetInstanceID("i-roundtrip")

	runCtx, cancel := context.WithCancel(ctx)
	go mem.Run(runCtx)
	defer cancel()

	var raw []byte
	for i := 0; i < 50; i++ {
		resp, gerr := cli.Get(ctx, metadata.MembershipKey("rt-node"))
		require.NoError(t, gerr)
		if len(resp.Kvs) > 0 {
			raw = resp.Kvs[0].Value
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NotEmpty(t, raw, "membership key was never written")

	assert.Equal(t, "i-roundtrip", metadata.InstanceIDFromMembership(raw))
}

func TestInstanceIDFromMembership_MissingFieldIsEmpty(t *testing.T) {
	// An older node's value has no instance_id at all; that must read as
	// empty rather than panicking or returning garbage.
	legacy := []byte(`{"node_id":"n1","cluster":"c","joined_at":"2026-01-01T00:00:00Z"}`)
	assert.Equal(t, "", metadata.InstanceIDFromMembership(legacy))
}

// The retry gap this mechanism closes: the membership watch is edge-triggered,
// so a fence that fails has no event left to re-trigger it. The intent record
// is what survives the failed attempt, and the sweep is what consumes it.
func TestController_SweepRetriesFailedFence(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	stub := &stubFencer{err: errors.New("preempt timed out")}
	c.SetFencer(stub)

	require.NoError(t, store.RecordFenceIntent(ctx, "wedged-node", "i-0123456789"))
	c.fenceNode(ctx, "wedged-node", "i-0123456789", true)

	gen, err := store.GetGeneration(ctx, "wedged-node")
	require.NoError(t, err)
	require.Equal(t, uint64(0), gen, "precondition: the first attempt did not fence")

	// The device comes back; the sweep must pick the owed fence up unprompted.
	stub.err = nil
	c.reconcile(ctx)

	assert.Equal(t, 2, stub.called, "the sweep must re-attempt the failed fence")
	gen, err = store.GetGeneration(ctx, "wedged-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen, "the retry must complete the fence")
}

// The intent is the record of a fence that is *owed*; leaving it after a
// successful fence would make the sweep re-fence the node forever.
func TestController_SuccessfulFenceClearsIntent(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	c.SetFencer(&stubFencer{})

	require.NoError(t, store.RecordFenceIntent(ctx, "dead-node", "i-0123456789"))
	c.fenceNode(ctx, "dead-node", "i-0123456789", true)

	intents, err := store.ListFenceIntents(ctx)
	require.NoError(t, err)
	assert.NotContains(t, intents, "dead-node", "a completed fence owes nothing")
}

// A node that re-registered holds a live lease again, so it recovered from the
// expiry that triggered the fence. Severing its device access at that point
// would take down a healthy node.
func TestController_SweepDropsIntentWhenNodeRejoins(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	stub := &stubFencer{}
	c.SetFencer(stub)

	require.NoError(t, store.RecordFenceIntent(ctx, "rejoined-node", "i-0123456789"))
	_, err := store.Put(ctx, metadata.MembershipKey("rejoined-node"),
		[]byte(`{"node_id":"rejoined-node","instance_id":"i-0123456789"}`))
	require.NoError(t, err)

	c.reconcile(ctx)

	assert.Equal(t, 0, stub.called, "a re-registered node must not be fenced")
	gen, err := store.GetGeneration(ctx, "rejoined-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), gen)
	intents, err := store.ListFenceIntents(ctx)
	require.NoError(t, err)
	assert.NotContains(t, intents, "rejoined-node")
}

// The race half of the gap: every survivor sees the same DELETE event, so
// dedup has to be cluster-wide, not the per-process map it used to be.
func TestController_ConcurrentControllersFenceOnce(t *testing.T) {
	c1, store, ctx := testController(t, "survivor-1")
	c2, _, _ := testController(t, "survivor-2")
	stub1, stub2 := &stubFencer{}, &stubFencer{}
	c1.SetFencer(stub1)
	c2.SetFencer(stub2)

	require.NoError(t, store.RecordFenceIntent(ctx, "dead-node", "i-0123456789"))

	// c1 holds the claim for the whole of c2's attempt.
	leaseID, won, err := store.ClaimFence(ctx, "dead-node", c1.claimTTL)
	require.NoError(t, err)
	require.True(t, won)

	c2.fenceNode(ctx, "dead-node", "i-0123456789", true)
	assert.Equal(t, 0, stub2.called, "the loser of the claim must not fence")

	require.NoError(t, store.ReleaseFenceClaim(ctx, leaseID))
	c1.fenceNode(ctx, "dead-node", "i-0123456789", true)
	assert.Equal(t, 1, stub1.called)

	gen, err := store.GetGeneration(ctx, "dead-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen, "exactly one fence must have landed")
}

// The TOCTOU the claim alone does not close: a sweep decides what to fence
// from a ListFenceIntents snapshot, and that snapshot can go stale while the
// call waits on the claim. Without the post-claim re-check, a straggler
// replays a fence another controller already finished — a second real preempt
// or detach, and a second generation bump.
func TestController_SweepSkipsFenceCompletedWhileWaitingForClaim(t *testing.T) {
	c, store, ctx := testController(t, "straggler")
	stub := &stubFencer{}
	c.SetFencer(stub)

	// Exactly the state a straggler wakes up to: it listed the intent, another
	// controller then completed the fence (bumped, cleared, released), and the
	// claim is free again by the time this call reaches it.
	require.NoError(t, store.PutGeneration(ctx, "dead-node", 1))

	c.fenceNode(ctx, "dead-node", "i-0123456789", true)

	assert.Equal(t, 0, stub.called,
		"a fence already completed must not be re-issued against the device")
	gen, err := store.GetGeneration(ctx, "dead-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen, "the generation must not be bumped a second time")
}

// The watch path must stay unguarded by that re-check: it acts on a DELETE
// event it observed itself, so there is no stale snapshot, and an intent that
// failed to record must not silently disable fencing.
func TestController_WatchPathFencesWithoutARecordedIntent(t *testing.T) {
	c, store, ctx := testController(t, "watcher")
	stub := &stubFencer{}
	c.SetFencer(stub)

	c.fenceNode(ctx, "dead-node", "i-0123456789", false)

	assert.Equal(t, 1, stub.called, "the watch path must fence regardless of intent state")
	gen, err := store.GetGeneration(ctx, "dead-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen)
}
