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

	c.fenceNode(ctx, "dead-node", "i-0123456789")

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

	c.fenceNode(ctx, "wedged-node", "i-0123456789")

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

	c.fenceNode(ctx, "legacy-node", "")

	gen, err := store.GetGeneration(ctx, "legacy-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), gen)
}

// Without a fencer configured (Docker, bare metal) the controller keeps its
// previous single-signal behaviour rather than refusing to fence at all.
func TestController_SingleSignalWhenNoFencer(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")

	c.fenceNode(ctx, "plain-node", "")

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
