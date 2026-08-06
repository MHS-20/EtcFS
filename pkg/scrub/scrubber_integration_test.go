//go:build integration
// +build integration

// Integration tests for the scrubber's orphan-extent block reclamation.
//
// Requires a running etcd. Start one with:
//
//	docker run -d -p 2379:2379 quay.io/coreos/etcd:v3.5.18 \
//	  /usr/local/bin/etcd --data-dir=/etcd-data \
//	  --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://0.0.0.0:2379
//
// Run with:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration -count=1 ./pkg/scrub/
package scrub

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/arena"
	"github.com/MHS-20/EtcFS/pkg/metadata"
)

type testLogger struct{}

func (testLogger) Warn(string, ...any)  {}
func (testLogger) Info(string, ...any)  {}
func (testLogger) Error(string, ...any) {}

func testStore(t *testing.T, nodeID string) *metadata.Store {
	t.Helper()

	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "http://localhost:2379"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("cannot connect to etcd at %s: %v", endpoints, err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		cli.Delete(ctx, metadata.PrefixArena, clientv3.WithPrefix())
		cli.Delete(ctx, metadata.PrefixFreeArena, clientv3.WithPrefix())
		cli.Delete(ctx, metadata.PrefixExtent, clientv3.WithPrefix())
		cli.Delete(ctx, metadata.PrefixInode, clientv3.WithPrefix())
		cli.Delete(ctx, metadata.PrefixArenaLog)
		cli.Close()
	})

	return metadata.NewStore(cli, nodeID)
}

// Deleting an unlinked file's dangling extent key must also return its
// blocks to the allocator — otherwise disk space leaks on every deletion,
// which is the hotter of the two paths item 6 closes (see
// docs/TODO-hardening.md § 6).
func TestIntegration_OrphanReclaimReturnsBlocksToAllocator(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := arena.NewAllocator("node-A", store)

	if _, err := alloc.AcquireArena(ctx); err != nil {
		t.Fatalf("acquire arena: %v", err)
	}
	off, err := alloc.Allocate(arena.BlockSize)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	// No inode record for ino 99 — this is what makes the extent an orphan:
	// AtomicUnlink already removed it, but the extent key survived.
	if err := store.AppendExtent(ctx, 99, 0, off, arena.BlockSize, 1); err != nil {
		t.Fatalf("append orphan extent: %v", err)
	}

	before := alloc.LiveRatio()

	s := New(store, "node-A", time.Hour, testLogger{})
	s.SetReclaimer(alloc)
	s.RunScrubPass(ctx)

	after := alloc.LiveRatio()
	if after >= before {
		t.Fatalf("live ratio did not drop after orphan reclaim: before=%f after=%f", before, after)
	}

	// The freed block must be reachable by a fresh allocation, not just
	// unmarked — Free and Allocate share one bitmap.
	got, err := alloc.Allocate(arena.BlockSize)
	if err != nil {
		t.Fatalf("allocate after reclaim: %v", err)
	}
	if got != off {
		t.Fatalf("reclaimed block %d not reissued, got %d instead", off, got)
	}

	kvs, err := store.GetPrefix(ctx, metadata.PrefixExtent)
	if err != nil {
		t.Fatalf("get extents: %v", err)
	}
	if len(kvs) != 0 {
		t.Fatalf("orphan extent key not deleted, %d remain", len(kvs))
	}
}

// Without a Reclaimer the scrubber must still clean up the metadata — the
// block leak is the documented degraded behaviour, not a crash.
func TestIntegration_OrphanReclaimWithoutReclaimerStillDeletesKey(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := arena.NewAllocator("node-A", store)

	if _, err := alloc.AcquireArena(ctx); err != nil {
		t.Fatalf("acquire arena: %v", err)
	}
	off, err := alloc.Allocate(arena.BlockSize)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if err := store.AppendExtent(ctx, 99, 0, off, arena.BlockSize, 1); err != nil {
		t.Fatalf("append orphan extent: %v", err)
	}

	s := New(store, "node-A", time.Hour, testLogger{})
	s.RunScrubPass(ctx)

	kvs, err := store.GetPrefix(ctx, metadata.PrefixExtent)
	if err != nil {
		t.Fatalf("get extents: %v", err)
	}
	if len(kvs) != 0 {
		t.Fatalf("orphan extent key not deleted without a reclaimer, %d remain", len(kvs))
	}
}
