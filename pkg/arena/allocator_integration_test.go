//go:build integration
// +build integration

// Integration tests for the arena allocator against a real etcd cluster.
//
// Requires a running etcd. Start one with:
//
//	docker run -d -p 2379:2379 quay.io/coreos/etcd:v3.5.18 \
//	  /usr/local/bin/etcd --data-dir=/etcd-data \
//	  --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://0.0.0.0:2379
//
// Run with:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration -count=1 ./pkg/arena/
package arena

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

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

// A node must never adopt another node's arena on restart.
//
// This is the allocator half of the Kleppmann stale-write hazard: if
// Reconstruct pulls in arenas owned by other nodes, two live nodes hand out
// the same disk offset and both extent commits succeed, because neither node
// is fenced and the generation guard has nothing to reject.  See
// docs/architecture/kleppmann-stale-write-analysis.md.
func TestIntegration_ReconstructDoesNotAdoptForeignArenas(t *testing.T) {
	ctx := context.Background()
	storeA := testStore(t, "node-A")
	storeB := testStore(t, "node-B")

	allocA := NewAllocator("node-A", storeA)
	allocB := NewAllocator("node-B", storeB)

	arenaA, err := allocA.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("node-A acquire: %v", err)
	}
	arenaB, err := allocB.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("node-B acquire: %v", err)
	}
	if arenaA.ID == arenaB.ID {
		t.Fatalf("two nodes were handed the same arena ID %d", arenaA.ID)
	}

	// node-A restarts and rebuilds its free-list from etcd.
	restartedA := NewAllocator("node-A", storeA)
	if err := restartedA.Reconstruct(ctx); err != nil {
		t.Fatalf("node-A reconstruct: %v", err)
	}

	if got := restartedA.ArenaCount(); got != 1 {
		t.Fatalf("node-A recovered %d arenas, want exactly its own 1", got)
	}
	for _, ar := range restartedA.arenas {
		if ar.ID == arenaB.ID {
			t.Fatalf("node-A adopted node-B's arena %d — foreign arena in free-list", ar.ID)
		}
	}

	// The offsets node-A hands out must stay outside node-B's byte range.
	off, err := restartedA.Allocate(BlockSize)
	if err != nil {
		t.Fatalf("node-A allocate after restart: %v", err)
	}
	if off >= arenaB.DiskStart && off < arenaB.DiskEnd {
		t.Fatalf("node-A allocated disk offset %d inside node-B's arena [%d,%d)",
			off, arenaB.DiskStart, arenaB.DiskEnd)
	}
}

// A malformed ownership record must not be read as arena 0, which the node
// probably does not own.  Compaction used to write an ASCII "id=%d" value here.
func TestIntegration_MalformedArenaRecordIsIgnored(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")

	if _, err := store.Put(ctx, metadata.ArenaKey("node-A"), []byte("id=7")); err != nil {
		t.Fatalf("seed malformed record: %v", err)
	}

	alloc := NewAllocator("node-A", store)
	if err := alloc.Reconstruct(ctx); err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if got := alloc.ArenaCount(); got != 0 {
		t.Fatalf("adopted %d arenas from a malformed record, want 0", got)
	}
}

// Arena ID 0 is a real arena — the global counter starts there — so a node
// owning it must recover it rather than treating 0 as "no record".
func TestIntegration_ArenaZeroIsRecovered(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")

	alloc := NewAllocator("node-A", store)
	first, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if first.ID != 0 {
		t.Skipf("counter did not start at 0 (got %d); test assumes a clean store", first.ID)
	}

	restarted := NewAllocator("node-A", store)
	if err := restarted.Reconstruct(ctx); err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if got := restarted.ArenaCount(); got != 1 {
		t.Fatalf("recovered %d arenas, want 1 (arena 0 must not be mistaken for 'no record')", got)
	}
}

// A freed arena must be reused before the counter is bumped for a new one —
// otherwise ClaimFreeArena is dead code and space never comes back.
func TestIntegration_ReleasedArenaIsReused(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := NewAllocator("node-A", store)

	first, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	arenaID, released, err := store.ReleaseArena(ctx, "node-A")
	if err != nil || !released || arenaID != first.ID {
		t.Fatalf("release: id=%d released=%v err=%v (want id=%d released=true)", arenaID, released, err, first.ID)
	}

	second, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("re-acquire got arena %d, want the freed arena %d back", second.ID, first.ID)
	}
}

// A recycled arena is not empty: its previous owner's live extents must be
// marked allocated before the new owner writes, or the new owner can hand out
// a block that still holds another inode's data.
func TestIntegration_RecycledArenaKeepsLiveExtentsMarked(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := NewAllocator("node-A", store)

	first, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	off, err := alloc.Allocate(BlockSize)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	// A live extent for an inode that still exists — the scrubber would not
	// reclaim it, and neither should a recycling node overwrite it.
	if _, err := store.Put(ctx, metadata.InodeKey(42), []byte("stub-inode")); err != nil {
		t.Fatalf("seed inode: %v", err)
	}
	if err := store.AppendExtent(ctx, 42, 0, off, BlockSize, 1); err != nil {
		t.Fatalf("append extent: %v", err)
	}

	if _, released, err := store.ReleaseArena(ctx, "node-A"); err != nil || !released {
		t.Fatalf("release: released=%v err=%v", released, err)
	}

	recycler := NewAllocator("node-B", metadata.NewStore(store.Client(), "node-B"))
	recycled, err := recycler.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("node-B acquire: %v", err)
	}
	if recycled.ID != first.ID {
		t.Skipf("arena %d was not recycled to node-B (got %d) — pool had another candidate", first.ID, recycled.ID)
	}

	// Allocating BlocksPerArena-1 more blocks must never return the offset the
	// old extent still occupies.
	for i := 0; i < 4; i++ {
		got, err := recycler.Allocate(BlockSize)
		if err != nil {
			t.Fatalf("node-B allocate %d: %v", i, err)
		}
		if got == off {
			t.Fatalf("node-B was handed disk_off=%d, which node-A's live extent still occupies", got)
		}
	}
}
