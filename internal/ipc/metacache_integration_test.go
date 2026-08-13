//go:build integration
// +build integration

package ipc

import (
	"context"
	"testing"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// The metadata a node caches under a held inode lock is what its next read is
// answered from, so any drift from etcd's own state is a wrong answer served
// without a round trip to catch it.  These tests compare the two after every
// shape of mutation the write path can commit.

// cachedMetaFor returns what the service has cached for an inode, or nil.
func cachedMetaFor(svc *Service, ino uint64) *inodeMeta {
	svc.lockMu.Lock()
	e := svc.locks[ino]
	svc.lockMu.Unlock()
	if e == nil {
		return nil
	}
	return e.cachedMeta()
}

// assertCacheMatchesEtcd fails if the cached view differs from a fresh read.
func assertCacheMatchesEtcd(t *testing.T, svc *Service, store *metadata.Store, ino uint64, stage string) {
	t.Helper()
	ctx := context.Background()

	m := cachedMetaFor(svc, ino)
	if m == nil {
		t.Fatalf("%s: nothing cached, so the next operation pays a read it should not", stage)
	}

	rec, extents, err := store.GetInodeAndExtents(ctx, ino)
	if err != nil {
		t.Fatalf("%s: read metadata: %v", stage, err)
	}
	if rec.Size != m.rec.Size || rec.Mode != m.rec.Mode {
		t.Errorf("%s: cached record (size %d mode %o) is not etcd's (size %d mode %o)",
			stage, m.rec.Size, m.rec.Mode, rec.Size, rec.Mode)
	}
	if len(extents) != len(m.extents) {
		t.Fatalf("%s: cached %d extents, etcd has %d", stage, len(m.extents), len(extents))
	}
	for i := range extents {
		if extents[i] != m.extents[i] {
			t.Errorf("%s: extent %d cached as %+v, etcd has %+v", stage, i, m.extents[i], extents[i])
		}
	}
}

// A write publishes its own outcome into the cache instead of re-reading it.
// Appends, overwrites that bury an earlier extent, and a write that splits one
// in two all go through the same replay, and all three have to land on exactly
// what etcd stored.
func TestIntegration_CachedMetadataMatchesEtcdAfterWrites(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9101
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 4096)
	for i := range block {
		block[i] = byte(i)
	}

	// Append, then extend: two extents, no burial.
	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, block, 0)); err != nil {
		t.Fatalf("write: %v", err)
	}
	assertCacheMatchesEtcd(t, svc, store, ino, "first write")

	if _, err := svc.handleWrite(ctx, writePayload(ino, 4096, block, 0)); err != nil {
		t.Fatalf("append: %v", err)
	}
	assertCacheMatchesEtcd(t, svc, store, ino, "append")

	// Full overwrite of the first extent: it is buried and reclaimed inside
	// the same transaction that publishes the new one.
	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, block, 0)); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	assertCacheMatchesEtcd(t, svc, store, ino, "overwrite")

	// A write landing strictly inside an extent splits it, so the replay has
	// to account for a delete and two puts at once.
	if _, err := svc.handleWrite(ctx, writePayload(ino, 5120, block[:1024], 0)); err != nil {
		t.Fatalf("split write: %v", err)
	}
	assertCacheMatchesEtcd(t, svc, store, ino, "split write")
}

// Every mutation that does not publish its outcome has to drop the cache
// instead, or the next read answers from a list the mutation invalidated.
func TestIntegration_MutationsThatDoNotPublishDropTheCache(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9102
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 8192)
	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, block, 0)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if cachedMetaFor(svc, ino) == nil {
		t.Fatal("write left nothing cached")
	}

	// Shrink through setattr, which rewrites extents without telling the cache.
	resp, err := svc.handleSetattr(ctx, setattrPayload(ino, fattrSize, 4096, 0, 0, 0, 0, 0, 0))
	if err != nil {
		t.Fatalf("setattr: %v", err)
	}
	if code := int32(resp[0]) | int32(resp[1])<<8 | int32(resp[2])<<16 | int32(resp[3])<<24; code != 0 {
		t.Fatalf("setattr returned %d", code)
	}
	if m := cachedMetaFor(svc, ino); m != nil {
		t.Fatalf("truncate left a stale snapshot cached: %+v", m.extents)
	}

	// The next read refills it, and what it refills with has to be etcd's.
	if _, err := svc.handleRead(ctx, readRequest(ino, 0, 8192)); err != nil {
		t.Fatalf("read: %v", err)
	}
	assertCacheMatchesEtcd(t, svc, store, ino, "read after truncate")
}

// readRequest builds a READ payload.
func readRequest(ino, offset uint64, size uint32) []byte {
	var b buf
	b.w64(ino)
	b.w64(offset)
	b.w32(size)
	return b.b
}
