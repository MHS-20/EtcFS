//go:build integration
// +build integration

package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// dirRev is the revision a directory's record currently carries, which is what
// says whether it has been written again.
func dirRev(t *testing.T, store *Store, ctx context.Context, ino uint64) int64 {
	t.Helper()
	_, rev, err := store.GetInodeRev(ctx, ino)
	require.NoError(t, err)
	return rev
}

// A directory's timestamp is owed a commit by every entry added to it, and
// coalescing those commits is only correct if the timestamp is still readable
// on this node straight away, still published eventually, and never moved
// backwards by a queued value that something newer has already overtaken.

// touchingStore returns a store whose directory timestamps are queued rather
// than written through, with a sweep too slow to fire during a test — the tests
// publish explicitly, so what is asserted is the queue rather than a race with
// a ticker.
func touchingStore(t *testing.T, node string) (*Store, context.Context) {
	t.Helper()
	store := testStore(t, node)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	store.StartDirTouchBatching(ctx, time.Hour, nil)
	return store, ctx
}

// seedDir writes a directory record with a known, old timestamp.
func seedDir(t *testing.T, store *Store, ctx context.Context, ino uint64, at time.Time) {
	t.Helper()
	rec := NewInodeRecord(ino, ModeDir|0755, 0, 0)
	rec.Mtime, rec.Ctime = at, at
	_, err := store.Put(ctx, InodeKey(ino), EncodeInode(rec))
	require.NoError(t, err)
}

func TestIntegration_DirTouchIsQueuedThenPublished(t *testing.T) {
	store, ctx := touchingStore(t, "node-1")
	const dir = uint64(500)
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	seedDir(t, store, ctx, dir, old)

	store.touchDir(ctx, dir)

	// Queued, so etcd still holds the old timestamp and this node answers with
	// the new one.
	seeded := &InodeRecord{Ino: dir, Mtime: old, Ctime: old}
	pending, queued := store.PendingInodeTimes(seeded)
	require.True(t, queued, "the touch should be queued rather than committed")
	assert.True(t, pending.Mtime.After(old))

	rec, err := store.GetInode(ctx, dir)
	require.NoError(t, err)
	assert.True(t, rec.Mtime.Equal(old), "etcd should not have been written yet")

	store.FlushInodeTimes(ctx, dir)

	_, queued = store.PendingInodeTimes(seeded)
	assert.False(t, queued, "the queue should be empty after a flush")
	rec, err = store.GetInode(ctx, dir)
	require.NoError(t, err)
	assert.True(t, rec.Mtime.After(old), "the flush should have published the timestamp")
}

func TestIntegration_DirTouchCoalescesManyEntries(t *testing.T) {
	store, ctx := touchingStore(t, "node-1")
	const dir = uint64(501)
	seedDir(t, store, ctx, dir, time.Now().Add(-time.Hour))

	before := dirRev(t, store, ctx, dir)

	for i := 0; i < 50; i++ {
		store.touchDir(ctx, dir)
	}
	assert.Equal(t, before, dirRev(t, store, ctx, dir),
		"fifty entries should not have written the record fifty times")

	store.FlushInodeTimes(ctx, 0)
	assert.Greater(t, dirRev(t, store, ctx, dir), before,
		"the flush should have written it exactly once")
}

func TestIntegration_DirTouchNeverMovesTheClockBackwards(t *testing.T) {
	store, ctx := touchingStore(t, "node-1")
	const dir = uint64(502)
	seedDir(t, store, ctx, dir, time.Now().Add(-time.Hour))

	store.touchDir(ctx, dir)

	// Something that folds the parent's timestamp into its own transaction —
	// a mkdir does exactly this — commits a newer one while the touch is still
	// queued.
	newer := time.Now().Add(time.Minute).Truncate(time.Second)
	rec, rev, err := store.GetInodeRev(ctx, dir)
	require.NoError(t, err)
	rec.Mtime, rec.Ctime = newer, newer
	ok, err := store.Txn(ctx, []clientv3.Cmp{InodeUnchanged(dir, rev)},
		[]clientv3.Op{clientv3.OpPut(InodeKey(dir), string(EncodeInode(rec)))}, nil)
	require.NoError(t, err)
	require.True(t, ok)

	store.FlushInodeTimes(ctx, dir)

	got, err := store.GetInode(ctx, dir)
	require.NoError(t, err)
	assert.True(t, got.Mtime.Equal(newer),
		"the stale queued timestamp must not overwrite a newer committed one")
}

func TestIntegration_DirTouchWritesThroughWithoutBatching(t *testing.T) {
	store := testStore(t, "node-1")
	ctx := context.Background()
	const dir = uint64(503)
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	seedDir(t, store, ctx, dir, old)

	store.touchDir(ctx, dir)

	_, queued := store.PendingInodeTimes(&InodeRecord{Ino: dir, Mtime: old, Ctime: old})
	assert.False(t, queued, "nothing should be queued when batching was never started")
	rec, err := store.GetInode(ctx, dir)
	require.NoError(t, err)
	assert.True(t, rec.Mtime.After(old), "the timestamp should have been committed inline")
}
