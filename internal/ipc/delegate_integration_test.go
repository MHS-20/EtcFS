//go:build integration
// +build integration

package ipc

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// Write delegation buffers an inode's extents while this node holds its lock
// and publishes them in one transaction.  The comparisons that transaction
// carries are what make it safe, and they are built from the cached snapshot —
// so the snapshot's revisions have to describe etcd exactly, at every point a
// write could read them.
//
// The hard case is a buffer that fills mid-workload: the flush that empties it
// moves every key it wrote to a new revision, and anything the next write plans
// from a snapshot predating that flush compares against a revision etcd has
// already left behind.  That transaction can never commit, so the buffer is
// stuck with it, fsync fails from then on, and the inode is wedged.

// fsyncInode drives the fsync handler and returns its errno.
func fsyncInode(t *testing.T, svc *Service, ino uint64) int32 {
	t.Helper()
	var b buf
	b.w64(ino)
	resp, err := svc.handleFsync(context.Background(), b.b)
	if err != nil {
		t.Fatalf("fsync ino %d: %v", ino, err)
	}
	return int32(binary.BigEndian.Uint32(resp))
}

// writeAt writes one block and fails the test on an errno.
func writeAt(t *testing.T, svc *Service, ino, offset uint64, data []byte) {
	t.Helper()
	resp, err := svc.handleWrite(context.Background(), writePayload(ino, offset, data, 0))
	if err != nil {
		t.Fatalf("write ino %d at %d: %v", ino, offset, err)
	}
	if e := int32(binary.BigEndian.Uint32(resp)); e != 0 {
		t.Fatalf("write ino %d at %d: errno %d", ino, offset, e)
	}
}

// A random-overwrite workload fills the buffer faster than the flush interval
// does: every overwrite contributes both a new extent and the rewrite of the
// one it buries, so the transaction's op ceiling is reached first.  The flush
// that ceiling forces must leave the node able to keep writing.
func TestIntegration_WritesSurviveABufferFilledByOverwrites(t *testing.T) {
	svc, store := newTestService(t)
	const ino = 9301
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 4096)
	for i := range block {
		block[i] = byte(i)
	}

	// Lay the file out first, then overwrite it in place.  The layout writes
	// bury nothing and contribute one op each; the overwrites contribute two,
	// so the buffer's op ceiling falls inside the second loop.
	const blocks = 2 * maxWriteTxnOps
	for i := uint64(0); i < blocks; i++ {
		writeAt(t, svc, ino, i*4096, block)
	}
	for i := uint64(0); i < blocks; i++ {
		writeAt(t, svc, ino, i*4096, block)
	}

	if e := fsyncInode(t, svc, ino); e != 0 {
		t.Fatalf("fsync returned errno %d: the buffer could not be published, "+
			"so every write acknowledged since the last flush is stranded", e)
	}

	// Published is not the same as correct: the file has to read back as the
	// last write left it, at the size the writes imply.
	rec, extents, err := store.GetInodeAndExtents(context.Background(), ino)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if want := uint64(blocks) * 4096; rec.Size != want {
		t.Errorf("size = %d after %d blocks, want %d", rec.Size, blocks, want)
	}
	if len(extents) == 0 {
		t.Fatal("no extents published for a file that was written twice over")
	}
}

// The snapshot a write plans from must agree with etcd about every revision it
// compares against.  Checking it directly says which of the two drifted, where
// the test above only reports that a flush stopped being possible.
func TestIntegration_CachedRevisionsSurviveAFlush(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9302
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 4096)

	// Enough writes to force at least one flush from the buffer's op ceiling.
	for i := uint64(0); i <= maxWriteTxnOps; i++ {
		writeAt(t, svc, ino, i*4096, block)
	}
	if e := fsyncInode(t, svc, ino); e != 0 {
		t.Fatalf("fsync returned errno %d", e)
	}

	m := cachedMetaFor(svc, ino)
	if m == nil {
		t.Fatal("nothing cached after a flush, so the next write pays a read it should not")
	}

	_, published, err := store.GetInodeAndExtents(ctx, ino)
	if err != nil {
		t.Fatalf("read extents: %v", err)
	}
	revs := make(map[string]int64, len(published))
	for _, e := range published {
		revs[e.Key] = e.ModRevision
	}
	for _, e := range m.extents {
		rev, found := revs[e.Key]
		if !found {
			t.Errorf("extent %s is cached but not published", e.Key)
			continue
		}
		if e.ModRevision != rev {
			t.Errorf("extent %s cached at revision %d, etcd holds it at %d: "+
				"a write planning against it would build a comparison that can never pass",
				e.Key, e.ModRevision, rev)
		}
	}
}

// A write that costs the file its set-user-ID bits must not leave them readable
// to anyone while the extent that dropped them waits in the buffer.  Deferring
// the bytes is a durability trade; deferring the mode change is a privilege one,
// and a peer that executes the file in the meantime gets the old mode.
func TestIntegration_SetIDBitsAreClearedWithoutWaitingForAFlush(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9303
	seedFile(t, store, ino, 0o104777) // setuid + setgid

	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, []byte("x"), 65534)); err != nil {
		t.Fatalf("write: %v", err)
	}

	rec, err := store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		t.Fatalf("read inode: %v", err)
	}
	if rec.Mode&(metadata.S_ISUID|metadata.S_ISGID) != 0 {
		t.Errorf("mode = %o in etcd right after an unprivileged write, want the "+
			"set-user-ID and set-group-ID bits gone", rec.Mode)
	}
}
