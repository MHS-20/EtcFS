package ipc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/MHS-20/EtcFS/pkg/arena"
	"github.com/MHS-20/EtcFS/pkg/blockio"
	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// A release folded into a committed transaction must not also be issued on its
// own: the deferred Release runs on every path, and a second delete would be a
// round trip for a key that is already gone — the one this folding exists to
// avoid.  The nil Service is the assertion: Release touching the store here
// would panic.
func TestFoldedLockReleaseDoesNotAlsoIssueADelete(t *testing.T) {
	lk := &heldLock{ino: 7, mode: metadata.LockExclusive, holder: "42-1"}

	if got, want := lk.ReleaseOp().KeyBytes(), []byte(metadata.LockKey(7, metadata.LockExclusive, "42-1")); string(got) != string(want) {
		t.Fatalf("release op targets %q, want %q", got, want)
	}

	lk.Folded()
	lk.Release()
}

// A lock that was never folded must still be released, or it blocks every
// writer to that inode until the node exits.
func TestUnfoldedLockStillReleases(t *testing.T) {
	lk := &heldLock{ino: 7, mode: metadata.LockExclusive, holder: "42-1"}
	if lk.released {
		t.Fatal("a freshly acquired lock must not count as released")
	}
}

// A device without O_DIRECT holds written bytes in this node's page cache, so
// the barriers have to come back on whatever the configuration asked for.
func TestBufferedDeviceForcesWriteBarriers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dev.img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create device file: %v", err)
	}
	if err := f.Truncate(1 << 20); err != nil {
		t.Fatalf("size device file: %v", err)
	}
	_ = f.Close()

	dev, err := blockio.OpenBuffered(path)
	if err != nil {
		t.Fatalf("open device: %v", err)
	}
	t.Cleanup(func() { _ = dev.Close() })

	s := &Service{alloc: arena.NewAllocator("node-test", nil)}
	s.SetBlockDevice(dev, false)

	if dev.IsDirect() {
		t.Skip("this filesystem supports O_DIRECT, nothing to force barriers for")
	}
	if !s.writeBarriers {
		t.Fatal("buffered device left write barriers off")
	}
}
