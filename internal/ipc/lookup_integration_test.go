//go:build integration
// +build integration

package ipc

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// A lookup answers three different situations, and the difference between them
// is the whole point of caching an absence: a name that is there, a name that
// is definitely not, and a name whose fate the store could not report.  Only
// the middle one may be cached, because only it is a fact.

func lookupReply(t *testing.T, svc *Service, parent uint64, name string) (errno int32, ino uint64, entryTimeout uint32) {
	t.Helper()
	resp, err := svc.handleLookup(context.Background(), lookupPayload(parent, name))
	if err != nil {
		t.Fatalf("lookup %d/%q: %v", parent, name, err)
	}
	errno = int32(binary.BigEndian.Uint32(resp[0:4]))
	if errno != 0 {
		return errno, 0, 0
	}
	if len(resp) != len(entryResp(0, &metadata.InodeRecord{})) {
		t.Fatalf("lookup %d/%q: reply is %d bytes, the C parser reads %d",
			parent, name, len(resp), len(entryResp(0, &metadata.InodeRecord{})))
	}
	return errno, binary.BigEndian.Uint64(resp[4:12]),
		binary.BigEndian.Uint32(resp[len(resp)-8 : len(resp)-4])
}

// A name that was never created is reported as absent in a form the kernel can
// remember, so the next probe for it never reaches this handler at all.  This
// is the case a build system generates thousands of times a second while
// walking an include path.
func TestLookupOfAMissingNameIsCacheable(t *testing.T) {
	svc, _ := newTestService(t)

	errno, ino, timeout := lookupReply(t, svc, metadata.RootIno, "nothing-here")
	if errno != 0 {
		t.Fatalf("errno = %d, want 0: an absence the store confirmed is an answer, not a failure", errno)
	}
	if ino != 0 {
		t.Errorf("ino = %d, want 0", ino)
	}
	if timeout == 0 {
		t.Error("entry_timeout = 0, so the kernel caches nothing and every probe costs an etcd read")
	}
}

// The cached absence must not survive the name coming into existence.  The
// kernel's copy is dropped by the dirent watch; what this pins is the half the
// daemon owns — that it never answers from anything but the store, so a lookup
// after a create reports the new inode immediately.
func TestLookupStopsBeingNegativeOnceTheNameExists(t *testing.T) {
	svc, store := newTestService(t)
	const name = "appears-later"

	if _, ino, _ := lookupReply(t, svc, metadata.RootIno, name); ino != 0 {
		t.Fatalf("ino = %d before the create, want 0", ino)
	}

	rec, err := store.AtomicCreateFile(context.Background(), metadata.RootIno, name, 4242, 0o100644, 1000, 1000)
	if err != nil {
		t.Fatalf("create %q: %v", name, err)
	}

	errno, ino, _ := lookupReply(t, svc, metadata.RootIno, name)
	if errno != 0 {
		t.Fatalf("errno = %d after the create, want 0", errno)
	}
	if ino != rec.Ino {
		t.Errorf("ino = %d, want %d: the daemon answered from something other than the store", ino, rec.Ino)
	}
}

// A dirent that names an inode with no record is a broken filesystem, not an
// absent name.  Reporting it as a cacheable absence would let the kernel serve
// ENOENT for a file that exists, so it stays an error.
func TestLookupOfADanglingDirentIsNotCacheable(t *testing.T) {
	svc, store := newTestService(t)
	const name = "dangling"

	if err := store.CreateDirent(context.Background(), metadata.RootIno, name, 999999); err != nil {
		t.Fatalf("create dirent: %v", err)
	}

	errno, _, _ := lookupReply(t, svc, metadata.RootIno, name)
	if errno == 0 {
		t.Fatal("a dirent pointing at a missing inode was answered as a valid entry")
	}
}
