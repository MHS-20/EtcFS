package verify

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

// A node that is fenced, restarts, adopts the new generation and resumes
// writing is behaving exactly as designed. The recorder appends to the same
// file under the same node ID across a restart, so the model must not read
// the resumed commits as writes from the fenced incarnation.
func TestAuditGenerationRestartIsNotAViolation(t *testing.T) {
	ops := []GuardOp{
		gop("n1", 0, true, false, 10, 20), // healthy, gen 0
		gop("n1", 0, false, true, 30, 40), // fenced: cluster moved to gen 1
		gop("n1", 1, true, false, 50, 60), // restarted, adopted gen 1, writes
	}
	if CheckGenerations(ops, timeout) != porcupine.Ok {
		t.Fatal("a legitimate restart-after-fence was reported as a violation")
	}
}

// The real violation must still be caught: a commit from the SAME incarnation
// (same cached generation) after that incarnation was fenced.
func TestAuditGenerationSameIncarnationAfterFenceStillRejected(t *testing.T) {
	ops := []GuardOp{
		gop("n1", 0, true, false, 10, 20),
		gop("n1", 0, false, true, 30, 40),
		gop("n1", 0, true, false, 50, 60), // same gen 0 — impossible, it was fenced
	}
	if CheckGenerations(ops, timeout) == porcupine.Ok {
		t.Fatal("a post-fence commit from the fenced incarnation was accepted")
	}
}

// Clock offset between two hosts must not read as two holders. Node A's
// release and node B's acquire are genuinely ordered, but B's clock is behind
// enough that the recorded intervals overlap. Recording each event over the
// interval its transaction spanned is what absorbs this; as zero-width points
// it was a false violation.
func TestAuditLockToleratesClockSkewBetweenNodes(t *testing.T) {
	ops := []LockOp{
		{Node: "n1", Ino: 1, Kind: lockAcquireExclusive, Call: 10, Ret: 20},
		{Node: "n1", Ino: 1, Kind: lockReleaseExclusive, Call: 50, Ret: 70},
		// n2's clock runs behind, so its acquire is stamped inside n1's release.
		{Node: "n2", Ino: 1, Kind: lockAcquireExclusive, Call: 60, Ret: 80},
		{Node: "n2", Ino: 1, Kind: lockReleaseExclusive, Call: 90, Ret: 100},
	}
	if !lockOK(t, ops) {
		t.Fatal("clock offset within the operations' own duration was read as a violation")
	}
}

// The tolerance must not swallow a real overlap: an acquire that completes
// well before the previous holder even begins releasing cannot be explained
// by skew, and must still be rejected.
func TestAuditLockStillCatchesRealOverlap(t *testing.T) {
	ops := []LockOp{
		{Node: "n1", Ino: 1, Kind: lockAcquireExclusive, Call: 10, Ret: 20},
		{Node: "n2", Ino: 1, Kind: lockAcquireExclusive, Call: 30, Ret: 40},
		{Node: "n1", Ino: 1, Kind: lockReleaseExclusive, Call: 50, Ret: 60},
		{Node: "n2", Ino: 1, Kind: lockReleaseExclusive, Call: 70, Ret: 80},
	}
	if lockOK(t, ops) {
		t.Fatal("two genuinely overlapping exclusive holders were accepted")
	}
}

// A node killed while holding a lock never records a release: etcd's lease
// expiry drops the key, and no code path in the dead process can log it. The
// next holder is then legitimate, and the checker must not report the gap as
// a mutual-exclusion violation.
func TestAuditLockSurvivesHolderKilledMidHold(t *testing.T) {
	ops := []LockOp{
		{Node: "n1", Ino: 1, Kind: lockAcquireExclusive, Call: 10, Ret: 20},
		// n1 is SIGKILLed here. No release is ever recorded for it.
		{Node: "n2", Ino: 1, Kind: lockAcquireExclusive, Call: 100, Ret: 110},
		{Node: "n2", Ino: 1, Kind: lockReleaseExclusive, Call: 120, Ret: 130},
	}
	if !lockOK(t, ops) {
		t.Fatal("a lock freed by lease expiry after its holder died was read as still held")
	}
}

// Truncation is not a write, but it changes what a read returns. A file
// written, truncated, and partly rewritten reads as zeroes past the new data
// -- and shell redirection (`>`), which the chaos suite uses throughout,
// opens with O_TRUNC. Without modelling it the extent model holds bytes that
// no longer exist and reports the correct read as a violation.
func TestAuditExtentHandlesTruncation(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 0, []byte("AAAAA"), 10, 20),
		{Node: "n1", Ino: 1, Kind: ExtentTruncate, Off: 0, Call: 30, Ret: 40},
		wop("n1", 1, 0, []byte("B"), 50, 60),
		rop("n1", 1, 0, []byte{'B', 0, 0, 0, 0}, 70, 80),
	}
	if !extentOK(t, ops) {
		t.Fatal("a read after truncation was reported as contradicting the pre-truncation bytes")
	}
}

// rdop builds one page of a directory listing starting at offset.
func rdop(node string, parent, offset uint64, call, ret int64, entries ...DirEntry) Op {
	return Op{
		Kind: KindReaddir, Node: node, Parent: parent, Offset: offset,
		Entries: entries, Call: call, Ret: ret,
	}
}

// A readdir is paginated, so a page that stops short of a name the model knows
// about proves nothing about it: the name is simply on a later page. Treating
// a page as a complete listing would report every large directory as missing
// entries.
func TestAuditReaddirPageIsNotACompleteListing(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "aaa", 41, ok, 10, 20),
		op(KindCreate, "n1", 1, "zzz", 42, ok, 25, 30),
		// A page covering only the start of the directory. "zzz" sorts past
		// its last entry, so its absence here says nothing.
		rdop("n1", 1, 0, 40, 50, DirEntry{Name: "aaa", Ino: 41}),
	}
	if !check(t, ops, AllLinearizable, 0) {
		t.Fatal("a partial readdir page was treated as a complete listing")
	}
}

// The entries of a page are a contiguous run in sorted order, because the
// listing comes straight out of an etcd prefix scan. A name that sorts inside
// that run and is missing from it has been dropped by the listing.
func TestAuditReaddirDroppedEntryIsRejected(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "aaa", 41, ok, 10, 20),
		op(KindCreate, "n1", 1, "mmm", 42, ok, 25, 30),
		op(KindCreate, "n1", 1, "zzz", 43, ok, 32, 35),
		// "mmm" sorts between "aaa" and "zzz" and must be on this page.
		rdop("n1", 1, 0, 40, 50,
			DirEntry{Name: "aaa", Ino: 41}, DirEntry{Name: "zzz", Ino: 43}),
	}
	if check(t, ops, AllLinearizable, 0) {
		t.Fatal("a listing that dropped an entry from inside its own range was accepted")
	}
}

// A listing must not resurrect a name that was removed.
func TestAuditReaddirListingAnUnlinkedNameIsRejected(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "f", 42, ok, 10, 20),
		op(KindUnlink, "n1", 1, "f", 0, ok, 30, 40),
		rdop("n2", 1, 0, 50, 60, DirEntry{Name: "f", Ino: 42}),
	}
	if check(t, ops, AllLinearizable, 0) {
		t.Fatal("a listing returned a name that had been unlinked")
	}
}

// A listing must agree with the rest of the history about which inode a name
// refers to.
func TestAuditReaddirWrongInodeIsRejected(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "f", 42, ok, 10, 20),
		rdop("n2", 1, 0, 30, 40, DirEntry{Name: "f", Ino: 99}),
	}
	if check(t, ops, AllLinearizable, 0) {
		t.Fatal("a listing named an inode the rest of the history contradicts")
	}
}

// An empty listing at offset 0 is a directory with nothing in it, which a
// live name contradicts.
func TestAuditReaddirEmptyListingWithLiveNameIsRejected(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "f", 42, ok, 10, 20),
		rdop("n2", 1, 0, 30, 40),
	}
	if check(t, ops, AllLinearizable, 0) {
		t.Fatal("an empty listing was accepted for a directory holding a live name")
	}
}

// A create that overlaps the listing is not a violation: the checker is free
// to order it after, which is the whole reason this is a linearizability
// question and not a sorted scan.
func TestAuditReaddirConcurrentCreateIsAccepted(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "f", 42, ok, 10, 60),
		rdop("n2", 1, 0, 20, 50),
	}
	if !check(t, ops, AllLinearizable, 0) {
		t.Fatal("a create overlapping the listing was reported as a violation")
	}
}
