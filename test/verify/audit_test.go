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
