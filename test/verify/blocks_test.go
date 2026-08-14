package verify

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

func reserve(node string, off, length uint64, call, ret int64) BlockOp {
	return BlockOp{Node: node, Reserve: true, DiskOff: off, Length: length, Call: call, Ret: ret}
}

func free(node string, off, length uint64, call, ret int64) BlockOp {
	return BlockOp{Node: node, DiskOff: off, Length: length, Call: call, Ret: ret}
}

func blocksOK(t *testing.T, ops []BlockOp) bool {
	t.Helper()
	res := CheckBlocks(ops, nil, timeout)
	if res == porcupine.Unknown {
		t.Fatal("checker timed out")
	}
	return res == porcupine.Ok
}

// Reserve, free, reserve again: the ordinary life of a block.
func TestBlockReuseAfterFreeIsAccepted(t *testing.T) {
	ops := []BlockOp{
		reserve("n1", 0, blockSize, 10, 20),
		free("n1", 0, blockSize, 30, 40),
		reserve("n2", 0, blockSize, 50, 60),
	}
	if !blocksOK(t, ops) {
		t.Fatal("a block reused after it was returned was rejected")
	}
}

// Two nodes holding one block at once is the corruption every layer above the
// allocator assumes cannot happen.
func TestBlockReservedTwiceIsRejected(t *testing.T) {
	ops := []BlockOp{
		reserve("n1", 0, blockSize, 10, 20),
		reserve("n2", 0, blockSize, 30, 40),
	}
	if blocksOK(t, ops) {
		t.Fatal("one block reserved by two nodes at once was accepted")
	}
}

// The overlap need not be exact: a larger run covering a reserved block is the
// same violation.
func TestBlockOverlappingReservationIsRejected(t *testing.T) {
	ops := []BlockOp{
		reserve("n1", blockSize, blockSize, 10, 20),
		reserve("n2", 0, 4*blockSize, 30, 40),
	}
	if blocksOK(t, ops) {
		t.Fatal("a run overlapping an already reserved block was accepted")
	}
}

// Freeing the same range twice hands it out while an extent still names it.
func TestBlockFreedTwiceIsRejected(t *testing.T) {
	ops := []BlockOp{
		reserve("n1", 0, blockSize, 10, 20),
		free("n1", 0, blockSize, 30, 40),
		free("n1", 0, blockSize, 50, 60),
	}
	if blocksOK(t, ops) {
		t.Fatal("a block returned to the arena twice was accepted")
	}
}

// A node rebuilds its allocator from the extent records when it restarts, so it
// legitimately frees blocks it never reserved inside the recorded window.
func TestBlockFreeWithoutARecordedReservationIsAccepted(t *testing.T) {
	ops := []BlockOp{
		free("n1", 4096000, blockSize, 10, 20),
	}
	if !blocksOK(t, ops) {
		t.Fatal("a block reserved before the history began could not be freed")
	}
}

// Ranges far enough apart to land in different arenas never constrain each
// other, and must not be checked against each other.
func TestBlockSeparateArenasDoNotConstrainEachOther(t *testing.T) {
	ops := []BlockOp{
		reserve("n1", 0, blockSize, 10, 20),
		reserve("n2", blockGroupSize, blockSize, 15, 25),
	}
	if !blocksOK(t, ops) {
		t.Fatal("two independent arenas were checked against each other")
	}
}

// A node killed holding a reservation, restarted, handing the same block out
// again: the reservation died with the process, so this is legitimate.
//
// This is the shape the chaos suite produces on every run that kills a daemon,
// and without the incarnation boundary it reads as the double allocation this
// model exists to catch.
func TestBlockReReservedAfterCrashIsAccepted(t *testing.T) {
	ops := []BlockOp{
		reserve("n1", 0, blockSize, 10, 20), // held when n1 was killed
		reserve("n1", 0, blockSize, 200, 210),
	}
	starts := []StartOp{{Node: "n1", At: 1}, {Node: "n1", At: 150}}
	if res := CheckBlocks(ops, starts, timeout); res != porcupine.Ok {
		t.Fatalf("a block re-reserved by a restarted node was reported as double-allocated (%v)", res)
	}
}

// The same two reservations with no restart between them is a real double
// allocation and must still be rejected: excusing the crash must not cost the
// model its teeth.
func TestBlockDoubleReserveWithoutCrashIsRejected(t *testing.T) {
	ops := []BlockOp{
		reserve("n1", 0, blockSize, 10, 20),
		reserve("n1", 0, blockSize, 200, 210),
	}
	starts := []StartOp{{Node: "n1", At: 1}}
	if res := CheckBlocks(ops, starts, timeout); res == porcupine.Ok {
		t.Fatal("a block reserved twice by a live node was accepted")
	}
}

// A crash later on must not excuse a peer that took the block while the holder
// was demonstrably still running. The crash boundary is bounded by the holder's
// last event, so a holder that goes on recording work after the peer's
// reservation puts that boundary after it, and the overlap stands.
//
// The weaker case — a peer reserving inside the silent window between the
// holder's last event and its restart — is deliberately *not* a violation:
// nothing observed when in that window the process actually died, and claiming
// otherwise would report a crash the history cannot place.
func TestBlockPeerReserveWhileHolderStillActiveIsRejected(t *testing.T) {
	ops := []BlockOp{
		reserve("n1", 0, blockSize, 10, 20),
		reserve("n2", 0, blockSize, 30, 40),
		reserve("n1", blockSize, blockSize, 50, 60), // n1 still working after n2's grab
	}
	starts := []StartOp{{Node: "n1", At: 1}, {Node: "n2", At: 1}, {Node: "n1", At: 150}}
	if res := CheckBlocks(ops, starts, timeout); res == porcupine.Ok {
		t.Fatal("a peer took a block still held by a demonstrably live node and it was accepted")
	}
}

// A block whose extent outlived the crash, reclaimed by the scrubber after the
// restart: the rebuilt allocator inherited it, so that release is real and the
// crash must not have already counted as one.
//
// This is the shape a chaos run produces whenever a node is killed holding a
// published extent — the crash boundary marks the hold unknown rather than
// released precisely so this does not read as a double free.
func TestBlockFreedAfterCrashByRebuiltAllocatorIsAccepted(t *testing.T) {
	ops := []BlockOp{
		reserve("n1", 0, blockSize, 10, 20), // published, then n1 is killed
		free("n1", 0, blockSize, 200, 210),  // scrubber reclaims it after restart
		reserve("n1", 0, blockSize, 300, 310),
	}
	starts := []StartOp{{Node: "n1", At: 1}, {Node: "n1", At: 150}}
	if res := CheckBlocks(ops, starts, timeout); res != porcupine.Ok {
		t.Fatalf("a block reclaimed after a restart was reported as a double free (%v)", res)
	}
}

// Forgetting the hold at a crash must not forgive a genuine double free that
// happens entirely after the restart.
func TestBlockDoubleFreeAfterCrashIsRejected(t *testing.T) {
	ops := []BlockOp{
		reserve("n1", 0, blockSize, 10, 20),
		free("n1", 0, blockSize, 200, 210),
		free("n1", 0, blockSize, 300, 310),
	}
	starts := []StartOp{{Node: "n1", At: 1}, {Node: "n1", At: 150}}
	if res := CheckBlocks(ops, starts, timeout); res == porcupine.Ok {
		t.Fatal("the same block freed twice after a restart was accepted")
	}
}
