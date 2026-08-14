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
	res := CheckBlocks(ops, timeout)
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
