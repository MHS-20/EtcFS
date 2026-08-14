package verify

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

func wop(node string, ino, off uint64, data []byte, call, ret int64) ExtentOp {
	return ExtentOp{Node: node, Ino: ino, Kind: ExtentWrite, Off: off, Data: data, Call: call, Ret: ret}
}

func rop(node string, ino, off uint64, data []byte, call, ret int64) ExtentOp {
	return ExtentOp{Node: node, Ino: ino, Kind: ExtentRead, Off: off, Data: data, Call: call, Ret: ret}
}

func extentOK(t *testing.T, ops []ExtentOp, crashed ...string) bool {
	t.Helper()
	res := CheckExtents(ops, crashed, timeout)
	if res == porcupine.Unknown {
		t.Fatal("checker timed out")
	}
	return res == porcupine.Ok
}

// A read returning exactly what was written: the ordinary case.
func TestExtentReadAfterWriteIsAccepted(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 0, []byte("hello"), 10, 20),
		rop("n2", 1, 0, []byte("hello"), 30, 40),
	}
	if !extentOK(t, ops) {
		t.Fatal("an ordinary read-after-write was rejected")
	}
}

// A read returning bytes that contradict the only write that ever touched
// that position: the violation this model exists to catch.
func TestExtentReadContradictingWriteIsRejected(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 0, []byte("hello"), 10, 20),
		rop("n2", 1, 0, []byte("world"), 30, 40),
	}
	if extentOK(t, ops) {
		t.Fatal("a read contradicting the only write was accepted")
	}
}

// A later write legitimately overwrites an earlier one; a read afterward must
// see the new bytes, not the old ones.
func TestExtentSecondWriteOverwritesTheFirst(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 0, []byte("aaaaa"), 10, 20),
		wop("n1", 1, 0, []byte("bbbbb"), 30, 40),
		rop("n2", 1, 0, []byte("bbbbb"), 50, 60),
	}
	if !extentOK(t, ops) {
		t.Fatal("a read of the overwritten value was rejected")
	}
	stale := []ExtentOp{
		wop("n1", 1, 0, []byte("aaaaa"), 10, 20),
		wop("n1", 1, 0, []byte("bbbbb"), 30, 40),
		rop("n2", 1, 0, []byte("aaaaa"), 50, 60),
	}
	if extentOK(t, stale) {
		t.Fatal("a read of the pre-overwrite value, with no overlap to excuse it, was accepted")
	}
}

// A read of a byte position this history never wrote is unconstrained: it
// might be a legitimate hole or data from before the recorded window, and
// this model has no way to tell, so it must not guess.
func TestExtentReadOfUntouchedPositionIsUnconstrained(t *testing.T) {
	ops := []ExtentOp{
		rop("n1", 1, 1000, []byte{0xAB, 0xCD, 0xEF}, 10, 20),
	}
	if !extentOK(t, ops) {
		t.Fatal("a read of a position this history never touched was rejected")
	}
}

// A read that establishes a value is itself evidence: a later read
// contradicting it, with no intervening write, is a violation.
func TestExtentTwoReadsMustAgreeAbsentAnyWrite(t *testing.T) {
	ops := []ExtentOp{
		rop("n1", 1, 500, []byte{0x01}, 10, 20),
		rop("n2", 1, 500, []byte{0x02}, 30, 40),
	}
	if extentOK(t, ops) {
		t.Fatal("two disagreeing reads of an unwritten position were accepted")
	}
}

// A write that overlaps a hole boundary only partially constrains it: bytes
// inside the write are pinned, bytes outside remain unconstrained.
func TestExtentPartialOverlapConstrainsOnlyTheWrittenBytes(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 10, []byte{0xAA, 0xBB}, 10, 20),
		// Reads offset 9 (untouched), 10 (=0xAA), 11 (=0xBB), 12 (untouched).
		rop("n2", 1, 9, []byte{0x00, 0xAA, 0xBB, 0x00}, 30, 40),
	}
	if !extentOK(t, ops) {
		t.Fatal("a read whose written bytes matched was rejected over its untouched neighbours")
	}
	wrong := []ExtentOp{
		wop("n1", 1, 10, []byte{0xAA, 0xBB}, 10, 20),
		rop("n2", 1, 9, []byte{0x00, 0xFF, 0xBB, 0x00}, 30, 40),
	}
	if extentOK(t, wrong) {
		t.Fatal("a read disagreeing with a written byte inside the touched range was accepted")
	}
}

// A rejected write (nonzero errno) changed nothing and asserts nothing.
func TestExtentRejectedWriteIsIgnored(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 0, []byte("hello"), 10, 20),
		{Node: "n2", Ino: 1, Kind: ExtentWrite, Off: 0, Data: []byte("world"), Errno: 5, Call: 25, Ret: 28},
		rop("n3", 1, 0, []byte("hello"), 30, 40),
	}
	if !extentOK(t, ops) {
		t.Fatal("a failed write was treated as though it had taken effect")
	}
}

// Different inodes never constrain each other.
func TestExtentSeparateInodesDoNotConstrainEachOther(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 0, []byte("aaaa"), 10, 20),
		wop("n1", 2, 0, []byte("bbbb"), 15, 25),
		rop("n2", 1, 0, []byte("aaaa"), 30, 40),
		rop("n2", 2, 0, []byte("bbbb"), 35, 45),
	}
	if !extentOK(t, ops) {
		t.Fatal("two independent inodes were checked against each other")
	}
}

func fsop(node string, ino uint64, call, ret int64) ExtentOp {
	return ExtentOp{Node: node, Ino: ino, Kind: ExtentFsync, Call: call, Ret: ret}
}

// A write acknowledged out of a buffer and never fsynced may be lost with the
// node that held it -- but only for a node the run actually killed.
func TestExtentUnflushedWriteIsLostOnlyWithACrashedNode(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 0, []byte("hello"), 10, 20),
		rop("n2", 1, 0, []byte{0, 0, 0, 0, 0}, 30, 40),
	}
	if extentOK(t, ops) {
		t.Fatal("a write vanished under a cluster where nothing crashed")
	}
	if !extentOK(t, ops, "n1") {
		t.Fatal("an unflushed write was required to survive the death of the node holding it")
	}
}

// fsync is the barrier: past it the write is published, and losing it is a
// violation no matter what happened to the node that made it.
func TestExtentFsyncedWriteSurvivesACrash(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 0, []byte("hello"), 10, 20),
		fsop("n1", 1, 21, 25),
		rop("n2", 1, 0, []byte{0, 0, 0, 0, 0}, 30, 40),
	}
	if extentOK(t, ops, "n1") {
		t.Fatal("data that fsync acknowledged was allowed to disappear")
	}
}

// An fsync that failed acknowledged nothing, so its writes stay losable.
func TestExtentFailedFsyncIsNotABarrier(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 0, []byte("hello"), 10, 20),
		{Node: "n1", Ino: 1, Kind: ExtentFsync, Errno: 5, Call: 21, Ret: 25},
		rop("n2", 1, 0, []byte{0, 0, 0, 0, 0}, 30, 40),
	}
	if !extentOK(t, ops, "n1") {
		t.Fatal("an fsync that returned EIO was treated as a durability promise")
	}
}

// A node reads back its own buffered write even before any flush, and the loss
// relaxation must never excuse the node that wrote the bytes from seeing them.
func TestExtentNodeSeesItsOwnBufferedWrite(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 0, []byte("hello"), 10, 20),
		rop("n1", 1, 0, []byte("world"), 30, 40),
	}
	if extentOK(t, ops, "n1") {
		t.Fatal("a node failed to read back its own buffered write and it was accepted")
	}
}

// Once a peer has returned the bytes to an application, no later crash can take
// them back: they were observed, whatever their durability was.
func TestExtentObservedBytesCannotVanish(t *testing.T) {
	ops := []ExtentOp{
		wop("n1", 1, 0, []byte("hello"), 10, 20),
		rop("n2", 1, 0, []byte("hello"), 30, 40),
		rop("n3", 1, 0, []byte{0, 0, 0, 0, 0}, 50, 60),
	}
	if extentOK(t, ops, "n1") {
		t.Fatal("bytes an earlier read had already returned were allowed to disappear")
	}
}
