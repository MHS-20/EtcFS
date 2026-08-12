package verify

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

func gop(node string, gen uint64, committed, fenced bool, call, ret int64) GuardOp {
	return GuardOp{Node: node, Gen: gen, Committed: committed, Fenced: fenced, Call: call, Ret: ret}
}

func genOK(t *testing.T, ops []GuardOp) bool {
	t.Helper()
	res := CheckGenerations(ops, timeout)
	if res == porcupine.Unknown {
		t.Fatal("checker timed out")
	}
	return res == porcupine.Ok
}

// Ordinary commits, none fenced: accepted.
func TestGenerationOrdinaryCommitsAreAccepted(t *testing.T) {
	ops := []GuardOp{
		gop("n1", 1, true, false, 10, 20),
		gop("n1", 1, true, false, 30, 40),
		gop("n1", 1, true, false, 50, 60),
	}
	if !genOK(t, ops) {
		t.Fatal("a healthy node's ordinary commits were rejected")
	}
}

// A commit rejected for a fence, then no further commit succeeds: exactly
// what the fencing design promises.
func TestGenerationRejectedThenSilentIsAccepted(t *testing.T) {
	ops := []GuardOp{
		gop("n1", 1, true, false, 10, 20),
		gop("n1", 2, false, true, 30, 40),
		gop("n1", 2, false, true, 50, 60),
	}
	if !genOK(t, ops) {
		t.Fatal("a node that stopped writing after its fence was rejected")
	}
}

// A commit that succeeds after this node has already been fenced is the
// violation the whole design exists to prevent.
func TestGenerationCommitAfterFenceIsRejected(t *testing.T) {
	ops := []GuardOp{
		gop("n1", 1, true, false, 10, 20),
		gop("n1", 2, false, true, 30, 40),
		gop("n1", 2, true, false, 50, 60), // slipped through after the fence
	}
	if genOK(t, ops) {
		t.Fatal("a commit that succeeded after a fence was accepted")
	}
}

// Two worker threads on one node genuinely racing before any fence: this is
// ordinary concurrency, not a violation, and overlapping intervals must not
// be mistaken for one — the model has to admit *some* valid order, not demand
// a specific one.
func TestGenerationOverlappingHealthyCommitsAreAccepted(t *testing.T) {
	ops := []GuardOp{
		gop("n1", 1, true, false, 10, 50),
		gop("n1", 1, true, false, 20, 40),
		gop("n1", 1, true, false, 25, 60),
	}
	if !genOK(t, ops) {
		t.Fatal("legitimately concurrent healthy commits were rejected")
	}
}

// A success whose entire interval comes after the fence-rejection's return is
// unambiguous: real-time order alone forces it after, and every ordering
// porcupine could pick puts it there, so it must fail regardless of how the
// checker searches.
func TestGenerationUnambiguousPostFenceSuccessIsRejected(t *testing.T) {
	ops := []GuardOp{
		gop("n1", 1, true, false, 10, 20),
		gop("n1", 2, false, true, 30, 40),
		gop("n1", 2, true, false, 41, 50),
	}
	if genOK(t, ops) {
		t.Fatal("a success strictly after a fence-rejection was accepted")
	}
}

// Different nodes never constrain each other — one node's fence says nothing
// about another's.
func TestGenerationSeparateNodesDoNotConstrainEachOther(t *testing.T) {
	ops := []GuardOp{
		gop("n1", 2, false, true, 10, 20),
		gop("n2", 1, true, false, 15, 25),
	}
	if !genOK(t, ops) {
		t.Fatal("two independent nodes were checked against each other")
	}
}
