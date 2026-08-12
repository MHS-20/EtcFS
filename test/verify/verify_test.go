package verify

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// A checker that never rejects anything is indistinguishable from a correct
// filesystem, so most of what follows is histories that must *fail*.

const (
	ok      = int32(0)
	enoent  = int32(errnoENOENT)
	eexist  = int32(errnoEEXIST)
	timeout = 10 * time.Second
)

// op builds one namespace operation over the interval [call, ret].
func op(kind Kind, node string, parent uint64, name string, ino uint64, errno int32, call, ret int64) Op {
	return Op{Kind: kind, Node: node, Parent: parent, Name: name, Ino: ino, Errno: errno, Call: call, Ret: ret}
}

func check(t *testing.T, ops []Op, class Classifier, staleness time.Duration) bool {
	t.Helper()
	res := Check(NamespaceModel, Operations(ops), class, staleness, timeout)
	if res == porcupine.Illegal {
		return false
	}
	if res == porcupine.Unknown {
		t.Fatalf("checker timed out; the history is too large or the model too loose")
	}
	return true
}

// The ordinary case: a file is created, seen, removed, and gone.
func TestLinearizableHistoryIsAccepted(t *testing.T) {
	ops := []Op{
		op(KindLookup, "n1", 1, "f", 0, enoent, 10, 20),
		op(KindCreate, "n1", 1, "f", 42, ok, 30, 40),
		op(KindLookup, "n2", 1, "f", 42, ok, 50, 60),
		op(KindUnlink, "n2", 1, "f", 0, ok, 70, 80),
		op(KindLookup, "n1", 1, "f", 0, enoent, 90, 100),
	}
	if !check(t, ops, AllLinearizable, 0) {
		t.Fatal("a linearizable history was rejected")
	}
}

// A read that returns a file after a later-than-it unlink reported success,
// with no overlap to excuse it, has no sequential explanation.
func TestStaleReadIsRejectedWhenLinearizable(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "f", 42, ok, 10, 20),
		op(KindUnlink, "n1", 1, "f", 0, ok, 30, 40),
		op(KindLookup, "n2", 1, "f", 42, ok, 50, 60),
	}
	if check(t, ops, AllLinearizable, 0) {
		t.Fatal("a stale read was accepted as linearizable")
	}
}

// The same history is legitimate for a reader allowed to be a little out of
// date — and that is the whole point of the relaxation, so it has to be shown
// to change the answer rather than merely to exist.
func TestStaleReadIsAcceptedWhenReadsMayBeCached(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "f", 42, ok, 10, 20),
		op(KindUnlink, "n1", 1, "f", 0, ok, 30, 40),
		op(KindLookup, "n2", 1, "f", 42, ok, 50, 60),
	}
	if !check(t, ops, ReadsAreCached, 40*time.Nanosecond) {
		t.Fatal("a read inside the cache window was rejected")
	}
	// A window too small to reach back before the unlink must not excuse it.
	if check(t, ops, ReadsAreCached, 5*time.Nanosecond) {
		t.Fatal("the staleness bound is not being enforced")
	}
}

// Relaxation must not become an excuse for anything: a read of a file that was
// never created has no explanation under any ordering.
func TestInventedValueIsRejectedEvenFullyRelaxed(t *testing.T) {
	ops := []Op{
		op(KindLookup, "n1", 1, "f", 0, enoent, 10, 20),
		op(KindLookup, "n2", 1, "f", 42, ok, 30, 40),
		op(KindLookup, "n1", 1, "f", 0, enoent, 50, 60),
	}
	everything := func(Op) Consistency { return Serializable }
	if check(t, ops, everything, 0) {
		t.Fatal("a value that was never written was accepted")
	}
}

// Two nodes creating the same name: exactly one may succeed, and a second
// success with no unlink between them is a lost update.
func TestTwoSuccessfulCreatesOfOneNameAreRejected(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "f", 42, ok, 10, 20),
		op(KindCreate, "n2", 1, "f", 43, ok, 30, 40),
		op(KindLookup, "n1", 1, "f", 42, ok, 50, 60),
	}
	if check(t, ops, AllLinearizable, 0) {
		t.Fatal("a create that overwrote a live name was accepted")
	}
}

// Concurrency is not a violation: overlapping operations may be explained in
// whichever order the model allows.
func TestConcurrentCreateAndLookupAreAccepted(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "f", 42, ok, 10, 50),
		op(KindLookup, "n2", 1, "f", 0, enoent, 20, 30),
		op(KindCreate, "n2", 1, "f", 43, eexist, 25, 45),
	}
	if !check(t, ops, AllLinearizable, 0) {
		t.Fatal("a legitimately concurrent history was rejected")
	}
}

// Operations on different directories constrain nothing about each other, and
// the partitioning must not invent a constraint between them.
func TestSeparateDirectoriesDoNotConstrainEachOther(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "f", 42, ok, 10, 20),
		op(KindLookup, "n2", 2, "f", 0, enoent, 30, 40),
		op(KindCreate, "n2", 2, "f", 43, ok, 50, 60),
		op(KindLookup, "n1", 1, "f", 42, ok, 70, 80),
	}
	if !check(t, ops, AllLinearizable, 0) {
		t.Fatal("two independent directories were checked against each other")
	}
}

// A cross-directory rename is checked from both ends: the source loses the
// name, the destination gains it.
func TestRenameMovesTheNameBetweenDirectories(t *testing.T) {
	move := Op{Kind: KindRename, Node: "n1", Parent: 1, Name: "a", NewParent: 2, NewName: "b", Call: 30, Ret: 40}
	ops := []Op{
		op(KindCreate, "n1", 1, "a", 42, ok, 10, 20),
		move,
		op(KindLookup, "n2", 1, "a", 0, enoent, 50, 60),
		op(KindLookup, "n2", 2, "b", 42, ok, 70, 80),
	}
	if !check(t, ops, AllLinearizable, 0) {
		t.Fatal("a valid rename was rejected")
	}

	// The source keeping the name afterwards is a violation.
	bad := []Op{
		op(KindCreate, "n1", 1, "a", 42, ok, 10, 20),
		move,
		op(KindLookup, "n2", 1, "a", 42, ok, 50, 60),
	}
	if check(t, bad, AllLinearizable, 0) {
		t.Fatal("a rename that left the source name in place was accepted")
	}
}

// Replacing a file is a rename's privilege: the destination existing is not a
// violation there, though it is for every other operation that adds a name.
func TestRenameMayReplaceAnExistingDestination(t *testing.T) {
	ops := []Op{
		op(KindCreate, "n1", 1, "a", 42, ok, 10, 20),
		op(KindCreate, "n1", 2, "b", 43, ok, 25, 30),
		{Kind: KindRename, Node: "n1", Parent: 1, Name: "a", NewParent: 2, NewName: "b", Call: 40, Ret: 50},
		op(KindLookup, "n2", 2, "b", 42, ok, 60, 70),
		op(KindLookup, "n2", 1, "a", 0, enoent, 80, 90),
	}
	if !check(t, ops, AllLinearizable, 0) {
		t.Fatal("a rename over an existing destination was rejected")
	}
}

// Relaxation may only ever add valid orderings: anything the strict check
// accepts, the relaxed check must accept too. Checked over the histories above
// rather than asserted in a comment.
func TestRelaxationOnlyEverAccepts(t *testing.T) {
	histories := [][]Op{
		{
			op(KindLookup, "n1", 1, "f", 0, enoent, 10, 20),
			op(KindCreate, "n1", 1, "f", 42, ok, 30, 40),
			op(KindUnlink, "n2", 1, "f", 0, ok, 50, 60),
		},
		{
			op(KindCreate, "n1", 1, "f", 42, ok, 10, 50),
			op(KindCreate, "n2", 1, "f", 43, eexist, 25, 45),
		},
	}
	for i, h := range histories {
		if !check(t, h, AllLinearizable, 0) {
			t.Fatalf("history %d: rejected while linearizable, so the comparison below says nothing", i)
		}
		if !check(t, h, ReadsAreCached, time.Second) {
			t.Fatalf("history %d: relaxing the reads turned an accepted history into a rejected one", i)
		}
	}
}

// A history is only as good as its decoder, so the wire format is exercised
// end to end: entries as the daemon records them, decoded by this package's
// own reader, checked as a history.
func TestDecodedHistoryChecksLikeADirectOne(t *testing.T) {
	entries := recordedNamespaceHistory()
	ops, err := DecodeNamespace(entries)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(ops) != len(entries) {
		t.Fatalf("decoded %d operations from %d entries", len(ops), len(entries))
	}
	if !check(t, ops, AllLinearizable, 0) {
		t.Fatal("a linearizable recorded history was rejected")
	}
}
