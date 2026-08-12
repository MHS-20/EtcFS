package verify

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

// lop builds a lock event over a narrow interval around at, standing in for
// the etcd transaction that moved the lock.
func lop(node string, ino uint64, kind lockOpKind, at int64) LockOp {
	return LockOp{Node: node, Ino: ino, Kind: kind, Call: at, Ret: at + 1}
}

func lockOK(t *testing.T, ops []LockOp) bool {
	t.Helper()
	res := CheckLocks(ops, DefaultLockLeaseTTL, timeout)
	if res == porcupine.Unknown {
		t.Fatal("checker timed out")
	}
	return res == porcupine.Ok
}

// Sequential acquire/release of an exclusive lock: no violation.
func TestLockSequentialExclusiveIsAccepted(t *testing.T) {
	ops := []LockOp{
		lop("n1", 1, lockAcquireExclusive, 10),
		lop("n1", 1, lockReleaseExclusive, 20),
		lop("n2", 1, lockAcquireExclusive, 30),
		lop("n2", 1, lockReleaseExclusive, 40),
	}
	if !lockOK(t, ops) {
		t.Fatal("a valid sequential exclusive history was rejected")
	}
}

// Two exclusive holders at once on the same inode: the violation this model
// exists to catch.
func TestLockTwoExclusiveHoldersIsRejected(t *testing.T) {
	ops := []LockOp{
		lop("n1", 1, lockAcquireExclusive, 10),
		lop("n2", 1, lockAcquireExclusive, 20), // acquired while n1 still holds it
		lop("n1", 1, lockReleaseExclusive, 30),
		lop("n2", 1, lockReleaseExclusive, 40),
	}
	if lockOK(t, ops) {
		t.Fatal("two overlapping exclusive holders were accepted")
	}
}

// Multiple shared holders at once is legitimate.
func TestLockMultipleSharedHoldersIsAccepted(t *testing.T) {
	ops := []LockOp{
		lop("n1", 1, lockAcquireShared, 10),
		lop("n2", 1, lockAcquireShared, 20),
		lop("n3", 1, lockAcquireShared, 30),
		lop("n1", 1, lockReleaseShared, 40),
		lop("n2", 1, lockReleaseShared, 50),
		lop("n3", 1, lockReleaseShared, 60),
	}
	if !lockOK(t, ops) {
		t.Fatal("legitimate concurrent shared holders were rejected")
	}
}

// A shared holder while another node holds exclusive is the same violation as
// two exclusive holders — the whole point of an exclusive lock.
func TestLockSharedDuringExclusiveIsRejected(t *testing.T) {
	ops := []LockOp{
		lop("n1", 1, lockAcquireExclusive, 10),
		lop("n2", 1, lockAcquireShared, 20),
		lop("n1", 1, lockReleaseExclusive, 30),
		lop("n2", 1, lockReleaseShared, 40),
	}
	if lockOK(t, ops) {
		t.Fatal("a shared holder admitted during an exclusive hold was accepted")
	}
}

// A release with nothing held is impossible and must be rejected — it would
// otherwise mean the model's arithmetic can go negative and hide a real
// double-release.
func TestLockReleaseWithoutAcquireIsRejected(t *testing.T) {
	ops := []LockOp{lop("n1", 1, lockReleaseExclusive, 10)}
	if lockOK(t, ops) {
		t.Fatal("a release with nothing held was accepted")
	}
}

// Different inodes never constrain each other.
func TestLockSeparateInodesDoNotConstrainEachOther(t *testing.T) {
	ops := []LockOp{
		lop("n1", 1, lockAcquireExclusive, 10),
		lop("n2", 2, lockAcquireExclusive, 15),
		lop("n1", 1, lockReleaseExclusive, 20),
		lop("n2", 2, lockReleaseExclusive, 25),
	}
	if !lockOK(t, ops) {
		t.Fatal("two independent inodes were checked against each other")
	}
}
