package verify

import (
	"fmt"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/MHS-20/EtcFS/internal/history"
)

// Wire constants for the lock_hold synthetic history entries, matching
// historyOpLockHold and the event bytes in internal/ipc/retry.go. Kept as a
// separate copy rather than a shared import, on purpose: see decode.go.
const (
	lockHoldOpcode   = 1000
	lockEventAcquire = 0
	lockEventRelease = 1
)

// The lock model: does the exclusive/shared inode lock ever admit two holders
// that should have excluded each other?
//
// This is not phrased as "is this history linearizable" over some register —
// a lock has no value for a read to disagree about, so an ordinary
// linearizability check would accept any interleaving. What actually matters
// is mutual exclusion, and it is checked as exactly that: a state machine over
// acquire/release events, each recorded at the precise instant it happened
// (the etcd transaction that granted or dropped it), not over an interval. A
// zero-width [t, t] operation is what tells Porcupine "this took effect at
// exactly this instant" rather than "sometime in this window" — which is the
// truth here, since AcquireLock and Release/Folded are each one etcd
// transaction.

// lockOpKind distinguishes the two events a lock's lifetime is made of.
type lockOpKind int

const (
	lockAcquireExclusive lockOpKind = iota
	lockReleaseExclusive
	lockAcquireShared
	lockReleaseShared
)

// LockOp is one endpoint of a lock's hold interval.
type LockOp struct {
	Node string
	Ino  uint64
	Kind lockOpKind
	At   int64
}

func (k lockOpKind) String() string {
	switch k {
	case lockAcquireExclusive:
		return "acquire-exclusive"
	case lockReleaseExclusive:
		return "release-exclusive"
	case lockAcquireShared:
		return "acquire-shared"
	case lockReleaseShared:
		return "release-shared"
	}
	return "?"
}

// DecodeLocks turns a recorded history into its lock acquire/release events,
// in call order. Entries other than historyOpLockHold are skipped.
func DecodeLocks(entries []history.Entry) ([]LockOp, error) {
	ops := make([]LockOp, 0, len(entries))
	for _, e := range entries {
		if e.Opcode != lockHoldOpcode {
			continue
		}
		req, _, err := e.Payloads()
		if err != nil {
			return nil, err
		}
		if len(req) < 10 {
			return nil, fmt.Errorf("lock event at %d: payload too short", e.CallNs)
		}
		event, mode := req[0], req[1]
		r := newReader(req[2:])
		ino := r.u64()
		if !r.ok {
			return nil, fmt.Errorf("lock event at %d: truncated inode", e.CallNs)
		}

		var kind lockOpKind
		switch {
		case event == lockEventAcquire && mode == 1:
			kind = lockAcquireExclusive
		case event == lockEventRelease && mode == 1:
			kind = lockReleaseExclusive
		case event == lockEventAcquire && mode == 0:
			kind = lockAcquireShared
		case event == lockEventRelease && mode == 0:
			kind = lockReleaseShared
		default:
			return nil, fmt.Errorf("lock event at %d: unknown event/mode %d/%d", e.CallNs, event, mode)
		}
		ops = append(ops, LockOp{Node: e.Node, Ino: ino, Kind: kind, At: e.CallNs})
	}
	return ops, nil
}

// lockOps turns decoded LockOps into zero-width Porcupine operations,
// partitioned by inode.
func lockOperations(ops []LockOp) []porcupine.Operation {
	clients := map[string]int{}
	out := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		id, seen := clients[op.Node]
		if !seen {
			id = len(clients)
			clients[op.Node] = id
		}
		out = append(out, porcupine.Operation{
			ClientId: id, Input: op, Output: nil, Call: op.At, Return: op.At,
		})
	}
	return out
}

// lockState is 0 (free), -1 (exclusive) or the number of shared holders.
type lockState int

func (s lockState) step(op LockOp) (bool, lockState) {
	switch op.Kind {
	case lockAcquireExclusive:
		if s != 0 {
			return false, s
		}
		return true, -1
	case lockReleaseExclusive:
		if s != -1 {
			return false, s
		}
		return true, 0
	case lockAcquireShared:
		if s == -1 {
			return false, s
		}
		return true, s + 1
	case lockReleaseShared:
		if s <= 0 {
			return false, s
		}
		return true, s - 1
	}
	return false, s
}

// LockModel is the mutual-exclusion state machine, partitioned per inode.
var LockModel = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		byIno := map[uint64][]porcupine.Operation{}
		for _, o := range h {
			ino := o.Input.(LockOp).Ino
			byIno[ino] = append(byIno[ino], o)
		}
		out := make([][]porcupine.Operation, 0, len(byIno))
		for _, ops := range byIno {
			out = append(out, ops)
		}
		return out
	},
	Init: func() interface{} { return lockState(0) },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		ok, next := state.(lockState).step(input.(LockOp))
		return ok, next
	},
	Equal: func(a, b interface{}) bool { return a.(lockState) == b.(lockState) },
	DescribeOperation: func(input, output interface{}) string {
		op := input.(LockOp)
		return fmt.Sprintf("ino=%d %s", op.Ino, op.Kind)
	},
}

// CheckLocks checks a decoded lock history for mutual-exclusion violations.
func CheckLocks(ops []LockOp, timeout time.Duration) porcupine.CheckResult {
	return porcupine.CheckOperationsTimeout(LockModel, lockOperations(ops), timeout)
}
