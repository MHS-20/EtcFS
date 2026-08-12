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
// Each event is checked over the interval its own etcd transaction spanned,
// not as a point: the transaction commits at a single revision, but nothing
// in the daemon observes that revision's wall-clock instant — only the call
// and return around it. Recording the interval is the honest statement of
// what is known, and it is what keeps ordinary clock offset between two
// hosts from reading as two holders of one lock.

// lockOpKind distinguishes the two events a lock's lifetime is made of.
type lockOpKind int

const (
	lockAcquireExclusive lockOpKind = iota
	lockReleaseExclusive
	lockAcquireShared
	lockReleaseShared
)

// LockOp is one endpoint of a lock's hold interval, over the interval the
// operation that moved it actually spanned.
type LockOp struct {
	Node string
	Ino  uint64
	Kind lockOpKind
	// Call and Ret bracket the etcd transaction that granted or dropped the
	// lock. The instant itself is somewhere inside; nothing observes it
	// directly, and pretending otherwise is what made clock offset between
	// two hosts look like two holders of one lock.
	Call int64
	Ret  int64
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
		ops = append(ops, LockOp{
			Node: e.Node, Ino: ino, Kind: kind,
			Call: e.CallNs, Ret: e.ReturnNs,
		})
	}
	return ops, nil
}

// lockOperations turns decoded LockOps into Porcupine operations, each over
// the interval its etcd transaction spanned.
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
			ClientId: id, Input: op, Output: nil, Call: op.Call, Return: op.Ret,
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

// DefaultLockLeaseTTL is the session TTL the lock keys are written under, and
// so the window inside which etcd drops the locks of a node that stopped
// renewing. It matches the daemon's default --lease-ttl.
const DefaultLockLeaseTTL = 5 * time.Second

// withLeaseExpiryReleases appends a release for every lock a node still holds
// at the end of the history.
//
// A node killed mid-hold never records one: its locks are written under a
// session lease, and it is etcd that drops them when the lease stops being
// renewed, inside a process that no longer exists to log anything. Without
// this the lock is modelled as held forever and the next holder -- which
// acquired it perfectly legitimately -- reads as a mutual-exclusion
// violation. The chaos suite SIGKILLs daemons, so this is the common case,
// not an exotic one.
//
// The synthetic release spans [last event from that node, + leaseTTL], which
// is the honest bound: the lease cannot have expired before the node's last
// observed activity, and cannot survive a TTL past it. That interval also
// keeps a genuinely leaked lock a violation -- a node that stayed alive and
// simply failed to release goes on emitting events, so its synthetic release
// lands after them, well after any conflicting acquire.
func withLeaseExpiryReleases(ops []LockOp, leaseTTL time.Duration) []LockOp {
	lastSeen := map[string]int64{}
	for _, op := range ops {
		if op.Ret > lastSeen[op.Node] {
			lastSeen[op.Node] = op.Ret
		}
	}

	type holding struct {
		node string
		ino  uint64
		excl bool
	}
	held := map[holding]int{}
	for _, op := range ops {
		key := holding{node: op.Node, ino: op.Ino,
			excl: op.Kind == lockAcquireExclusive || op.Kind == lockReleaseExclusive}
		switch op.Kind {
		case lockAcquireExclusive, lockAcquireShared:
			held[key]++
		case lockReleaseExclusive, lockReleaseShared:
			held[key]--
		}
	}

	out := ops
	for key, n := range held {
		for i := 0; i < n; i++ {
			kind := lockReleaseShared
			if key.excl {
				kind = lockReleaseExclusive
			}
			out = append(out, LockOp{
				Node: key.node, Ino: key.ino, Kind: kind,
				Call: lastSeen[key.node],
				Ret:  lastSeen[key.node] + int64(leaseTTL),
			})
		}
	}
	return out
}

// CheckLocks checks a decoded lock history for mutual-exclusion violations.
//
// leaseTTL is the session TTL the locks were written under; pass
// DefaultLockLeaseTTL unless the cluster was configured otherwise.
func CheckLocks(ops []LockOp, leaseTTL, timeout time.Duration) porcupine.CheckResult {
	return porcupine.CheckOperationsTimeout(
		LockModel, lockOperations(withLeaseExpiryReleases(ops, leaseTTL)), timeout)
}
