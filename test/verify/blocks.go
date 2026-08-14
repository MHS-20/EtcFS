package verify

import (
	"fmt"
	"maps"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/MHS-20/EtcFS/internal/history"
)

// The block model: is a device range ever owned by two things at once, or given
// back twice?
//
// Every byte of the shared volume is handed out by exactly one node's allocator
// and returned by it, and the whole extent layer rests on that being true: two
// inodes reserving one range means one file's write silently overwrites
// another's, and a range freed twice is the same thing one step removed. Neither
// is visible in the WRITE/READ history — a corrupted read looks identical to a
// lost write — so the reservations and releases are recorded directly and
// checked here.
//
// The lifetime is modelled as a two-state machine per block: free or reserved.
// A reservation of a range not wholly free is a violation, and so is a release
// of a range not wholly reserved. Publication is deliberately not a third state.
// An extent is published by the flush transaction, which names blocks this node
// reserved and nothing else, and whether a *published* extent's blocks were
// freed underneath it is what fsck and the scrubber check against etcd itself —
// with the whole extent list in front of them, which a history does not have.
//
// Blocks are checked one at a time rather than as ranges: the events carry
// whole runs, but a violation is always about some particular block, and the
// state machine is far easier to be sure of at that granularity. The block size
// is the arena's, so a run of a megabyte is 256 steps of bookkeeping and not
// 262144.

const (
	blockOpcode       = 1004
	blockEventReserve = 0
	blockEventFree    = 1

	// blockSize matches arena.BlockSize. Copied rather than imported, for the
	// same reason the wire payloads are decoded again here: a checker that
	// shares the constant cannot notice the daemon changing it.
	blockSize = 4096
)

// BlockOp is one reservation or release of a device range.
type BlockOp struct {
	Node    string
	Reserve bool
	DiskOff uint64
	Length  uint64
	Call    int64
	Ret     int64
}

// DecodeBlocks turns a recorded history into its block lifetime events, in call
// order. Entries of other opcodes are skipped.
func DecodeBlocks(entries []history.Entry) ([]BlockOp, error) {
	ops := make([]BlockOp, 0, len(entries))
	for _, e := range entries {
		if e.Opcode != blockOpcode {
			continue
		}
		req, _, err := e.Payloads()
		if err != nil {
			return nil, err
		}
		if len(req) < 17 {
			return nil, fmt.Errorf("block event at %d: payload too short", e.CallNs)
		}
		event := req[0]
		if event != blockEventReserve && event != blockEventFree {
			return nil, fmt.Errorf("block event at %d: unknown event %d", e.CallNs, event)
		}
		r := newReader(req[1:])
		off, length := r.u64(), r.u64()
		if !r.ok {
			return nil, fmt.Errorf("block event at %d: truncated range", e.CallNs)
		}
		ops = append(ops, BlockOp{
			Node: e.Node, Reserve: event == blockEventReserve,
			DiskOff: off, Length: length, Call: e.CallNs, Ret: e.ReturnNs,
		})
	}
	return ops, nil
}

// blockState is what the history knows about each block: the node holding it,
// or blockFree for one this history has seen released.
//
// A block absent from the map is one the history has said nothing about, and
// that is not the same as free. An allocator is rebuilt from the extent records
// after a restart, so a node comes back holding blocks it never reserved inside
// the recorded window; freeing one of those is ordinary, and only a *second*
// release of the same block is the double free worth reporting.
type blockState map[uint64]string

const blockFree = ""

func (s blockState) step(op BlockOp) (bool, blockState) {
	next := maps.Clone(s)
	if next == nil {
		next = blockState{}
	}
	for off := op.DiskOff; off < op.DiskOff+op.Length; off += blockSize {
		holder, known := next[off]
		switch {
		case op.Reserve && known && holder != blockFree:
			// Two live owners for one block: the corruption every layer above
			// this assumes cannot happen.
			return false, s
		case op.Reserve:
			next[off] = op.Node
		case known && holder == blockFree:
			// Freed twice. The second release is what hands a block to a peer
			// while an extent still names it.
			return false, s
		default:
			next[off] = blockFree
		}
	}
	return true, next
}

// BlockModel is the per-block lifetime machine, partitioned by the arena-sized
// region the range falls in so that independent parts of the device are checked
// independently.
var BlockModel = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		// One partition per contiguous group: a run never spans two groups
		// because allocations come out of one arena, and separating them lets
		// Porcupine search each far smaller history on its own.
		byGroup := map[uint64][]porcupine.Operation{}
		for _, o := range h {
			g := o.Input.(BlockOp).DiskOff / blockGroupSize
			byGroup[g] = append(byGroup[g], o)
		}
		out := make([][]porcupine.Operation, 0, len(byGroup))
		for _, ops := range byGroup {
			out = append(out, ops)
		}
		return out
	},
	Init: func() interface{} { return blockState{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		ok, next := state.(blockState).step(input.(BlockOp))
		return ok, next
	},
	Equal: func(a, b interface{}) bool {
		return maps.Equal(a.(blockState), b.(blockState))
	},
	DescribeOperation: func(input, output interface{}) string {
		op := input.(BlockOp)
		kind := "free"
		if op.Reserve {
			kind = "reserve"
		}
		return fmt.Sprintf("%s(off=%d len=%d)", kind, op.DiskOff, op.Length)
	},
}

// blockGroupSize is the span one partition covers. It is the arena size, so a
// run — which is carved out of a single arena — never straddles two partitions
// and no violation can hide in the gap between them.
const blockGroupSize = 1 << 30

// CheckBlocks checks a decoded block history for double allocation and double
// free.
func CheckBlocks(ops []BlockOp, timeout time.Duration) porcupine.CheckResult {
	clients := map[string]int{}
	h := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		id, seen := clients[op.Node]
		if !seen {
			id = len(clients)
			clients[op.Node] = id
		}
		h = append(h, porcupine.Operation{
			ClientId: id, Input: op, Output: nil, Call: op.Call, Return: op.Ret,
		})
	}
	return porcupine.CheckOperationsTimeout(BlockModel, h, timeout)
}
