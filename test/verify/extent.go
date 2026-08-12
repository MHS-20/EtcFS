package verify

import (
	"encoding/binary"
	"fmt"
	"maps"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/MHS-20/EtcFS/internal/history"
)

// The extent model: does a read ever return bytes that disagree with every
// write that could have produced them?
//
// WRITE and READ are decoded from the daemon's own IPC history — the same
// entries the namespace model reads, no separate recording needed, since both
// operations already cross the socket and are already logged.
//
// Scope, stated plainly: this model only constrains a byte position once some
// operation in the history has shown a value for it, exactly the way the
// namespace model's dirState.known tracks names it has not yet seen evidence
// about. A read of a position the history never touched is accepted
// unconditionally — it might be a legitimate hole (reads as zero) or bytes
// written before the recorded window started, and this model has no way to
// tell those apart, so it does not guess. What it does catch: a write's bytes
// disappearing, a read returning a value that contradicts every write and
// every prior read of that position, or a torn read mixing bytes from two
// different writes at one offset. Truncate, fallocate and SETATTR-driven size
// changes are out of scope — they do not cross WRITE/READ, and this file adds
// nothing for them.
const (
	extentWriteOpcode = 23
	extentReadOpcode  = 22
)

// ExtentOp is one decoded WRITE or READ.
type ExtentOp struct {
	Node  string
	Ino   uint64
	Write bool // false for a read
	Off   uint64
	Data  []byte // the bytes written, or the bytes a read returned
	Errno int32
	Call  int64
	Ret   int64
}

// DecodeExtents turns a recorded history into WRITE/READ operations, in call
// order. Entries of other opcodes are skipped. A short write or a short read
// (fewer bytes acted on than requested) is decoded using the length the
// response actually reports, since that is what happened.
func DecodeExtents(entries []history.Entry) ([]ExtentOp, error) {
	ops := make([]ExtentOp, 0, len(entries))
	for _, e := range entries {
		switch e.Opcode {
		case extentWriteOpcode:
			op, err := decodeWrite(e)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)
		case extentReadOpcode:
			op, err := decodeRead(e)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)
		}
	}
	return ops, nil
}

func decodeWrite(e history.Entry) (ExtentOp, error) {
	req, resp, err := e.Payloads()
	if err != nil {
		return ExtentOp{}, err
	}
	if len(req) < 20 {
		return ExtentOp{}, fmt.Errorf("write at %d: request too short", e.CallNs)
	}
	ino := binary.BigEndian.Uint64(req)
	off := binary.BigEndian.Uint64(req[8:])
	dataLen := binary.BigEndian.Uint32(req[16:])
	if len(req) < 20+int(dataLen) {
		return ExtentOp{}, fmt.Errorf("write at %d: request truncated", e.CallNs)
	}
	data := req[20 : 20+dataLen]

	errno, written := int32(0), uint32(0)
	if len(resp) >= 4 {
		errno = int32(binary.BigEndian.Uint32(resp))
	}
	if errno == 0 && len(resp) >= 8 {
		written = binary.BigEndian.Uint32(resp[4:])
	}
	if written < uint32(len(data)) {
		data = data[:written]
	}
	return ExtentOp{Node: e.Node, Ino: ino, Write: true, Off: off, Data: data,
		Errno: errno, Call: e.CallNs, Ret: e.ReturnNs}, nil
}

func decodeRead(e history.Entry) (ExtentOp, error) {
	req, resp, err := e.Payloads()
	if err != nil {
		return ExtentOp{}, err
	}
	if len(req) < 20 {
		return ExtentOp{}, fmt.Errorf("read at %d: request too short", e.CallNs)
	}
	ino := binary.BigEndian.Uint64(req)
	off := binary.BigEndian.Uint64(req[8:])

	errno := int32(0)
	var data []byte
	if len(resp) >= 4 {
		errno = int32(binary.BigEndian.Uint32(resp))
	}
	if errno == 0 && len(resp) >= 8 {
		n := binary.BigEndian.Uint32(resp[4:])
		if len(resp) >= int(8+n) {
			data = resp[8 : 8+n]
		}
	}
	return ExtentOp{Node: e.Node, Ino: ino, Write: false, Off: off, Data: data,
		Errno: errno, Call: e.CallNs, Ret: e.ReturnNs}, nil
}

func extentOperations(ops []ExtentOp) []porcupine.Operation {
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

// byteState is the bytes this history has established, per absolute offset —
// deliberately sparse rather than a flat buffer, since only positions the
// history has touched carry any constraint at all.
type byteState map[uint64]byte

func (s byteState) step(op ExtentOp) (bool, byteState) {
	if op.Errno != 0 {
		// An operation the store rejected changed nothing; there is nothing
		// here to check or to learn from.
		return true, s
	}
	next := maps.Clone(s)
	if next == nil {
		next = byteState{}
	}
	for i, b := range op.Data {
		pos := op.Off + uint64(i)
		if !op.Write {
			// A read disagreeing with an established byte is the violation
			// this model exists to catch.
			if known, seen := next[pos]; seen && known != b {
				return false, s
			}
		}
		next[pos] = b
	}
	return true, next
}

// ExtentModel is the per-inode byte-position register described above.
var ExtentModel = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		byIno := map[uint64][]porcupine.Operation{}
		for _, o := range h {
			ino := o.Input.(ExtentOp).Ino
			byIno[ino] = append(byIno[ino], o)
		}
		out := make([][]porcupine.Operation, 0, len(byIno))
		for _, ops := range byIno {
			out = append(out, ops)
		}
		return out
	},
	Init: func() interface{} { return byteState{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		ok, next := state.(byteState).step(input.(ExtentOp))
		return ok, next
	},
	Equal: func(a, b interface{}) bool {
		return maps.Equal(a.(byteState), b.(byteState))
	},
	DescribeOperation: func(input, output interface{}) string {
		op := input.(ExtentOp)
		kind := "read"
		if op.Write {
			kind = "write"
		}
		return fmt.Sprintf("%s(ino=%d off=%d len=%d)", kind, op.Ino, op.Off, len(op.Data))
	},
}

// CheckExtents checks a decoded WRITE/READ history against the byte-register
// model. WRITE and READ are both linearizable as observed over the socket —
// see docs/verification/porcupine.md for why the write path's internal
// serializable pre-read does not need its own classifier here.
func CheckExtents(ops []ExtentOp, timeout time.Duration) porcupine.CheckResult {
	return porcupine.CheckOperationsTimeout(ExtentModel, extentOperations(ops), timeout)
}
