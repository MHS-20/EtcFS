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
	extentWriteOpcode   = 23
	extentReadOpcode    = 22
	extentSetattrOpcode = 12
	extentFallocOpcode  = 35
	fattrSize           = 1 << 3 // FUSE_SET_ATTR_SIZE, from internal/ipc/handlers.go
)

// ExtentKind is what a decoded data-path operation does to a file's bytes.
type ExtentKind int

const (
	// ExtentWrite puts bytes at an offset; ExtentRead observes them.
	ExtentWrite ExtentKind = iota
	ExtentRead
	// ExtentTruncate sets the file's size: everything at or past Off stops
	// existing, and reads there return zeroes rather than what was written.
	ExtentTruncate
	// ExtentInvalidate marks a range as no longer described by this model --
	// a hole punched by fallocate, whose exact result is not worth modelling
	// precisely when forgetting the range is already sound.
	ExtentInvalidate
)

// ExtentOp is one decoded data-path operation.
type ExtentOp struct {
	Node string
	Ino  uint64
	Kind ExtentKind
	// Off is the byte offset for a write or read, the new size for a
	// truncate, and the start of the range for an invalidate.
	Off uint64
	// Len is the length of an invalidated range; unused otherwise.
	Len uint64
	// Data is the bytes written, or the bytes a read returned.
	Data  []byte
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
		case extentSetattrOpcode:
			op, ok, err := decodeTruncate(e)
			if err != nil {
				return nil, err
			}
			if ok {
				ops = append(ops, op)
			}
		case extentFallocOpcode:
			op, err := decodeFallocate(e)
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
	return ExtentOp{Node: e.Node, Ino: ino, Kind: ExtentWrite, Off: off, Data: data,
		Errno: errno, Call: e.CallNs, Ret: e.ReturnNs}, nil
}

// decodeTruncate picks out the setattr calls that change a file's size, which
// are the ones that change what a read returns. The kernel's valid mask says
// which fields it actually means; the rest carry whatever the caller's struct
// stat happened to hold.
func decodeTruncate(e history.Entry) (ExtentOp, bool, error) {
	req, resp, err := e.Payloads()
	if err != nil {
		return ExtentOp{}, false, err
	}
	if len(req) < 28 {
		return ExtentOp{}, false, fmt.Errorf("setattr at %d: request too short", e.CallNs)
	}
	valid := binary.BigEndian.Uint32(req[16:])
	if valid&fattrSize == 0 {
		return ExtentOp{}, false, nil
	}
	errno := int32(0)
	if len(resp) >= 4 {
		errno = int32(binary.BigEndian.Uint32(resp))
	}
	return ExtentOp{
		Node: e.Node, Ino: binary.BigEndian.Uint64(req), Kind: ExtentTruncate,
		Off: binary.BigEndian.Uint64(req[20:]), Errno: errno,
		Call: e.CallNs, Ret: e.ReturnNs,
	}, true, nil
}

// decodeFallocate treats every fallocate as invalidating its range. Punching a
// hole zeroes those bytes and the other modes only allocate, but forgetting
// the range covers all of them without having to be right about which.
func decodeFallocate(e history.Entry) (ExtentOp, error) {
	req, resp, err := e.Payloads()
	if err != nil {
		return ExtentOp{}, err
	}
	if len(req) < 28 {
		return ExtentOp{}, fmt.Errorf("fallocate at %d: request too short", e.CallNs)
	}
	errno := int32(0)
	if len(resp) >= 4 {
		errno = int32(binary.BigEndian.Uint32(resp))
	}
	return ExtentOp{
		Node: e.Node, Ino: binary.BigEndian.Uint64(req), Kind: ExtentInvalidate,
		Off: binary.BigEndian.Uint64(req[12:]), Len: binary.BigEndian.Uint64(req[20:]),
		Errno: errno, Call: e.CallNs, Ret: e.ReturnNs,
	}, nil
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
	return ExtentOp{Node: e.Node, Ino: ino, Kind: ExtentRead, Off: off, Data: data,
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

	switch op.Kind {
	case ExtentTruncate:
		// Everything at or past the new size stops existing. Reads there
		// return zeroes, which is not what was written, and holding on to
		// the old bytes would report that correct read as a contradiction.
		for pos := range next {
			if pos >= op.Off {
				delete(next, pos)
			}
		}
		return true, next
	case ExtentInvalidate:
		for pos := op.Off; pos < op.Off+op.Len; pos++ {
			delete(next, pos)
		}
		return true, next
	}

	for i, b := range op.Data {
		pos := op.Off + uint64(i)
		if op.Kind == ExtentRead {
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
		names := map[ExtentKind]string{
			ExtentWrite: "write", ExtentRead: "read",
			ExtentTruncate: "truncate", ExtentInvalidate: "invalidate",
		}
		return fmt.Sprintf("%s(ino=%d off=%d len=%d)", names[op.Kind], op.Ino, op.Off, len(op.Data))
	},
}

// CheckExtents checks a decoded WRITE/READ history against the byte-register
// model. WRITE and READ are both linearizable as observed over the socket —
// see docs/verification/porcupine.md for why the write path's internal
// serializable pre-read does not need its own classifier here.
func CheckExtents(ops []ExtentOp, timeout time.Duration) porcupine.CheckResult {
	return porcupine.CheckOperationsTimeout(ExtentModel, extentOperations(ops), timeout)
}
