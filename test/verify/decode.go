// Package verify checks recorded operation histories against consistency
// models.
//
// The wire payloads are decoded here rather than reused from internal/ipc.
// That duplication is the point: a checker that shares the daemon's own decoder
// cannot detect a disagreement between what the daemon meant to write and what
// it wrote, and a verification tool that can only confirm the system's own
// assumptions verifies nothing.
package verify

import (
	"encoding/binary"
	"fmt"

	"github.com/MHS-20/EtcFS/internal/history"
)

// Kind is the operation a history entry describes, as far as the models care.
type Kind string

const (
	KindLookup Kind = "lookup"
	KindCreate Kind = "create"
	KindMkdir  Kind = "mkdir"
	KindMknod  Kind = "mknod"
	KindSymlnk Kind = "symlink"
	KindUnlink Kind = "unlink"
	KindRmdir  Kind = "rmdir"
	KindRename Kind = "rename"
	KindLink   Kind = "link"
)

// Op is one decoded namespace operation, with the interval it took effect in.
type Op struct {
	Kind   Kind
	Node   string
	Parent uint64
	Name   string
	// NewParent and NewName are the destination of a rename.
	NewParent uint64
	NewName   string
	// Ino is the inode the operation produced or referred to, when the wire
	// carries one: the created inode for a create, the linked inode for a link,
	// the resolved inode for a lookup.
	Ino uint64
	// Errno is 0 on success and the positive errno otherwise.
	Errno int32
	Call  int64
	Ret   int64
}

// reader decodes big-endian fields, refusing to read past the payload. It is a
// deliberate re-implementation of the daemon's own reader.
type reader struct {
	b   []byte
	pos int
	ok  bool
}

func newReader(b []byte) *reader { return &reader{b: b, ok: true} }

func (r *reader) u64() uint64 {
	if !r.ok || r.pos+8 > len(r.b) {
		r.ok = false
		return 0
	}
	v := binary.BigEndian.Uint64(r.b[r.pos:])
	r.pos += 8
	return v
}

func (r *reader) u32() uint32 {
	if !r.ok || r.pos+4 > len(r.b) {
		r.ok = false
		return 0
	}
	v := binary.BigEndian.Uint32(r.b[r.pos:])
	r.pos += 4
	return v
}

func (r *reader) str() string {
	n := int(r.u32())
	if !r.ok || r.pos+n > len(r.b) {
		r.ok = false
		return ""
	}
	s := string(r.b[r.pos : r.pos+n])
	r.pos += n
	return s
}

// errnoOf reads the leading int32 of a response, which every handler writes.
// It is negative on failure, and reported here as the positive errno.
func errnoOf(resp []byte) (int32, bool) {
	if len(resp) < 4 {
		return 0, false
	}
	code := int32(binary.BigEndian.Uint32(resp))
	if code < 0 {
		return -code, true
	}
	return 0, true
}

// namespaceOps are the opcodes the namespace model understands. Anything else
// in a history is skipped rather than guessed at.
var namespaceOps = map[uint16]Kind{
	1:  KindLookup,
	5:  KindCreate,
	6:  KindMkdir,
	7:  KindUnlink,
	8:  KindRmdir,
	9:  KindRename,
	10: KindSymlnk,
	11: KindLink,
	25: KindMknod,
}

// DecodeNamespace turns a recorded history into the namespace operations it
// contains, in the order recorded. Entries of other kinds are skipped.
func DecodeNamespace(entries []history.Entry) ([]Op, error) {
	ops := make([]Op, 0, len(entries))
	for _, e := range entries {
		kind, wanted := namespaceOps[e.Opcode]
		if !wanted {
			continue
		}
		req, resp, err := e.Payloads()
		if err != nil {
			return nil, err
		}
		errno, ok := errnoOf(resp)
		if !ok {
			return nil, fmt.Errorf("%s at %d: response is too short to hold an errno", e.Op, e.CallNs)
		}

		op := Op{Kind: kind, Node: e.Node, Errno: errno, Call: e.CallNs, Ret: e.ReturnNs}
		r := newReader(req)
		switch kind {
		case KindLookup, KindCreate, KindMkdir, KindMknod, KindSymlnk, KindUnlink, KindRmdir:
			op.Parent, op.Name = r.u64(), r.str()
		case KindRename:
			op.Parent, op.Name = r.u64(), r.str()
			op.NewParent, op.NewName = r.u64(), r.str()
		case KindLink:
			op.Ino, op.NewParent, op.NewName = r.u64(), r.u64(), r.str()
			op.Parent, op.Name = op.NewParent, op.NewName
		}
		if !r.ok {
			return nil, fmt.Errorf("%s at %d: request payload is truncated", e.Op, e.CallNs)
		}

		// A successful lookup, create, mkdir, mknod, symlink or link answers
		// with the inode it resolved to or made, right after the errno.
		if op.Errno == 0 && kind != KindUnlink && kind != KindRmdir && kind != KindRename {
			rr := newReader(resp[4:])
			if ino := rr.u64(); rr.ok {
				op.Ino = ino
			}
		}
		ops = append(ops, op)
	}
	return ops, nil
}
