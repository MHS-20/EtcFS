package ipc

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// Every handler used to slice with a length field it had not checked, so a
// frame claiming more than it carries panicked the connection goroutine — and
// an unrecovered panic there ends the daemon serving every mount on the node.
func TestReaderRefusesLengthsPastTheEndOfThePayload(t *testing.T) {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint64(payload[0:], 42)   // parent
	binary.BigEndian.PutUint32(payload[8:], 4096) // a name length nothing follows

	r := newReader(payload)
	if parent := r.u64(); parent != 42 {
		t.Fatalf("parent read as %d", parent)
	}
	if name := r.str(); name != "" {
		t.Errorf("out-of-range name read as %q", name)
	}
	if r.ok {
		t.Error("a read past the end left the reader usable")
	}
	// Once short, every later read is zero rather than a panic.
	if r.u64() != 0 || r.u32() != 0 || r.blob() != nil {
		t.Error("reads after a short payload returned data")
	}
}

func TestReaderReadsAWellFormedPayload(t *testing.T) {
	var b buf
	b.w64(7)
	b.w32(3)
	b.b = append(b.b, "abc"...)
	b.w32(2)
	b.b = append(b.b, 0xde, 0xad)

	r := newReader(b.b)
	if got := r.u64(); got != 7 {
		t.Errorf("u64 = %d", got)
	}
	if got := r.str(); got != "abc" {
		t.Errorf("str = %q", got)
	}
	if got := r.blob(); !bytes.Equal(got, []byte{0xde, 0xad}) {
		t.Errorf("blob = %x", got)
	}
	if !r.ok {
		t.Error("a well-formed payload was rejected")
	}
}

// The frame length is read off the wire and used as an allocation size, so an
// unbounded field lets a desynchronised peer ask for 4 GiB.
func TestRecvReqRefusesAnOversizedFrame(t *testing.T) {
	var frame []byte
	frame = append(frame, 0, 1) // opcode
	frame = binary.BigEndian.AppendUint32(frame, maxFrameLen+1)

	if _, _, err := recvReq(bytes.NewReader(frame)); err == nil {
		t.Fatal("an oversized frame was accepted")
	}
}
