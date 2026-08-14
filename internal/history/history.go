// Package history records the operations a daemon served, as the input a
// consistency checker needs.
//
// One line of JSON per operation, with the request and the response exactly as
// they crossed the socket. Recording the raw frames rather than decoded fields
// is deliberate: the checker decodes them itself, so a disagreement between the
// daemon's encoder and the checker's reading of it shows up as a checker
// failure rather than being hidden by shared code — which is the point of
// checking the system with something other than its own assertions.
//
// A history is only as good as its clock. Each entry carries wall-clock call
// and return times, because entries from several nodes have to be ordered
// against each other; the checker treats a pair it cannot order as concurrent,
// so clock skew costs precision rather than correctness.
package history

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// OpStart marks the beginning of one daemon incarnation. It is not an
// operation the filesystem served; it is the boundary that tells a reader of
// the file where a restart happened. Numbered above the operation opcodes so
// it cannot collide with one.
const OpStart = 1005

// Entry is one served operation.
type Entry struct {
	Node   string `json:"node"`
	Op     string `json:"op"`
	Opcode uint16 `json:"opcode"`
	// CallNs and ReturnNs are Unix nanoseconds: the operation took effect at
	// some instant between them.
	CallNs   int64  `json:"call_ns"`
	ReturnNs int64  `json:"return_ns"`
	Request  string `json:"request"`  // base64 of the request payload
	Response string `json:"response"` // base64 of the response payload
}

// Recorder appends entries to a file. The zero value is not usable; a nil
// *Recorder is, and records nothing, so a caller never needs to check.
type Recorder struct {
	node string

	mu  sync.Mutex
	f   *os.File
	enc *json.Encoder
}

// NewRecorder opens path for appending. The file is not rotated or bounded:
// a history is recorded for the length of a test run, and a checker that is
// handed a truncated one would report violations that are artefacts of the
// truncation.
func NewRecorder(path, node string) (*Recorder, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("open history log %s: %w", path, err)
	}
	r := &Recorder{node: node, f: f, enc: json.NewEncoder(f)}
	// A restarted daemon appends to the same file under the same node name, so
	// without this marker one file reads as a single unbroken incarnation. That
	// matters to any checker reasoning about what a lease expiry dropped: the
	// keys of a killed daemon die one TTL after *that* incarnation stopped
	// renewing, not one TTL after the last thing the node name ever did. It is
	// recorded here rather than by a caller so that it cannot be forgotten:
	// exactly one lands at the head of every incarnation's entries.
	now := time.Now()
	r.Record("start", OpStart, now, now, nil, nil)
	return r, nil
}

// Record appends one operation. Errors are dropped: a history that cannot be
// written is a lost test result, never a reason to fail the filesystem
// operation that produced it.
func (r *Recorder) Record(op string, opcode uint16, call, ret time.Time, req, resp []byte) {
	if r == nil {
		return
	}
	e := Entry{
		Node:     r.node,
		Op:       op,
		Opcode:   opcode,
		CallNs:   call.UnixNano(),
		ReturnNs: ret.UnixNano(),
		Request:  base64.StdEncoding.EncodeToString(req),
		Response: base64.StdEncoding.EncodeToString(resp),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = r.enc.Encode(&e)
}

// Close flushes and closes the file.
func (r *Recorder) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.f.Close()
}

// Load reads a history back, in the order it was written.
func Load(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open history %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var entries []Entry
	dec := json.NewDecoder(f)
	for dec.More() {
		var e Entry
		if err := dec.Decode(&e); err != nil {
			return nil, fmt.Errorf("decode history %s: %w", path, err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Payloads decodes an entry's request and response back to bytes.
func (e Entry) Payloads() (req, resp []byte, err error) {
	req, err = base64.StdEncoding.DecodeString(e.Request)
	if err != nil {
		return nil, nil, fmt.Errorf("decode request of %s: %w", e.Op, err)
	}
	resp, err = base64.StdEncoding.DecodeString(e.Response)
	if err != nil {
		return nil, nil, fmt.Errorf("decode response of %s: %w", e.Op, err)
	}
	return req, resp, nil
}
