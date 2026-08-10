package ipc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// opcodes matching pkg/fuse/ops.c
const (
	ipcOpLookup     = 1
	ipcOpGetattr    = 2
	ipcOpReaddir    = 3
	ipcOpReadlink   = 4
	ipcOpCreate     = 5
	ipcOpMkdir      = 6
	ipcOpUnlink     = 7
	ipcOpRmdir      = 8
	ipcOpRename     = 9
	ipcOpSymlink    = 10
	ipcOpLink       = 11
	ipcOpSetattr    = 12
	ipcOpOpen       = 13
	ipcOpRelease    = 14
	ipcOpOpendir    = 15
	ipcOpReleasedir = 16
	ipcOpStatfs     = 17
	ipcOpAlloc      = 18
	ipcOpCommit     = 19
	// 27 and 28 were GETLK/SETLK, removed so the kernel handles fcntl() locks
	// locally; not reused, so an old C daemon's lock request fails loudly.
	ipcOpRead        = 22
	ipcOpWrite       = 23
	ipcOpFsync       = 24
	ipcOpMknod       = 25
	ipcOpFlush       = 26
	ipcOpReadDirPlus = 29
)

// RunSocket starts a raw binary IPC server on the given listener.
// Each connection is handled in its own goroutine.
func (s *Service) RunSocket(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("accept: %w", err)
		}
		go s.handleConn(conn)
	}
}

func (s *Service) handleConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	s.log.Info("ipc connection accepted", "remote", conn.RemoteAddr())

	for {
		op, payload, err := recvReq(conn)
		if err != nil {
			if err != io.EOF {
				s.log.Warn("ipc recv", "error", err)
			}
			return
		}

		resp, err := s.safeDispatch(op, payload)
		if err != nil {
			s.log.Warn("ipc dispatch", "op", op, "error", err)
			return
		}

		if resp != nil {
			if err := sendResp(conn, resp); err != nil {
				s.log.Warn("ipc send", "error", err)
				return
			}
		}
	}
}

// safeDispatch runs a handler and turns a panic into one failed request.
//
// Connections are served one goroutine each with no recovery of their own, so
// an unrecovered panic ends the whole process — every mount this daemon serves,
// not just the request that tripped it.  The payload readers make that
// unreachable through a malformed frame; this is the backstop for everything
// else, and it logs loudly because reaching it is a bug either way.
func (s *Service) safeDispatch(op uint16, payload []byte) (resp []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			s.log.Error("ipc handler panicked", "op", op, "payload_len", len(payload),
				"panic", p, "stack", string(debug.Stack()))
			resp, err = int32Resp(-5), nil // EIO for this request only
		}
	}()
	return s.dispatch(op, payload)
}

// ---- wire format ----
//
// Request:  [u16:be opcode][u32:be payload_len][payload]
// Response: [u32:be payload_len][payload]

// maxFrameLen caps what one request may claim to carry.  The length is read
// straight off the wire and used as an allocation size, so an unbounded field
// lets a desynchronised peer ask for 4 GiB.  The largest legitimate frame is a
// write payload plus its fixed header, and the C daemon caps its own reads at
// the same number.
const maxFrameLen = 1 << 20 // 1 MiB

func recvReq(r io.Reader) (uint16, []byte, error) {
	var hdr [6]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	op := binary.BigEndian.Uint16(hdr[0:2])
	plen := binary.BigEndian.Uint32(hdr[2:6])
	if plen > maxFrameLen {
		// The stream is desynchronised: whatever follows is not a frame
		// boundary, so the connection cannot be recovered by skipping ahead.
		return 0, nil, fmt.Errorf("ipc frame of %d bytes exceeds the %d byte limit", plen, maxFrameLen)
	}

	payload := make([]byte, plen)
	if plen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, err
		}
	}
	return op, payload, nil
}

func sendResp(w io.Writer, data []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return nil
}

// ---- payload readers ----

// reader walks a request payload without ever slicing past its end.
//
// The peer is the local C daemon over a 0600 socket, so a malformed frame is a
// protocol desync rather than an attack — but a desync of exactly this kind has
// happened before, in the readdirplus parser, and every handler used to slice
// with a length field it had not checked.  One out-of-range length panicked the
// connection goroutine, and an unrecovered panic takes down the daemon serving
// every mount on the node.
//
// Once a read has run past the end, ok stays false and every later read returns
// a zero value, so a handler only has to test ok once, before it acts.
type reader struct {
	b  []byte
	ok bool
}

func newReader(payload []byte) *reader {
	return &reader{b: payload, ok: true}
}

func (r *reader) u32() uint32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func (r *reader) u64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// str reads a u32 length followed by that many bytes — the encoding every name
// and symlink target uses.
func (r *reader) str() string {
	n := r.u32()
	b := r.take(int(n))
	if b == nil {
		return ""
	}
	return string(b)
}

// blob is str's counterpart for payload data, returning the bytes themselves
// rather than a copy of them as a string.
func (r *reader) blob() []byte {
	n := r.u32()
	b := r.take(int(n))
	if b == nil {
		return nil
	}
	return b
}

func (r *reader) take(n int) []byte {
	if !r.ok || n < 0 || n > len(r.b) {
		r.ok = false
		return nil
	}
	b := r.b[:n]
	r.b = r.b[n:]
	return b
}

// ---- payload writers ----

type buf struct {
	b []byte
}

func (b *buf) w32(v uint32) {
	b.b = append(b.b,
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (b *buf) w64(v uint64) {
	b.b = append(b.b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (b *buf) wAttr(rec *metadata.InodeRecord) {
	b.w64(rec.Ino)
	b.w64(rec.Size)
	b.w64(rec.Blocks)
	b.w64(uint64(rec.Atime.Unix()))
	b.w64(uint64(rec.Mtime.Unix()))
	b.w64(uint64(rec.Ctime.Unix()))
	b.w32(uint32(rec.Atime.Nanosecond()))
	b.w32(uint32(rec.Mtime.Nanosecond()))
	b.w32(uint32(rec.Ctime.Nanosecond()))
	b.w32(rec.Mode)
	b.w32(rec.Nlink)
	b.w32(rec.UID)
	b.w32(rec.GID)
	b.w32(rec.Rdev)
	b.w32(rec.Blksize)
}

// ---- response helpers ----

func int32Resp(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func okResp() []byte {
	return int32Resp(0)
}

// writtenResp answers a WRITE: [i32:error][u32:written]
func writtenResp(written uint32) []byte {
	var b buf
	b.w32(0)
	b.w32(written)
	return b.b
}

// dataResp answers a READ: [i32:error][u32:len][bytes]
func dataResp(data []byte) []byte {
	var b buf
	b.w32(0)
	b.w32(uint32(len(data)))
	b.b = append(b.b, data...)
	return b.b
}

// entryResp answers every op that returns a directory entry — LOOKUP, CREATE,
// MKDIR, MKNOD, SYMLINK, LINK:
//
//	[i32:error][u64:ino][attr][u32:entry_timeout][u32:attr_timeout]
//
// The attr block is fixed-width, so a missing inode record is sent as a zeroed
// one rather than omitted; a short response would desynchronise the C parser.
func entryResp(ino uint64, rec *metadata.InodeRecord) []byte {
	if rec == nil {
		rec = &metadata.InodeRecord{Ino: ino}
	}
	var b buf
	b.w32(0) // success
	b.w64(ino)
	b.wAttr(rec)
	b.w32(1) // entry_timeout (seconds)
	b.w32(1) // attr_timeout (seconds)
	return b.b
}

func attrResp(rec *metadata.InodeRecord) []byte {
	var b buf
	b.w32(0) // error = success
	b.wAttr(rec)
	b.w32(1) // attr_timeout
	return b.b
}

// ---- dispatch ----

func (s *Service) dispatch(op uint16, payload []byte) ([]byte, error) {
	// Bounded here rather than at each of the ~35 individual store calls the
	// handlers make: a context without a deadline blocks for as long as the
	// etcd client will keep retrying, which under a partition is indefinitely,
	// and every handler inherits this one.  Calls that manage their own budget
	// (commitGuarded, retryKV) build their contexts from Background and are
	// deliberately not truncated by this deadline — a commit already in flight
	// should finish or fail on its own terms rather than be cut mid-transaction.
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	switch op {
	case ipcOpLookup:
		return s.handleLookup(ctx, payload)
	case ipcOpGetattr:
		return s.handleGetattr(ctx, payload)
	case ipcOpReaddir:
		return s.handleReaddir(ctx, payload)
	case ipcOpReadlink:
		return s.handleReadlink(ctx, payload)
	case ipcOpStatfs:
		return s.handleStatfs(ctx, payload)
	// Open/close and flush have no distributed state to keep: locks are taken
	// per operation and every write is committed before it is acknowledged.
	case ipcOpOpen, ipcOpOpendir, ipcOpRelease, ipcOpReleasedir, ipcOpFlush, ipcOpFsync:
		return okResp(), nil
	case ipcOpReadDirPlus:
		return s.handleReaddirPlus(ctx, payload)
	case ipcOpRead:
		return s.handleRead(ctx, payload)
	// Write operations
	case ipcOpCreate:
		return s.handleCreate(ctx, payload)
	case ipcOpMkdir:
		return s.handleMkdir(ctx, payload)
	case ipcOpUnlink:
		return s.handleUnlink(ctx, payload)
	case ipcOpRmdir:
		return s.handleRmdir(ctx, payload)
	case ipcOpRename:
		return s.handleRename(ctx, payload)
	case ipcOpWrite:
		return s.handleWrite(ctx, payload)
	case ipcOpSetattr:
		return s.handleSetattr(ctx, payload)
	case ipcOpSymlink:
		return s.handleSymlink(ctx, payload)
	case ipcOpLink:
		return s.handleLink(ctx, payload)
	case ipcOpMknod:
		return s.handleMknod(ctx, payload)
	case ipcOpAlloc, ipcOpCommit:
		// Block allocation — Phase 6
		return int32Resp(-38), nil // ENOSYS
	default:
		return int32Resp(-38), nil // ENOSYS
	}
}

// StartSocketServer is the public entry point: listens on the Unix socket path
// and blocks until ctx is cancelled or the listener errors.
//
// RunSocket's Accept loop has no ctx of its own — closing the listener from a
// side goroutine when ctx is cancelled is what turns "the process received
// SIGTERM" into "Accept returns an error and RunSocket returns", which is what
// lets main's post-serve shutdown steps (releasing this node's arena) run at
// all. Without this, main blocked in RunSocket forever and never reached them
// on anything short of SIGKILL.
func StartSocketServer(ctx context.Context, svc *Service, sockPath string, log *config.Logger) error {
	_ = os.Remove(sockPath)

	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("listen unix %s: %w", sockPath, err)
	}
	defer func() { _ = listener.Close() }()

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	if err := os.Chmod(sockPath, 0600); err != nil {
		log.Warn("cannot chmod socket", "path", sockPath, "error", err)
	}

	log.Info("binary IPC server listening", "path", sockPath)
	if err := svc.RunSocket(listener); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}
