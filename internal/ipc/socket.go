package ipc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// opcodes matching pkg/fuse/ops.c
const (
	ipcOpLookup      = 1
	ipcOpGetattr     = 2
	ipcOpReaddir     = 3
	ipcOpReadlink    = 4
	ipcOpCreate      = 5
	ipcOpMkdir       = 6
	ipcOpUnlink      = 7
	ipcOpRmdir       = 8
	ipcOpRename      = 9
	ipcOpSymlink     = 10
	ipcOpLink        = 11
	ipcOpSetattr     = 12
	ipcOpOpen        = 13
	ipcOpRelease     = 14
	ipcOpOpendir     = 15
	ipcOpReleasedir  = 16
	ipcOpStatfs      = 17
	ipcOpAlloc       = 18
	ipcOpCommit      = 19
	ipcOpGetlk       = 27
	ipcOpSetlk       = 28
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

		resp, err := s.dispatch(op, payload)
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

// ---- wire format ----
//
// Request:  [u16:be opcode][u32:be payload_len][payload]
// Response: [u32:be payload_len][payload]

func recvReq(r io.Reader) (uint16, []byte, error) {
	var hdr [6]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	op := binary.BigEndian.Uint16(hdr[0:2])
	plen := binary.BigEndian.Uint32(hdr[2:6])

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

func readU64(b []byte) (uint64, []byte) {
	return binary.BigEndian.Uint64(b[:8]), b[8:]
}

func readU32(b []byte) (uint32, []byte) {
	return binary.BigEndian.Uint32(b[:4]), b[4:]
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
	case ipcOpGetlk:
		return s.handleGetlk(ctx, payload)
	case ipcOpSetlk:
		return s.handleSetlk(ctx, payload)
	// Write operations (Phase 3)
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
