package ipc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/anomalyco/etcfuse/internal/config"
	"github.com/anomalyco/etcfuse/pkg/metadata"
)

// opcodes matching pkg/fuse/ops.c
const (
	ipcOpLookup    = 1
	ipcOpGetattr   = 2
	ipcOpReaddir   = 3
	ipcOpReadlink  = 4
	ipcOpStatfs    = 17
	ipcOpOpen      = 13
	ipcOpRelease   = 14
	ipcOpOpendir   = 15
	ipcOpReleasedir = 16
	ipcOpRead      = 22
	ipcOpWrite     = 23
	ipcOpFlush     = 25 // FLUSH has no opcode currently defined
	ipcOpFsync     = 24
	ipcOpGetlk     = 20
	ipcOpSetlk     = 21
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
	_ = metadata.InodeRecord{} // ensure import
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

// ---- dispatch ----

func (s *Service) dispatch(op uint16, payload []byte) ([]byte, error) {
	ctx := context.Background()

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
	case ipcOpOpen, ipcOpOpendir, ipcOpRelease, ipcOpReleasedir, ipcOpFlush, ipcOpFsync:
		// No-op for read-only phase — return success
		return okResp(), nil
	case ipcOpRead:
		// Read returns empty data — no block device yet
		return int32Resp(0), nil
	case ipcOpGetlk:
		return okResp(), nil
	case ipcOpSetlk, ipcOpWrite:
		// Write operations — return EROFS during Phase 2
		return int32Resp(makeErrno(-30)), nil // EROFS
	default:
		return int32Resp(makeErrno(-38)), nil // ENOSYS
	}
}

// ---- handler implementations ----

// LOOKUP payload: [u64:parent][u32:name_len][name_bytes]
// Response: [i32:error][u64:ino][u64×9+u32×6:attr][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleLookup(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 12 {
		return int32Resp(makeErrno(-22)), nil // EINVAL
	}

	parent, rest := readU64(payload)
	nameLen, rest := readU32(rest)
	name := string(rest[:nameLen])

	// Resolve dirent
	ino, err := s.store.LookupDirent(ctx, parent, name)
	if err != nil {
		s.log.Warn("lookup", "parent", parent, "name", name, "error", err)
		return int32Resp(makeErrno(-5)), nil // EIO
	}
	if ino == 0 {
		return int32Resp(makeErrno(-2)), nil // ENOENT
	}

	// Get inode attrs
	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(makeErrno(-5)), nil // EIO
	}

	return lookupResp(0, ino, rec), nil
}

// GETATTR payload: [u64:ino]
// Response: [i32:error][u64×9+u32×6:attr][u32:attr_timeout]
func (s *Service) handleGetattr(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 8 {
		return int32Resp(makeErrno(-22)), nil
	}

	ino, _ := readU64(payload)

	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(makeErrno(-2)), nil // ENOENT
	}

	return attrResp(rec), nil
}

// READDIR payload: [u64:ino][u64:offset][u32:size]
// Response: [i32:error][u32:count][entries...]
// Each entry: [u64:ino][u32:name_len][name_bytes][u32:type][u64:off]
func (s *Service) handleReaddir(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(makeErrno(-22)), nil
	}

	ino, rest := readU64(payload)
	_, rest = readU64(rest) // offset (not used yet)
	_ = rest                // size hint

	entries, err := s.store.ListDirents(ctx, ino)
	if err != nil {
		return int32Resp(makeErrno(-5)), nil
	}

	var b buf
	b.w32(0) // error = success
	b.w32(uint32(len(entries)))

	for i, e := range entries {
		// Determine type from inode
		rec, _ := s.store.GetInode(ctx, e.Ino)
		dtype := uint32(metadata.DirentTypeFile)
		if rec != nil {
			if rec.Mode&metadata.ModeDir != 0 {
				dtype = metadata.DirentTypeDir
			} else if rec.Mode&metadata.ModeSymlink != 0 {
				dtype = metadata.DirentTypeSymlink
			}
		}

		b.w64(e.Ino)
		b.w32(uint32(len(e.Name)))
		b.b = append(b.b, []byte(e.Name)...)
		b.w32(dtype)
		b.w64(uint64(i + 1)) // directory offset cookie
	}

	return b.b, nil
}

// READLINK payload: [u64:ino]
// Response: [i32:error][u32:target_len][target_bytes]
func (s *Service) handleReadlink(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 8 {
		return int32Resp(makeErrno(-22)), nil
	}

	ino, _ := readU64(payload)

	target, err := s.store.Get(ctx, metadata.InodeSymlinkKey(ino))
	if err != nil || target == nil {
		return int32Resp(makeErrno(-2)), nil // ENOENT
	}

	var b buf
	b.w32(0) // error = success
	b.w32(uint32(len(target)))
	b.b = append(b.b, target...)
	return b.b, nil
}

// STATFS payload: empty
// Response: [i32:error][u64:blocks][u64:bfree][u64:bavail][u64:files][u64:ffree][u32:bsize][u32:namelen][u32:frsize]
func (s *Service) handleStatfs(ctx context.Context, _ []byte) ([]byte, error) {
	var b buf
	b.w32(0)         // error = success
	b.w64(1 << 30)   // blocks
	b.w64(1 << 29)   // bfree
	b.w64(1 << 29)   // bavail
	b.w64(1000000)   // files
	b.w64(900000)    // ffree
	b.w32(4096)      // bsize
	b.w32(255)       // namelen
	b.w32(4096)      // frsize
	return b.b, nil
}

// ---- response helpers ----

func makeErrno(e int) int32 { return int32(e) }

func int32Resp(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func okResp() []byte {
	return int32Resp(0)
}

func lookupResp(err int32, ino uint64, rec *metadata.InodeRecord) []byte {
	var b buf
	b.w32(uint32(err))
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

// StartSocketServer is the public entry point: listens on the Unix socket path
// and handles binary IPC connections.
func StartSocketServer(svc *Service, sockPath string, log *config.Logger) error {
	_ = os.Remove(sockPath)

	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("listen unix %s: %w", sockPath, err)
	}
	defer func() { _ = listener.Close() }()

	if err := os.Chmod(sockPath, 0600); err != nil {
		log.Warn("cannot chmod socket", "path", sockPath, "error", err)
	}

	log.Info("binary IPC server listening", "path", sockPath)
	return svc.RunSocket(listener)
}
