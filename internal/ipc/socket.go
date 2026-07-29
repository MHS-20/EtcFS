package ipc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/pkg/blockio"
	"github.com/MHS-20/EtcFS/pkg/metadata"
	wal "github.com/MHS-20/EtcFS/pkg/walgo"
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
	case ipcOpOpen, ipcOpOpendir, ipcOpRelease, ipcOpReleasedir:
		return okResp(), nil
	case ipcOpFlush:
		return okResp(), nil
	case ipcOpFsync:
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
		return int32Resp(makeErrno(-38)), nil // ENOSYS
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

	ino, err := s.store.LookupDirent(ctx, parent, name)
	if err != nil {
		s.log.Warn("lookup dirent", "parent", parent, "name", name, "error", err)
		return int32Resp(makeErrno(-5)), nil
	}
	if ino == 0 {
		return int32Resp(makeErrno(-2)), nil // ENOENT
	}

	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		s.log.Warn("lookup getinode", "ino", ino, "error", err)
		return int32Resp(makeErrno(-5)), nil
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
			if (rec.Mode & metadata.S_IFMT) == metadata.ModeDir {
				dtype = metadata.DirentTypeDir
			} else if (rec.Mode & metadata.S_IFMT) == metadata.ModeSymlink {
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

func (s *Service) handleReaddirPlus(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(makeErrno(-22)), nil
	}
	ino, rest := readU64(payload)
	_, rest = readU64(rest)
	_ = rest

	entries, err := s.store.ListDirents(ctx, ino)
	if err != nil {
		return int32Resp(makeErrno(-5)), nil
	}

	var b buf
	b.w32(0)
	b.w32(uint32(len(entries)))

	for i, e := range entries {
		rec, _ := s.store.GetInode(ctx, e.Ino)
		dtype := uint32(metadata.DirentTypeFile)
		if rec != nil {
			if (rec.Mode & metadata.S_IFMT) == metadata.ModeDir {
				dtype = metadata.DirentTypeDir
			} else if (rec.Mode & metadata.S_IFMT) == metadata.ModeSymlink {
				dtype = metadata.DirentTypeSymlink
			}
		}
		b.w64(e.Ino)
		b.w32(uint32(len(e.Name)))
		b.b = append(b.b, []byte(e.Name)...)
		b.w32(dtype)
		b.w64(uint64(i + 1))
		if rec != nil {
			b.wAttr(rec)
		} else {
			for j := 0; j < 72; j++ {
				b.b = append(b.b, 0)
			}
		}
		b.w32(1)
		b.w32(1)
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
	blocks := uint64(1 << 30)
	bfree := uint64(1 << 29)
	if s.dev != nil {
		blocks = uint64(s.dev.TotalSize()) / 512
		ratio := s.alloc.LiveRatio()
		bfree = uint64(float64(blocks) * (1 - ratio))
	}
	inodeKvs, _ := s.store.GetPrefix(ctx, "inode:")
	files := uint64(len(inodeKvs))
	ffree := uint64(1000000 - len(inodeKvs))
	if ffree > 1000000 {
		ffree = 900000
	}

	var b buf
	b.w32(0)
	b.w64(blocks)
	b.w64(bfree)
	b.w64(bfree)
	b.w64(files)
	b.w64(ffree)
	b.w32(4096)
	b.w32(255)
	b.w32(4096)
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

// ---- write operation handlers (Phase 3) ----

// CREATE payload:  [u64:parent][u32:name_len][name][u32:mode][u32:flags][u32:umask]
// Response: [i32:error][u64:ino][u64×9+u32×6:attr][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleCreate(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(makeErrno(-22)), nil
	}
	parent, rest := readU64(payload)
	nameLen, rest := readU32(rest)
	name := string(rest[:nameLen])
	rest = rest[nameLen:]
	mode, rest := readU32(rest)
	_, rest = readU32(rest) // flags
	umask, _ := readU32(rest)
	_ = umask

	// Reserve inode number
	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(makeErrno(-28)), nil // ENOSPC
	}

	rec, err := s.store.AtomicCreateFile(ctx, parent, name, ino, mode, 1000, 1000)
	if err != nil {
		return int32Resp(makeErrno(-17)), nil // EEXIST
	}

	var b buf
	b.w32(0) // success
	b.w64(rec.Ino)
	b.wAttr(rec)
	b.w32(1) // entry_timeout
	b.w32(1) // attr_timeout
	return b.b, nil
}

// MKDIR payload:  [u64:parent][u32:name_len][name][u32:mode][u32:umask]
// Response: same as CREATE
func (s *Service) handleMkdir(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(makeErrno(-22)), nil
	}
	parent, rest := readU64(payload)
	nameLen, rest := readU32(rest)
	name := string(rest[:nameLen])
	rest = rest[nameLen:]
	mode, rest := readU32(rest)
	_, _ = readU32(rest) // umask

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(makeErrno(-28)), nil
	}

	rec, err := s.store.AtomicCreateDir(ctx, parent, name, ino, mode, 1000, 1000)
	if err != nil {
		return int32Resp(makeErrno(-17)), nil
	}

	var b buf
	b.w32(0)
	b.w64(rec.Ino)
	b.wAttr(rec)
	b.w32(1)
	b.w32(1)
	return b.b, nil
}

// UNLINK payload: [u64:parent][u32:name_len][name]
// Response: [i32:error]
func (s *Service) handleUnlink(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 12 {
		return int32Resp(makeErrno(-22)), nil
	}
	parent, rest := readU64(payload)
	nameLen, _ := readU32(rest)
	name := string(rest[4 : 4+nameLen])

	err := s.store.AtomicUnlink(ctx, parent, name)
	if err != nil {
		return int32Resp(makeErrno(-2)), nil // ENOENT
	}
	return okResp(), nil
}

// RMDIR payload: [u64:parent][u32:name_len][name]
// Response: [i32:error]
func (s *Service) handleRmdir(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 12 {
		return int32Resp(makeErrno(-22)), nil
	}
	parent, rest := readU64(payload)
	nameLen, _ := readU32(rest)
	name := string(rest[4 : 4+nameLen])

	ino, err := s.store.LookupDirent(ctx, parent, name)
	if err != nil || ino == 0 {
		return int32Resp(makeErrno(-2)), nil
	}
	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil || rec.Mode&metadata.ModeDir == 0 {
		return int32Resp(makeErrno(-20)), nil // ENOTDIR
	}
	// Check directory is empty
	entries, _ := s.store.ListDirents(ctx, ino)
	if len(entries) > 0 {
		return int32Resp(makeErrno(-39)), nil // ENOTEMPTY
	}

	err = s.store.AtomicUnlink(ctx, parent, name)
	if err != nil {
		return int32Resp(makeErrno(-2)), nil
	}
	return okResp(), nil
}

// RENAME payload: [u64:old_parent][u32:old_name_len][old_name][u64:new_parent][u32:new_name_len][new_name][u32:flags]
// Response: [i32:error]
func (s *Service) handleRename(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 28 {
		return int32Resp(makeErrno(-22)), nil
	}
	oldParent, rest := readU64(payload)
	oldNameLen, rest := readU32(rest)
	oldName := string(rest[:oldNameLen])
	rest = rest[oldNameLen:]
	newParent, rest := readU64(rest)
	newNameLen, rest := readU32(rest)
	newName := string(rest[:newNameLen])
	rest = rest[newNameLen:]
	flags, _ := readU32(rest)

	// Resolve old inode
	ino, err := s.store.LookupDirent(ctx, oldParent, oldName)
	if err != nil || ino == 0 {
		return int32Resp(makeErrno(-2)), nil
	}

	err = s.store.AtomicRename(ctx, oldParent, oldName, newParent, newName, ino, flags)
	if err != nil {
		return int32Resp(makeErrno(-17)), nil // EEXIST or other
	}
	return okResp(), nil
}

// WRITE payload: [u64:ino][u64:offset][u32:data_len][data]
// Response: [i32:error][u32:written]
func (s *Service) handleWrite(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(makeErrno(-22)), nil
	}
	ino, rest := readU64(payload)
	offset, rest := readU64(rest)
	dataLen, rest := readU32(rest)
	data := rest[:dataLen]

	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(makeErrno(-2)), nil
	}

	if s.dev != nil {
		return s.handleWriteBlock(ctx, ino, offset, data, rec)
	}

	// No block device — update size only (metadata-only mode)
	newEnd := offset + uint64(dataLen)
	if newEnd > rec.Size {
		rec.Size = newEnd
		_, _ = s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
	}

	var b buf
	b.w32(0)
	b.w32(dataLen)
	return b.b, nil
}

func (s *Service) handleWriteBlock(ctx context.Context, ino uint64, offset uint64,
	data []byte, rec *metadata.InodeRecord) ([]byte, error) {

	dataLen := len(data)
	if dataLen == 0 {
		var b buf
		b.w32(0)
		b.w32(0)
		return b.b, nil
	}

	if s.alloc.ArenaCount() == 0 {
		if _, err := s.alloc.AcquireArena(ctx); err != nil {
			s.log.Warn("write: cannot acquire arena", "error", err)
			return int32Resp(makeErrno(-28)), nil
		}
	}

	diskOff, err := s.alloc.Allocate(uint64(dataLen))
	if err != nil {
		if _, aerr := s.alloc.AcquireArena(ctx); aerr != nil {
			s.log.Warn("write: cannot allocate blocks or acquire arena", "error", err)
			return int32Resp(makeErrno(-28)), nil
		}
		diskOff, err = s.alloc.Allocate(uint64(dataLen))
		if err != nil {
			s.log.Warn("write: cannot allocate blocks even after arena expansion", "error", err)
			return int32Resp(makeErrno(-28)), nil
		}
	}

	writeData := data
	var alignedBuf []byte
	if s.dev.IsDirect() {
		ss := s.dev.SectorSize()
		alignedLen := (dataLen + ss - 1) / ss * ss
		buf, aerr := blockio.AlignedBuffer(alignedLen, ss)
		if aerr != nil {
			s.alloc.Free(diskOff, uint64(dataLen))
			return int32Resp(makeErrno(-12)), nil
		}
		alignedBuf = buf
		copy(buf, data)
		writeData = buf[:alignedLen]
	}
	defer func() {
		if alignedBuf != nil {
			_ = blockio.FreeBuffer(alignedBuf)
		}
	}()

	n, werr := s.dev.WriteAt(writeData, int64(diskOff))
	if werr != nil {
		s.alloc.Free(diskOff, uint64(dataLen))
		s.log.Warn("write: block device write failed", "error", werr)
		return int32Resp(makeErrno(-5)), nil
	}

	_ = s.dev.FlushDevice()

	if s.wal != nil {
		gen, _ := s.store.GetMyGeneration(ctx)
		if gen < 1 {
			gen = 1
		}
		_ = s.wal.Append(&wal.Entry{
			Ino:        ino,
			LogicalOff: offset,
			DiskOff:    diskOff,
			Length:     uint64(dataLen),
			Generation: gen,
		})
	}

	if err := s.dev.SyncRange(int64(diskOff), int64(n)); err != nil {
		s.log.Warn("write: sync failed", "error", err)
	}

	gen, _ := s.store.GetMyGeneration(ctx)
	if gen < 1 {
		gen = 1
	}

	chunk := s.nextExtentChunk(ctx, ino)
	extKey := fmt.Sprintf("extent:%d/%d", ino, chunk)
	extVal := fmt.Sprintf("%d,%d,%d,%d", offset, diskOff, uint64(dataLen), gen)
	_, _ = s.store.Put(ctx, extKey, []byte(extVal))

	newEnd := offset + uint64(dataLen)
	if newEnd > rec.Size {
		rec.Size = newEnd
		_, _ = s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
	}

	if s.wal != nil {
		_ = s.wal.MarkCommitted(ino, offset)
	}

	var b buf
	b.w32(0)
	b.w32(uint32(n))
	return b.b, nil
}

func (s *Service) nextExtentChunk(ctx context.Context, ino uint64) uint64 {
	prefix := fmt.Sprintf("extent:%d/", ino)
	kvs, _ := s.store.GetPrefix(ctx, prefix)
	return uint64(len(kvs))
}

// READ payload: [u64:ino][u64:offset][u32:size]
// Response: [i32:error][u32:data_len][data_bytes]
func (s *Service) handleRead(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(makeErrno(-22)), nil
	}
	ino, rest := readU64(payload)
	offset, rest := readU64(rest)
	size, rest := readU32(rest)
	_ = rest

	s.log.Info("READ", "ino", ino, "offset", offset, "size", size,
		"data", fmt.Sprintf("%x", payload))

	if s.dev == nil {
		return int32Resp(makeErrno(-5)), nil
	}

	prefix := fmt.Sprintf("extent:%d/", ino)
	kvs, _ := s.store.GetPrefix(ctx, prefix)
	s.log.Info("READ extents", "ino", ino, "count", len(kvs))
	if len(kvs) == 0 {
		var b buf
		b.w32(0)
		b.w32(0)
		return b.b, nil
	}

	data := make([]byte, size)
	bytesRead := uint32(0)
	rem := size

	for _, kv := range kvs {
		var logOff, diskOff, length, gen uint64
		_, _ = fmt.Sscanf(string(kv.Value), "%d,%d,%d,%d", &logOff, &diskOff, &length, &gen)

		s.log.Info("READ ext", "key", string(kv.Key), "log", logOff, "disk", diskOff, "len", length)

		eStart := logOff
		eEnd := logOff + length

		if offset >= eEnd || offset+uint64(rem) <= eStart {
			if offset < eStart && offset+uint64(rem) > eStart {
				gapLen := eStart - offset
				if gapLen > uint64(rem) {
					gapLen = uint64(rem)
				}
				bytesRead += uint32(gapLen)
				rem -= uint32(gapLen)
				offset += gapLen
			}
			continue
		}

		readStart := uint64(0)
		if offset > eStart {
			readStart = offset - eStart
		}
		readLen := length - readStart
		if readLen > uint64(rem) {
			readLen = uint64(rem)
		}

		n, err := s.dev.ReadAt(data[bytesRead:bytesRead+uint32(readLen)],
			int64(diskOff+readStart))
		if err != nil {
			break
		}
		bytesRead += uint32(n)
		rem -= uint32(readLen)
		if rem == 0 {
			break
		}
		offset = eStart + readLen
	}

	var b buf
	b.w32(0)
	b.w32(bytesRead)
	b.b = append(b.b, data[:bytesRead]...)
	return b.b, nil
}

// TRUNCATE (= setattr with size change) — handled by setattr
// SETATTR payload: [u64:ino][u64:fh][u32:valid][u64:size]
// Response: [i32:error][attr:84][u32:attr_timeout]
const fattrSize = 1 << 3

func (s *Service) handleSetattr(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 28 {
		return int32Resp(makeErrno(-22)), nil
	}
	ino, rest := readU64(payload)
	_, rest = readU64(rest)
	valid, rest := readU32(rest)
	newSize, _ := readU64(rest)

	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(makeErrno(-2)), nil
	}

	if valid&fattrSize != 0 && newSize < rec.Size {
		s.truncate(ctx, ino, newSize, rec)
		rec.Size = newSize
		_, _ = s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
	}

	return attrResp(rec), nil
}

func (s *Service) truncate(ctx context.Context, ino uint64, newSize uint64, rec *metadata.InodeRecord) {
	prefix := fmt.Sprintf("extent:%d/", ino)
	kvs, _ := s.store.GetPrefix(ctx, prefix)
	for _, kv := range kvs {
		var logOff, diskOff, length, gen uint64
		_, _ = fmt.Sscanf(string(kv.Value), "%d,%d,%d,%d", &logOff, &diskOff, &length, &gen)
		eEnd := logOff + length
		if logOff >= newSize {
			_ = s.store.Delete(ctx, string(kv.Key))
			if s.dev != nil {
				s.alloc.Free(diskOff, length)
			}
		} else if eEnd > newSize {
			keepLen := newSize - logOff
			freeLen := length - keepLen
			newVal := fmt.Sprintf("%d,%d,%d,%d", logOff, diskOff, keepLen, gen)
			_, _ = s.store.Put(ctx, string(kv.Key), []byte(newVal))
			if s.dev != nil {
				s.alloc.Free(diskOff+keepLen, freeLen)
			}
		}
	}
	_ = rec
}

// SYMLINK payload: [u64:parent][u32:name_len][name][u32:target_len][target]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleSymlink(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(makeErrno(-22)), nil
	}
	parent, rest := readU64(payload)
	nameLen, rest := readU32(rest)
	name := string(rest[:nameLen])
	rest = rest[nameLen:]
	targetLen, _ := readU32(rest)
	target := string(rest[4 : 4+targetLen])

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(makeErrno(-28)), nil
	}

	// Create inode with symlink mode
	_, err = s.store.CreateInode(ctx, ino, metadata.ModeSymlink|0777, 1000, 1000)
	if err != nil {
		return int32Resp(makeErrno(-17)), nil
	}

	// Store target
	_, _ = s.store.Put(ctx, metadata.InodeSymlinkKey(ino), []byte(target))

	// Create directory entry
	err = s.store.CreateDirent(ctx, parent, name, ino)
	if err != nil {
		return int32Resp(makeErrno(-17)), nil
	}

	var b buf
	b.w32(0)
	b.w64(ino)
	b.wAttr(&metadata.InodeRecord{
		Ino: ino, Mode: metadata.ModeSymlink | 0777, Nlink: 1, Size: uint64(len(target)),
	})
	b.w32(1) // entry_timeout
	b.w32(1) // attr_timeout
	return b.b, nil
}

// LINK payload: [u64:ino][u64:new_parent][u32:new_name_len][new_name]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleLink(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 24 {
		return int32Resp(makeErrno(-22)), nil
	}
	ino, rest := readU64(payload)
	newParent, rest := readU64(rest)
	nameLen, rest := readU32(rest)
	name := string(rest[:nameLen])

	// Increment nlink
	err := s.store.IncrementNlink(ctx, ino)
	if err != nil {
		return int32Resp(makeErrno(-2)), nil
	}

	// Create new directory entry
	err = s.store.CreateDirent(ctx, newParent, name, ino)
	if err != nil {
		return int32Resp(makeErrno(-17)), nil
	}

	rec, _ := s.store.GetInode(ctx, ino)
	var b buf
	b.w32(0)
	b.w64(ino)
	if rec != nil {
		b.wAttr(rec)
	}
	b.w32(1)
	b.w32(1)
	return b.b, nil
}

// MKNOD payload: [u64:parent][u32:name_len][name][u32:mode][u32:rdev]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleMknod(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 24 {
		return int32Resp(makeErrno(-22)), nil
	}
	parent, rest := readU64(payload)
	nameLen, rest := readU32(rest)
	name := string(rest[:nameLen])
	rest = rest[nameLen:]
	mode, rest := readU32(rest)
	rdev, _ := readU32(rest)

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(makeErrno(-28)), nil
	}

	rec, err := s.store.CreateInode(ctx, ino, mode, 1000, 1000)
	if err != nil {
		return int32Resp(makeErrno(-17)), nil
	}
	rec.Rdev = rdev
	_, _ = s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))

	err = s.store.CreateDirent(ctx, parent, name, ino)
	if err != nil {
		return int32Resp(makeErrno(-17)), nil
	}

	var b buf
	b.w32(0)
	b.w64(ino)
	b.wAttr(rec)
	b.w32(1)
	b.w32(1)
	return b.b, nil
}

// ---- lock handlers ----

// GETLK payload: [u64:ino][u64:start][u64:len][u32:type][u32:pid]
func (s *Service) handleGetlk(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 40 {
		return int32Resp(makeErrno(-22)), nil
	}
	ino, rest := readU64(payload)
	start, rest := readU64(rest)
	length, rest := readU64(rest)
	ltype, rest := readU32(rest)
	pid, _ := readU32(rest)

	rec, _ := s.store.GetLockInfo(ctx, ino)
	_ = start
	_ = length
	_ = pid

	if rec == nil || rec.Mode == string(metadata.LockShared) {
		var b buf
		b.w32(0)
		b.w64(start)
		b.w64(length)
		b.w32(0)
		b.w32(pid)
		return b.b, nil
	}
	var b buf
	b.w32(0)
	b.w64(start)
	b.w64(length)
	b.w32(ltype)
	b.w32(pid)
	return b.b, nil
}

// SETLK payload: [u64:ino][u64:start][u64:len][u32:type][u32:pid][u32:sleep]
func (s *Service) handleSetlk(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 44 {
		return int32Resp(makeErrno(-22)), nil
	}
	ino, rest := readU64(payload)
	_, rest = readU64(rest) // start
	_, rest = readU64(rest) // len
	ltype, rest := readU32(rest)
	_, rest = readU32(rest) // pid
	_, _ = readU32(rest)    // sleep
	_ = ino

	if ltype == 2 { // F_UNLCK
		return okResp(), nil
	}
	return int32Resp(makeErrno(-11)), nil // EAGAIN
}

// allocInode reserves an inode number from etcd.
// Simple sequential allocation for Phase 3.
func (s *Service) allocInode(ctx context.Context) (uint64, error) {
	for attempt := 0; attempt < 8; attempt++ {
		v, err := s.store.Get(ctx, metadata.KeyInodeAllocCounter)
		if err != nil {
			return 0, err
		}
		current := uint64(0)
		if v != nil {
			current = metadata.DecodeUint64(v)
		}
		next := current + 1

		var cmps []clientv3.Cmp
		if current == 0 {
			cmps = []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(metadata.KeyInodeAllocCounter), "=", 0)}
		} else {
			cmps = []clientv3.Cmp{clientv3.Compare(clientv3.Value(metadata.KeyInodeAllocCounter), "=", string(metadata.EncodeUint64(current)))}
		}

		ok, err := s.store.Txn(ctx, cmps,
			[]clientv3.Op{clientv3.OpPut(metadata.KeyInodeAllocCounter, string(metadata.EncodeUint64(next)))}, nil)
		if err != nil {
			return 0, err
		}
		if ok {
			return current, nil
		}
		time.Sleep(time.Duration(1<<attempt) * time.Millisecond)
	}
	return 0, fmt.Errorf("inode alloc exhausted")
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
