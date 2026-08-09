package ipc

import (
	"context"
	"errors"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// Metadata operation handlers: everything answerable from etcd alone.
// Operations that touch the block device live in datapath.go.

// errnoFor maps a store error to the errno the FUSE client should see.
//
// A fencing rejection must surface as EIO and never as the operation's usual
// failure code: reporting a fenced create as EEXIST, or a fenced unlink as
// ENOENT, makes a fencing bug indistinguishable from ordinary contention in a
// fuzz log, and misleads anyone reading the mount's errors during an incident.
// Anything else keeps the caller's ordinary errno.
func errnoFor(err error, fallback int32) int32 {
	if errors.Is(err, metadata.ErrFenced) || errors.Is(err, metadata.ErrGuardUnavailable) {
		return -5 // EIO
	}
	return fallback
}

// LOOKUP payload: [u64:parent][u32:name_len][name_bytes]
// Response: [i32:error][u64:ino][u64×9+u32×6:attr][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleLookup(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 12 {
		return int32Resp(-22), nil // EINVAL
	}

	parent, rest := readU64(payload)
	nameLen, rest := readU32(rest)
	name := string(rest[:nameLen])

	ino, err := s.store.LookupDirent(ctx, parent, name)
	if err != nil {
		s.log.Warn("lookup dirent", "parent", parent, "name", name, "error", err)
		return int32Resp(-5), nil
	}
	if ino == 0 {
		return int32Resp(-2), nil // ENOENT
	}

	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		s.log.Warn("lookup getinode", "ino", ino, "error", err)
		return int32Resp(-5), nil
	}

	return entryResp(ino, rec), nil
}

// GETATTR payload: [u64:ino]
// Response: [i32:error][u64×9+u32×6:attr][u32:attr_timeout]
func (s *Service) handleGetattr(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 8 {
		return int32Resp(-22), nil
	}

	ino, _ := readU64(payload)

	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(-2), nil // ENOENT
	}

	return attrResp(rec), nil
}

// READDIR payload: [u64:ino][u64:offset][u32:size]
// Response: [i32:error][u32:count][entries...]
// Each entry: [u64:ino][u32:name_len][name_bytes][u32:type][u64:off]
func (s *Service) handleReaddir(ctx context.Context, payload []byte) ([]byte, error) {
	return s.readdirResp(ctx, payload, false)
}

// READDIRPLUS is READDIR with an attr block and two timeouts appended to
// every entry.
func (s *Service) handleReaddirPlus(ctx context.Context, payload []byte) ([]byte, error) {
	return s.readdirResp(ctx, payload, true)
}

func (s *Service) readdirResp(ctx context.Context, payload []byte, plus bool) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(-22), nil
	}

	ino, _ := readU64(payload)
	// The remaining offset and size hints are unused: the whole directory is
	// returned and the C daemon skips entries at or below its own cookie.

	entries, err := s.store.ListDirents(ctx, ino)
	if err != nil {
		return int32Resp(-5), nil
	}

	var b buf
	b.w32(0) // error = success
	b.w32(uint32(len(entries)))

	for i, e := range entries {
		rec, _ := s.store.GetInode(ctx, e.Ino)

		b.w64(e.Ino)
		b.w32(uint32(len(e.Name)))
		b.b = append(b.b, []byte(e.Name)...)
		b.w32(direntType(rec))
		b.w64(uint64(i + 1)) // directory offset cookie

		if !plus {
			continue
		}
		// The attr block is fixed-width, so an entry whose inode record has
		// vanished still has to write a full-size placeholder — a short write
		// here desynchronises the C parser and turns every following entry
		// into garbage.
		if rec == nil {
			rec = &metadata.InodeRecord{Ino: e.Ino}
		}
		b.wAttr(rec)
		b.w32(1) // entry_timeout (seconds)
		b.w32(1) // attr_timeout (seconds)
	}

	return b.b, nil
}

// direntType maps an inode record to its DT_* directory entry type.
// A missing record is reported as a regular file.
func direntType(rec *metadata.InodeRecord) uint32 {
	if rec == nil {
		return metadata.DirentTypeFile
	}
	switch rec.Mode & metadata.S_IFMT {
	case metadata.ModeDir:
		return metadata.DirentTypeDir
	case metadata.ModeSymlink:
		return metadata.DirentTypeSymlink
	default:
		return metadata.DirentTypeFile
	}
}

// READLINK payload: [u64:ino]
// Response: [i32:error][u32:target_len][target_bytes]
func (s *Service) handleReadlink(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 8 {
		return int32Resp(-22), nil
	}

	ino, _ := readU64(payload)

	target, err := s.store.Get(ctx, metadata.InodeSymlinkKey(ino))
	if err != nil || target == nil {
		return int32Resp(-2), nil // ENOENT
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

// ---- write operation handlers (Phase 3) ----

// CREATE payload:  [u64:parent][u32:name_len][name][u32:mode][u32:flags][u32:umask]
// Response: [i32:error][u64:ino][u64×9+u32×6:attr][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleCreate(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(-22), nil
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
		return int32Resp(-28), nil // ENOSPC
	}

	rec, err := s.store.AtomicCreateFile(ctx, parent, name, ino, mode, 1000, 1000)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil // EEXIST unless fenced
	}

	return entryResp(rec.Ino, rec), nil
}

// MKDIR payload:  [u64:parent][u32:name_len][name][u32:mode][u32:umask]
// Response: same as CREATE
func (s *Service) handleMkdir(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(-22), nil
	}
	parent, rest := readU64(payload)
	nameLen, rest := readU32(rest)
	name := string(rest[:nameLen])
	rest = rest[nameLen:]
	mode, rest := readU32(rest)
	_, _ = readU32(rest) // umask

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(-28), nil
	}

	rec, err := s.store.AtomicCreateDir(ctx, parent, name, ino, mode, 1000, 1000)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil
	}

	return entryResp(rec.Ino, rec), nil
}

// UNLINK payload: [u64:parent][u32:name_len][name]
// Response: [i32:error]
func (s *Service) handleUnlink(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 12 {
		return int32Resp(-22), nil
	}
	parent, rest := readU64(payload)
	nameLen, _ := readU32(rest)
	name := string(rest[4 : 4+nameLen])

	err := s.store.AtomicUnlink(ctx, parent, name)
	if err != nil {
		return int32Resp(errnoFor(err, -2)), nil // ENOENT unless fenced
	}
	return okResp(), nil
}

// RMDIR payload: [u64:parent][u32:name_len][name]
// Response: [i32:error]
func (s *Service) handleRmdir(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 12 {
		return int32Resp(-22), nil
	}
	parent, rest := readU64(payload)
	nameLen, _ := readU32(rest)
	name := string(rest[4 : 4+nameLen])

	ino, err := s.store.LookupDirent(ctx, parent, name)
	if err != nil || ino == 0 {
		return int32Resp(-2), nil
	}
	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil || rec.Mode&metadata.ModeDir == 0 {
		return int32Resp(-20), nil // ENOTDIR
	}
	// Check directory is empty
	entries, _ := s.store.ListDirents(ctx, ino)
	if len(entries) > 0 {
		return int32Resp(-39), nil // ENOTEMPTY
	}

	err = s.store.AtomicUnlink(ctx, parent, name)
	if err != nil {
		return int32Resp(errnoFor(err, -2)), nil
	}
	return okResp(), nil
}

// RENAME payload: [u64:old_parent][u32:old_name_len][old_name][u64:new_parent][u32:new_name_len][new_name][u32:flags]
// Response: [i32:error]
func (s *Service) handleRename(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 28 {
		return int32Resp(-22), nil
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
		return int32Resp(-2), nil
	}

	err = s.store.AtomicRename(ctx, oldParent, oldName, newParent, newName, ino, flags)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil // EEXIST or other, unless fenced
	}
	return okResp(), nil
}

// TRUNCATE (= setattr with size change) — handled by setattr
// SETATTR payload: [u64:ino][u64:fh][u32:valid][u64:size]
// Response: [i32:error][attr:84][u32:attr_timeout]
const fattrSize = 1 << 3

func (s *Service) handleSetattr(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 28 {
		return int32Resp(-22), nil
	}
	ino, rest := readU64(payload)
	_, rest = readU64(rest)
	valid, rest := readU32(rest)
	newSize, _ := readU64(rest)

	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(-2), nil
	}

	if valid&fattrSize != 0 && newSize < rec.Size {
		s.truncate(ctx, ino, newSize)
		rec.Size = newSize
		if _, err := s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec)); err != nil {
			// Previously discarded.  A fenced node must not report a truncate
			// as successful when the size update was rejected.
			return int32Resp(errnoFor(err, -5)), nil
		}
	}

	return attrResp(rec), nil
}

// SYMLINK payload: [u64:parent][u32:name_len][name][u32:target_len][target]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleSymlink(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(-22), nil
	}
	parent, rest := readU64(payload)
	nameLen, rest := readU32(rest)
	name := string(rest[:nameLen])
	rest = rest[nameLen:]
	targetLen, _ := readU32(rest)
	target := string(rest[4 : 4+targetLen])

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(-28), nil
	}

	// Create inode with symlink mode
	_, err = s.store.CreateInode(ctx, ino, metadata.ModeSymlink|0777, 1000, 1000)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil
	}

	// Store target
	if _, err := s.store.Put(ctx, metadata.InodeSymlinkKey(ino), []byte(target)); err != nil {
		return int32Resp(errnoFor(err, -5)), nil
	}

	// Create directory entry
	err = s.store.CreateDirent(ctx, parent, name, ino)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil
	}

	return entryResp(ino, &metadata.InodeRecord{
		Ino: ino, Mode: metadata.ModeSymlink | 0777, Nlink: 1, Size: uint64(len(target)),
	}), nil
}

// LINK payload: [u64:ino][u64:new_parent][u32:new_name_len][new_name]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleLink(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 24 {
		return int32Resp(-22), nil
	}
	ino, rest := readU64(payload)
	newParent, rest := readU64(rest)
	nameLen, rest := readU32(rest)
	name := string(rest[:nameLen])

	// Increment nlink
	err := s.store.IncrementNlink(ctx, ino)
	if err != nil {
		return int32Resp(errnoFor(err, -2)), nil
	}

	// Create new directory entry
	err = s.store.CreateDirent(ctx, newParent, name, ino)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil
	}

	rec, _ := s.store.GetInode(ctx, ino)
	return entryResp(ino, rec), nil
}

// MKNOD payload: [u64:parent][u32:name_len][name][u32:mode][u32:rdev]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleMknod(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 24 {
		return int32Resp(-22), nil
	}
	parent, rest := readU64(payload)
	nameLen, rest := readU32(rest)
	name := string(rest[:nameLen])
	rest = rest[nameLen:]
	mode, rest := readU32(rest)
	rdev, _ := readU32(rest)

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(-28), nil
	}

	rec, err := s.store.CreateInode(ctx, ino, mode, 1000, 1000)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil
	}
	rec.Rdev = rdev
	if _, err := s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec)); err != nil {
		return int32Resp(errnoFor(err, -5)), nil
	}

	err = s.store.CreateDirent(ctx, parent, name, ino)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil
	}

	return entryResp(ino, rec), nil
}

// ---- lock handlers ----
//
// EtcFS does not track byte-range locks in etcd.  The lock:<ino> keys used by
// the read and write paths are whole-inode leases held for the duration of a
// single operation, which is a different thing from a process-owned POSIX
// record lock and cannot answer GETLK/SETLK on its own.
//
// So both handlers report "no conflict", which leaves the kernel's own local
// lock bookkeeping in charge: correct within one node, not enforced across
// nodes.  This matches the documented Phase 3 behaviour.  Reporting a
// conflict instead would be worse than useless — SETLK cannot grant a lock it
// does not track, so every caller would spin on EAGAIN forever.

// GETLK and SETLK are deliberately not handled here, and the C daemon does not
// implement the matching FUSE operations.  libfuse's contract is that "if the
// locking methods are not implemented, the kernel will still allow file locking
// to work locally" — implementing them takes that job away from the kernel, so
// answering "always free / always granted" left fcntl() locks excluding nothing
// at all, not even between two processes on one node.  Leaving them unwired
// gives fcntl() the same node-local-correct behavior flock() already has.
// See docs/architecture/metadata/posix-lock-operations.md.

// allocInode reserves an inode number from etcd.
//
// Inodes 0 and 1 are both reserved: 0 is not a valid inode, and 1 is
// FUSE_ROOT_ID — the root directory, which the C daemon answers for locally
// and which seed-etcd writes to inode:1.  Handing 1 out to a regular file
// overwrites the root inode record and makes the whole mount return EIO, so
// the first allocation must be 2.
func (s *Service) allocInode(ctx context.Context) (uint64, error) {
	return s.store.NextCounter(ctx, metadata.KeyInodeAllocCounter, metadata.FirstUsableIno)
}
