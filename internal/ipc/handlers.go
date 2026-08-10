package ipc

import (
	"context"
	"errors"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

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
	switch {
	case errors.Is(err, metadata.ErrFenced), errors.Is(err, metadata.ErrGuardUnavailable):
		return -5 // EIO
	case errors.Is(err, metadata.ErrExists):
		return -17 // EEXIST
	case errors.Is(err, metadata.ErrNotFound):
		return -2 // ENOENT
	case errors.Is(err, metadata.ErrNotDir):
		return -20 // ENOTDIR
	case errors.Is(err, metadata.ErrIsDir):
		return -21 // EISDIR
	case errors.Is(err, metadata.ErrInvalid):
		return -22 // EINVAL
	case errors.Is(err, metadata.ErrNotEmpty):
		return -39 // ENOTEMPTY
	case errors.Is(err, metadata.ErrPerm):
		return -1 // EPERM
	}
	return fallback
}

// LOOKUP payload: [u64:parent][u32:name_len][name_bytes]
// Response: [i32:error][u64:ino][u64×9+u32×6:attr][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleLookup(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	if !r.ok {
		return int32Resp(-22), nil // EINVAL
	}

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
	r := newReader(payload)
	ino := r.u64()
	if !r.ok {
		return int32Resp(-22), nil
	}

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
	r := newReader(payload)
	ino := r.u64()
	r.u64() // offset hint, unused
	r.u32() // size hint, unused: the whole directory is returned and the C
	// daemon skips entries at or below its own cookie.
	if !r.ok {
		return int32Resp(-22), nil
	}

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
	r := newReader(payload)
	ino := r.u64()
	if !r.ok {
		return int32Resp(-22), nil
	}

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

// applyUmask clears the permission bits the caller's umask masks out, leaving
// the file type alone.  The kernel does not apply it for us on a filesystem
// that declares FUSE_DONT_MASK behaviour, and every create path needs the same
// treatment, so it lives here rather than at four call sites.
func applyUmask(mode, umask uint32) uint32 {
	return mode &^ (umask & 0777)
}

// CREATE payload:  [u64:parent][u32:name_len][name][u32:mode][u32:flags][u32:umask][u32:uid][u32:gid]
// Response: [i32:error][u64:ino][u64×9+u32×6:attr][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleCreate(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	mode := r.u32()
	r.u32() // flags
	umask, uid, gid := r.u32(), r.u32(), r.u32()
	if !r.ok {
		return int32Resp(-22), nil
	}

	// Reserve inode number
	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(-28), nil // ENOSPC
	}

	rec, err := s.store.AtomicCreateFile(ctx, parent, name, ino, applyUmask(mode, umask), uid, gid)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil // EEXIST unless fenced
	}

	return entryResp(rec.Ino, rec), nil
}

// MKDIR payload:  [u64:parent][u32:name_len][name][u32:mode][u32:umask][u32:uid][u32:gid]
// Response: same as CREATE
func (s *Service) handleMkdir(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	mode, umask, uid, gid := r.u32(), r.u32(), r.u32(), r.u32()
	if !r.ok {
		return int32Resp(-22), nil
	}

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(-28), nil
	}

	rec, err := s.store.AtomicCreateDir(ctx, parent, name, ino, applyUmask(mode, umask), uid, gid)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil
	}

	return entryResp(rec.Ino, rec), nil
}

// UNLINK payload: [u64:parent][u32:name_len][name]
// Response: [i32:error]
func (s *Service) handleUnlink(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	if !r.ok {
		return int32Resp(-22), nil
	}

	err := s.store.AtomicUnlink(ctx, parent, name)
	if err != nil {
		return int32Resp(errnoFor(err, -2)), nil // ENOENT unless fenced
	}
	return okResp(), nil
}

// RMDIR payload: [u64:parent][u32:name_len][name]
// Response: [i32:error]
func (s *Service) handleRmdir(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	if !r.ok {
		return int32Resp(-22), nil
	}

	if err := s.store.AtomicRmdir(ctx, parent, name); err != nil {
		return int32Resp(errnoFor(err, -2)), nil
	}
	return okResp(), nil
}

// RENAME payload: [u64:old_parent][u32:old_name_len][old_name][u64:new_parent][u32:new_name_len][new_name][u32:flags]
// Response: [i32:error]
func (s *Service) handleRename(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	oldParent, oldName := r.u64(), r.str()
	newParent, newName := r.u64(), r.str()
	flags := r.u32()
	if !r.ok {
		return int32Resp(-22), nil
	}

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

// SETATTR payload:
//
//	[u64:ino][u64:fh][u32:valid][u64:size][u32:mode][u32:uid][u32:gid]
//	[u64:atime][u64:mtime][u64:ctime]
//
// Response: [i32:error][attr:84][u32:attr_timeout]
//
// valid is the kernel's FUSE_SET_ATTR_* mask saying which of those fields it
// actually means; the rest carry whatever the caller's struct stat happened to
// hold and must be ignored.
const (
	fattrMode     = 1 << 0
	fattrUID      = 1 << 1
	fattrGID      = 1 << 2
	fattrSize     = 1 << 3
	fattrAtime    = 1 << 4
	fattrMtime    = 1 << 5
	fattrAtimeNow = 1 << 7
	fattrMtimeNow = 1 << 8
	fattrCtime    = 1 << 10
)

const setattrPayloadLen = 8 + 8 + 4 + 8 + 4 + 4 + 4 + 8 + 8 + 8

func (s *Service) handleSetattr(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino := r.u64()
	r.u64() // fh
	valid := r.u32()
	newSize := r.u64()
	mode, uid, gid := r.u32(), r.u32(), r.u32()
	atime, mtime, ctime := r.u64(), r.u64(), r.u64()
	if !r.ok {
		return int32Resp(-22), nil
	}

	rec, rev, err := s.store.GetInodeRev(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(-2), nil
	}

	// Shrinking releases the extents past the new end, and that runs before the
	// size is published: metadata-then-data, so no reader can still resolve a
	// range whose blocks have already gone back to the arena.
	if valid&fattrSize != 0 && newSize < rec.Size {
		if terr := s.truncate(ctx, ino, newSize); terr != nil {
			// Reporting success here would tell the caller the file had shrunk
			// while every extent past the new end was still readable — which is
			// what a fenced node did, since the guard rejects its writes.
			s.log.Error("truncate failed, size not changed", "ino", ino, "error", terr)
			return int32Resp(errnoFor(terr, -5)), nil
		}
	}

	now := time.Now()
	if valid&fattrSize != 0 {
		rec.Size = newSize
	}
	if valid&fattrMode != 0 {
		// The kernel sends a whole st_mode, but chmod may not change what kind
		// of file this is.  Keeping the stored type bits is what stops a chmod
		// on a symlink or a device node quietly turning it into something else.
		rec.Mode = (rec.Mode & metadata.S_IFMT) | (mode &^ metadata.S_IFMT)
	}
	if valid&fattrUID != 0 {
		rec.UID = uid
	}
	if valid&fattrGID != 0 {
		rec.GID = gid
	}
	if valid&fattrAtime != 0 {
		rec.Atime = time.Unix(int64(atime), 0)
	}
	if valid&fattrMtime != 0 {
		rec.Mtime = time.Unix(int64(mtime), 0)
	}
	if valid&fattrCtime != 0 {
		rec.Ctime = time.Unix(int64(ctime), 0)
	}
	if valid&fattrAtimeNow != 0 {
		rec.Atime = now
	}
	if valid&fattrMtimeNow != 0 {
		rec.Mtime = now
	}
	// Any attribute change is a status change.
	if valid&(fattrMode|fattrUID|fattrGID|fattrSize) != 0 && valid&fattrCtime == 0 {
		rec.Ctime = now
	}

	// Pinned to the revision the record was read at, so a concurrent update to
	// a different field is not silently overwritten by this one.
	ok, err := s.store.Txn(ctx,
		[]clientv3.Cmp{metadata.InodeUnchanged(ino, rev)},
		[]clientv3.Op{clientv3.OpPut(metadata.InodeKey(ino), string(metadata.EncodeInode(rec)))}, nil)
	if err != nil {
		return int32Resp(errnoFor(err, -5)), nil
	}
	if !ok {
		return int32Resp(-11), nil // EAGAIN: the inode moved, let the kernel retry
	}

	return attrResp(rec), nil
}

// SYMLINK payload: [u64:parent][u32:name_len][name][u32:target_len][target][u32:uid][u32:gid]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleSymlink(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name, target := r.u64(), r.str(), r.str()
	uid, gid := r.u32(), r.u32()
	if !r.ok {
		return int32Resp(-22), nil
	}

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(-28), nil
	}

	rec, err := s.store.AtomicCreateSymlink(ctx, parent, name, ino, target, uid, gid)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil
	}

	return entryResp(ino, rec), nil
}

// LINK payload: [u64:ino][u64:new_parent][u32:new_name_len][new_name]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleLink(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino, newParent, name := r.u64(), r.u64(), r.str()
	if !r.ok {
		return int32Resp(-22), nil
	}

	rec, err := s.store.AtomicLink(ctx, ino, newParent, name)
	if err != nil {
		return int32Resp(errnoFor(err, -17)), nil
	}
	return entryResp(ino, rec), nil
}

// MKNOD payload: [u64:parent][u32:name_len][name][u32:mode][u32:rdev][u32:umask][u32:uid][u32:gid]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleMknod(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	mode, rdev, umask := r.u32(), r.u32(), r.u32()
	uid, gid := r.u32(), r.u32()
	if !r.ok {
		return int32Resp(-22), nil
	}

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(-28), nil
	}

	rec, err := s.store.AtomicCreateNode(ctx, parent, name, ino, applyUmask(mode, umask), rdev, uid, gid)
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
