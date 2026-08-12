package metadata

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Dirent operations: lookup, create, unlink, rename.
//
// Every namespace mutation is a single atomic etcd Txn.  No directory-level
// locking is used — concurrent creates in the same directory are independent
// transactions that each CAS on their specific dirent key.

// LookupDirent resolves a name in a directory to an inode number.
// Returns 0 if the entry does not exist.
func (s *Store) LookupDirent(ctx context.Context, parent uint64, name string) (uint64, error) {
	value, err := s.Get(ctx, DirentKey(parent, name))
	if err != nil {
		return 0, fmt.Errorf("lookup %d/%q: %w", parent, name, err)
	}
	if value == nil {
		return 0, nil
	}
	return DecodeUint64(value), nil
}

// CreateDirent atomically creates a directory entry pointing to an inode.
// The transaction fails if the entry already exists (CREATE exclusivity).
func (s *Store) CreateDirent(ctx context.Context, parent uint64, name string, ino uint64) error {
	cmp := clientv3.Compare(clientv3.CreateRevision(DirentKey(parent, name)), "=", 0)
	op := PutDirent(parent, name, ino)

	ok, err := s.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{op}, nil)
	if err != nil {
		return fmt.Errorf("create dirent %d/%q: %w", parent, name, err)
	}
	if !ok {
		return fmt.Errorf("create dirent %d/%q: already exists (%w)", parent, name, ErrExists)
	}
	return nil
}

// RemoveDirent atomically removes a directory entry.
// The transaction fails if the entry does not exist.
func (s *Store) RemoveDirent(ctx context.Context, parent uint64, name string) error {
	cmp := clientv3.Compare(clientv3.CreateRevision(DirentKey(parent, name)), ">", 0)
	del := DeleteDirent(parent, name)

	ok, err := s.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{del}, nil)
	if err != nil {
		return fmt.Errorf("remove dirent %d/%q: %w", parent, name, err)
	}
	if !ok {
		return fmt.Errorf("remove dirent %d/%q: not found", parent, name)
	}
	return nil
}

// ListDirents returns all entries in a directory as (name, ino) pairs.
func (s *Store) ListDirents(ctx context.Context, parent uint64) ([]DirentEntry, error) {
	kvs, err := s.GetPrefix(ctx, DirentPrefix(parent))
	if err != nil {
		return nil, fmt.Errorf("list dirents %d: %w", parent, err)
	}

	entries := make([]DirentEntry, 0, len(kvs))
	for _, kv := range kvs {
		entries = append(entries, DirentEntry{
			Name: extractNameFromKey(string(kv.Key), parent),
			Ino:  DecodeUint64(kv.Value),
		})
	}
	return entries, nil
}

// DirentEntry is a directory listing entry.
type DirentEntry struct {
	Name string
	Ino  uint64
}

// extractNameFromKey strips the dirent prefix from an etcd key to get the filename.
// Example: "dirent:42/foo" → "foo"
func extractNameFromKey(key string, parent uint64) string {
	prefix := fmt.Sprintf("%s%d/", PrefixDirent, parent)
	if len(key) > len(prefix) {
		return key[len(prefix):]
	}
	return key
}

// atomicCreate publishes a new inode and the name that refers to it in one
// transaction, together with whatever else the file type needs (a symlink's
// target, say).  Every creating operation goes through here: an inode written
// without its dirent is unreachable and invisible to the orphan checks, which
// look for extents without inodes, not inodes without names.
//
// The transaction asserts both that the name is free and that the inode number
// is unused, so a create that loses either race writes nothing at all.
func (s *Store) atomicCreate(ctx context.Context, parent uint64, name string, rec *InodeRecord, extra ...clientv3.Op) error {
	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(DirentKey(parent, name)), "=", 0),
		clientv3.Compare(clientv3.CreateRevision(InodeKey(rec.Ino)), "=", 0),
	}
	ops := append([]clientv3.Op{
		PutDirent(parent, name, rec.Ino),
		clientv3.OpPut(InodeKey(rec.Ino), string(EncodeInode(rec))),
	}, extra...)

	ok, err := s.Txn(ctx, cmps, ops, nil)
	if err != nil {
		return fmt.Errorf("atomic create %d/%q: %w", parent, name, err)
	}
	if !ok {
		return fmt.Errorf("atomic create %d/%q: %w", parent, name, ErrExists)
	}
	return nil
}

// AtomicCreateFile creates a regular file and its directory entry in a single
// etcd transaction.
func (s *Store) AtomicCreateFile(ctx context.Context, parent uint64, name string, ino uint64, mode uint32, uid, gid uint32) (*InodeRecord, error) {
	rec := NewInodeRecord(ino, mode, uid, gid)
	return rec, s.atomicCreate(ctx, parent, name, rec)
}

// AtomicCreateDir creates a directory (mkdir) in a single etcd transaction.
// Same pattern as AtomicCreateFile but with nlink=2 (. and ..) and S_IFDIR mode.
func (s *Store) AtomicCreateDir(ctx context.Context, parent uint64, name string, ino uint64, mode uint32, uid, gid uint32) (*InodeRecord, error) {
	rec := NewInodeRecord(ino, mode|ModeDir, uid, gid)
	rec.Size = 4096
	return rec, s.atomicCreate(ctx, parent, name, rec)
}

// AtomicCreateSymlink creates a symlink inode, its target record and its
// directory entry in a single etcd transaction.
func (s *Store) AtomicCreateSymlink(ctx context.Context, parent uint64, name string, ino uint64, target string, uid, gid uint32) (*InodeRecord, error) {
	// A symlink's own permission bits are not meaningful — the target's are what
	// an access check consults — so the mode is fixed rather than umask-masked.
	rec := NewInodeRecord(ino, ModeSymlink|0777, uid, gid)
	rec.Size = uint64(len(target))
	return rec, s.atomicCreate(ctx, parent, name, rec, clientv3.OpPut(InodeSymlinkKey(ino), target))
}

// AtomicCreateNode creates a device node, FIFO or socket (mknod) and its
// directory entry in a single etcd transaction.
func (s *Store) AtomicCreateNode(ctx context.Context, parent uint64, name string, ino uint64, mode, rdev uint32, uid, gid uint32) (*InodeRecord, error) {
	rec := NewInodeRecord(ino, mode, uid, gid)
	rec.Rdev = rdev
	return rec, s.atomicCreate(ctx, parent, name, rec)
}

// AtomicLink adds a second name for an existing inode: the new dirent and the
// raised link count commit together, so a name that loses the race to another
// creator does not leave the count permanently inflated.
//
// The inode is pinned to the revision its link count was read at, for the same
// reason every other read-modify-write of an inode record is.
func (s *Store) AtomicLink(ctx context.Context, ino, parent uint64, name string) (*InodeRecord, error) {
	var linked *InodeRecord
	err := retryCAS(ctx, fmt.Sprintf("atomic link %d/%q", parent, name), func() (bool, error) {
		rec, rev, err := s.GetInodeRev(ctx, ino)
		if err != nil {
			return false, fmt.Errorf("atomic link %d/%q: %w", parent, name, err)
		}
		if rec == nil {
			return false, fmt.Errorf("atomic link %d/%q: inode %d %w", parent, name, ino, ErrNotFound)
		}
		// Hard links to a directory would let the namespace form a cycle that no
		// unlink can break, and POSIX reserves the right to refuse them.
		if rec.Mode&S_IFMT == ModeDir {
			return false, fmt.Errorf("atomic link %d/%q: %w", parent, name, ErrPerm)
		}
		rec.Nlink++

		direntKey := DirentKey(parent, name)
		cmps := []clientv3.Cmp{
			clientv3.Compare(clientv3.CreateRevision(direntKey), "=", 0),
			InodeUnchanged(ino, rev),
		}
		ops := []clientv3.Op{
			PutDirent(parent, name, ino),
			clientv3.OpPut(InodeKey(ino), string(EncodeInode(rec))),
		}
		ok, err := s.Txn(ctx, cmps, ops, nil)
		if err != nil {
			return false, fmt.Errorf("atomic link %d/%q: %w", parent, name, err)
		}
		if ok {
			linked = rec
			return true, nil
		}
		// The inode moving is contention worth retrying; the name already
		// existing is the caller's answer.
		if existing, lerr := s.LookupDirent(ctx, parent, name); lerr == nil && existing != 0 {
			return false, fmt.Errorf("atomic link %d/%q: %w", parent, name, ErrExists)
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return linked, nil
}

// AtomicUnlink removes a directory entry and drops one reference to the inode
// it named, deleting the inode once nothing points at it.
//
// The transaction is pinned to both records as they were read: the dirent still
// naming this inode, and the inode still holding the link count the new one was
// computed from. Without the second, two concurrent unlinks of two names for
// one inode each read nlink=2, each write nlink=1, and the inode is never
// freed. Losing either comparison is contention, so the work is redone against
// fresh state rather than reported as a failure.
func (s *Store) AtomicUnlink(ctx context.Context, parent uint64, name string) error {
	return retryCAS(ctx, fmt.Sprintf("atomic unlink %d/%q", parent, name), func() (bool, error) {
		fail := func(format string, args ...any) (bool, error) {
			prefix := fmt.Sprintf("atomic unlink %d/%q: ", parent, name)
			return false, fmt.Errorf(prefix+format, args...)
		}

		direntKey := DirentKey(parent, name)
		direntKvs, _, err := s.GetRevision(ctx, direntKey)
		if err != nil {
			return fail("%w", err)
		}
		if len(direntKvs) == 0 {
			return fail("%w", ErrNotFound)
		}
		ino := DecodeUint64(direntKvs[0].Value)

		rec, rev, err := s.GetInodeRev(ctx, ino)
		if err != nil {
			return fail("%w", err)
		}
		if rec == nil {
			return fail("inode %d not found (%w)", ino, ErrNotFound)
		}

		cmps := []clientv3.Cmp{
			clientv3.Compare(clientv3.ModRevision(direntKey), "=", direntKvs[0].ModRevision),
			InodeUnchanged(ino, rev),
		}
		ops := append([]clientv3.Op{DeleteDirent(parent, name)}, s.unlinkInodeOps(rec)...)
		return s.Txn(ctx, cmps, ops, nil)
	})
}

// AtomicRmdir removes an empty directory and its entry in one transaction.
//
// Emptiness is asserted inside the transaction rather than read beforehand.
// etcd cannot express "no keys under this prefix" as a value comparison, but it
// can compare a whole range: "every key under dirent:<ino>/ has creation
// revision 0" is true only when the range is empty, since a key that exists has
// a non-zero one.  A separate listing followed by a delete would let another
// node create an entry in between and strand the subtree — the parent's name
// gone, the children reachable by nothing.
func (s *Store) AtomicRmdir(ctx context.Context, parent uint64, name string) error {
	return retryCAS(ctx, fmt.Sprintf("atomic rmdir %d/%q", parent, name), func() (bool, error) {
		fail := func(format string, args ...any) (bool, error) {
			prefix := fmt.Sprintf("atomic rmdir %d/%q: ", parent, name)
			return false, fmt.Errorf(prefix+format, args...)
		}

		direntKey := DirentKey(parent, name)
		direntKvs, _, err := s.GetRevision(ctx, direntKey)
		if err != nil {
			return fail("%w", err)
		}
		if len(direntKvs) == 0 {
			return fail("%w", ErrNotFound)
		}
		ino := DecodeUint64(direntKvs[0].Value)

		rec, rev, err := s.GetInodeRev(ctx, ino)
		if err != nil {
			return fail("%w", err)
		}
		if rec == nil {
			return fail("inode %d not found (%w)", ino, ErrNotFound)
		}
		if rec.Mode&S_IFMT != ModeDir {
			return fail("%w", ErrNotDir)
		}

		cmps := []clientv3.Cmp{
			clientv3.Compare(clientv3.ModRevision(direntKey), "=", direntKvs[0].ModRevision),
			InodeUnchanged(ino, rev),
			DirEmpty(ino),
		}
		ops := []clientv3.Op{
			DeleteDirent(parent, name),
			clientv3.OpDelete(InodeKey(ino)),
		}
		ok, err := s.Txn(ctx, cmps, ops, nil)
		if err != nil {
			return fail("%w", err)
		}
		if ok {
			return true, nil
		}
		// Losing a revision comparison is contention worth retrying; an entry
		// having appeared in the directory is the caller's answer.
		entries, lerr := s.ListDirents(ctx, ino)
		if lerr == nil && len(entries) > 0 {
			return fail("%w", ErrNotEmpty)
		}
		return false, nil
	})
}

// DirEmpty holds only when a directory has no entries.  A range comparison is
// the only way to ask etcd this: every key under the prefix must have creation
// revision 0, which no existing key does, so it is true exactly when the range
// is empty.
func DirEmpty(ino uint64) clientv3.Cmp {
	return clientv3.Compare(clientv3.CreateRevision(DirentPrefix(ino)), "=", 0).WithPrefix()
}

// AtomicRename moves ino from oldParent/oldName to newParent/newName in a
// single etcd transaction.
//
// POSIX requires the rename to replace an existing target, and replacing it is
// an unlink: the target's nlink drops, and its inode record goes with it at
// zero.  Leaving that out is what orphaned the replaced file — its inode and
// every extent behind it stayed in etcd, reachable by nothing.
//
// flags carries RENAME_NOREPLACE and RENAME_EXCHANGE.  Exchange is rejected
// rather than approximated: an ordinary rename deletes the source and
// overwrites the target, which is data loss for a caller that asked for the two
// names to swap.
func (s *Store) AtomicRename(ctx context.Context, oldParent uint64, oldName string, newParent uint64, newName string, ino uint64, flags uint32) error {
	fail := func(format string, args ...any) error {
		prefix := fmt.Sprintf("atomic rename %d/%q → %d/%q: ", oldParent, oldName, newParent, newName)
		return fmt.Errorf(prefix+format, args...)
	}

	if flags&RenameExchange != 0 {
		return fail("RENAME_EXCHANGE is not implemented (%w)", ErrInvalid)
	}

	// A dirent whose inode is missing is already corrupt, and fsck reports it.
	// Renaming it is still allowed: moving a broken pointer breaks nothing
	// further, and refusing would take away the obvious way to clear it.  Its
	// type is unknown, so the directory checks below simply do not apply.
	src, err := s.GetInode(ctx, ino)
	if err != nil {
		return fail("%w", err)
	}
	srcIsDir := src != nil && src.Mode&S_IFMT == ModeDir

	// A directory moved beneath itself detaches its whole subtree: the entries
	// still exist, but no path from the root reaches them, and no later
	// operation can tell that from ordinary data.
	if srcIsDir {
		under, aerr := s.isDescendant(ctx, newParent, ino)
		if aerr != nil {
			return fail("%w", aerr)
		}
		if newParent == ino || under {
			return fail("destination is inside the directory being moved (%w)", ErrInvalid)
		}
	}

	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(DirentKey(oldParent, oldName)), ">", 0),
	}
	ops := []clientv3.Op{
		DeleteDirent(oldParent, oldName),
		PutDirent(newParent, newName, ino),
	}

	targetKey := DirentKey(newParent, newName)
	targetKvs, _, err := s.GetRevision(ctx, targetKey)
	if err != nil {
		return fail("read target: %w", err)
	}

	if len(targetKvs) == 0 {
		// Nothing there now, and nothing may appear before the commit lands.
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(targetKey), "=", 0))
		if err := s.commitRename(ctx, cmps, ops); err != nil {
			return fail("%w", err)
		}
		return nil
	}

	if flags&RenameNoReplace != 0 {
		return fail("target exists (%w)", ErrExists)
	}

	// Pin the target to the revision the checks below are about to read, so a
	// concurrent write to that name aborts this rename instead of being
	// silently replaced by it.
	cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(targetKey), "=", targetKvs[0].ModRevision))

	victimIno := DecodeUint64(targetKvs[0].Value)
	victim, victimRev, err := s.GetInodeRev(ctx, victimIno)
	if err != nil {
		return fail("read target inode: %w", err)
	}
	if victim != nil {
		victimIsDir := victim.Mode&S_IFMT == ModeDir
		switch {
		case srcIsDir && !victimIsDir:
			return fail("cannot replace a file with a directory (%w)", ErrNotDir)
		case !srcIsDir && victimIsDir:
			return fail("cannot replace a directory with a file (%w)", ErrIsDir)
		}
		if victimIsDir {
			entries, lerr := s.ListDirents(ctx, victimIno)
			if lerr != nil {
				return fail("list target: %w", lerr)
			}
			if len(entries) > 0 {
				return fail("target directory is not empty (%w)", ErrNotEmpty)
			}
			// The listing above is what produces ENOTEMPTY; this is what makes
			// it safe, by refusing the rename if an entry appears in between.
			cmps = append(cmps, DirEmpty(victimIno))
		}
		// Pinned like the dirent above: the link count written below is
		// computed from this record, so a concurrent change to it has to abort
		// the rename rather than be overwritten by it.
		cmps = append(cmps, InodeUnchanged(victimIno, victimRev))
		ops = append(ops, s.unlinkInodeOps(victim)...)
	}

	if err := s.commitRename(ctx, cmps, ops); err != nil {
		return fail("%w", err)
	}
	return nil
}

// unlinkInodeOp is the write that drops one reference to rec: a smaller nlink,
// or the inode's removal once nothing points at it any more.
//
// The extents of a removed inode are deliberately left behind.  They become
// orphans, which the scrubber reclaims on the node that owns their arena — the
// only node that may, since its in-memory free list is rebuilt from exactly
// those records.
func (s *Store) unlinkInodeOps(rec *InodeRecord) []clientv3.Op {
	// A directory carries a fixed count of 2 for its whole life, so the count
	// says nothing about whether it is still referenced: losing its one name
	// removes it outright.  Decrementing instead would leave the record behind
	// with a count no path can ever bring back to zero.
	if rec.Mode&S_IFMT == ModeDir {
		return []clientv3.Op{
			clientv3.OpDelete(InodeKey(rec.Ino)),
			clientv3.OpDelete(XattrPrefix(rec.Ino), clientv3.WithPrefix()),
		}
	}
	// A surviving hard link keeps the inode, and the attributes belong to the
	// inode rather than to the name being removed, so they stay.
	if rec.Nlink > 1 {
		reduced := *rec
		reduced.Nlink--
		return []clientv3.Op{clientv3.OpPut(InodeKey(rec.Ino), string(EncodeInode(&reduced)))}
	}
	ops := []clientv3.Op{
		clientv3.OpDelete(InodeKey(rec.Ino)),
		// Extended attributes are owned outright by the inode, so they go with
		// it.  Left behind they would leak a key per attribute, and — worse —
		// a later inode reusing this number would inherit them.
		clientv3.OpDelete(XattrPrefix(rec.Ino), clientv3.WithPrefix()),
	}
	// A symlink's target lives in its own key, which nothing else references —
	// leaving it behind leaks a key per deleted symlink, and no check looks for
	// one.
	if rec.Mode&S_IFMT == ModeSymlink {
		ops = append(ops, clientv3.OpDelete(InodeSymlinkKey(rec.Ino)))
	}
	return ops
}

func (s *Store) commitRename(ctx context.Context, cmps []clientv3.Cmp, ops []clientv3.Op) error {
	ok, err := s.Txn(ctx, cmps, ops, nil)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("source or target changed under the rename (%w)", ErrConflict)
	}
	return nil
}

// isDescendant reports whether ino is anywhere beneath ancestor in the
// namespace.
//
// Inodes record no parent, so the only way up the tree is a reverse index built
// from the dirent values.  One prefix scan is enough, and this runs only when a
// *directory* is being renamed, which is rare — a per-inode parent pointer
// would be a second source of truth to keep consistent for no benefit at this
// scale.
//
// The walk is bounded by the number of directories it has seen: a namespace
// that already contains a cycle would otherwise spin here forever.
func (s *Store) isDescendant(ctx context.Context, ino, ancestor uint64) (bool, error) {
	if ino == ancestor {
		return true, nil
	}

	kvs, err := s.GetPrefix(ctx, PrefixDirent)
	if err != nil {
		return false, fmt.Errorf("build parent index: %w", err)
	}
	parent := make(map[uint64]uint64, len(kvs))
	for _, kv := range kvs {
		p, _, ok := ParseDirentKey(string(kv.Key))
		if !ok {
			continue
		}
		parent[DecodeUint64(kv.Value)] = p
	}

	for step := 0; step <= len(parent); step++ {
		next, ok := parent[ino]
		if !ok {
			return false, nil // reached the root, or an unparented inode
		}
		if next == ancestor {
			return true, nil
		}
		ino = next
	}
	return false, fmt.Errorf("parent chain from %d does not terminate", ino)
}

// ---- rename constants ----
const (
	RenameNoReplace = 1 << 0
	RenameExchange  = 1 << 1
)

// ---- helpers ----

// timeNow is a variable so a test can pin the clock.
var timeNow = time.Now
