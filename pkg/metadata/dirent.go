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

// ListDirentsPaginated returns entries with cursor-based pagination.
// limit is the max entries per page.  cursor is the name to start after
// (empty = start from beginning).  Returns the next cursor for the next page.
func (s *Store) ListDirentsPaginated(ctx context.Context, parent uint64, cursor string, limit int64) ([]DirentEntry, string, int64, error) {
	prefix := DirentPrefix(parent)
	rangeEnd := DirentPrefix(parent + 1)

	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithRange(rangeEnd))
	opts = append(opts, clientv3.WithLimit(limit))
	opts = append(opts, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))

	if cursor != "" {
		opts = append(opts, clientv3.WithFromKey())
		prefix = DirentKey(parent, cursor) + "\x00"
	}

	kvs, revision, err := s.GetRevision(ctx, prefix, opts...)
	if err != nil {
		return nil, "", 0, fmt.Errorf("list dirents %d paginated: %w", parent, err)
	}

	entries := make([]DirentEntry, 0, len(kvs))
	var lastKey string
	for _, kv := range kvs {
		name := extractNameFromKey(string(kv.Key), parent)
		lastKey = name
		entries = append(entries, DirentEntry{
			Name: name,
			Ino:  DecodeUint64(kv.Value),
		})
	}
	return entries, lastKey, revision, nil
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

// AtomicCreateFile creates a file in a single etcd transaction:
//  1. Check that the dirent key does not exist
//  2. Check that the inode key does not exist
//  3. Create the dirent
//  4. Create the inode
//
// This is the canonical create pattern from init_plan §4:
// "insert dirent if absent, insert inode if absent" in one transaction.
func (s *Store) AtomicCreateFile(ctx context.Context, parent uint64, name string, ino uint64, mode uint32, uid, gid uint32) (*InodeRecord, error) {
	now := timeNow()

	rec := &InodeRecord{
		Ino:     ino,
		Size:    0,
		Blocks:  0,
		Mode:    mode,
		Nlink:   InitialNlink(mode),
		UID:     uid,
		GID:     gid,
		Blksize: 4096,
		Atime:   now,
		Mtime:   now,
		Ctime:   now,
	}

	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(DirentKey(parent, name)), "=", 0), // entry doesn't exist
		clientv3.Compare(clientv3.CreateRevision(InodeKey(ino)), "=", 0),           // inode doesn't exist
	}

	ops := []clientv3.Op{
		PutDirent(parent, name, ino),
		clientv3.OpPut(InodeKey(ino), string(EncodeInode(rec))),
	}

	ok, err := s.Txn(ctx, cmps, ops, nil)
	if err != nil {
		return nil, fmt.Errorf("atomic create %d/%q: %w", parent, name, err)
	}
	if !ok {
		exists, _ := s.LookupDirent(ctx, parent, name)
		if exists != 0 {
			return nil, fmt.Errorf("atomic create %d/%q: %w", parent, name, ErrExists)
		}
		_, err := s.GetInode(ctx, ino)
		if err == nil {
			return nil, fmt.Errorf("atomic create %d/%q: inode %d already exists", parent, name, ino)
		}
		return nil, fmt.Errorf("atomic create %d/%q: transaction failed", parent, name)
	}

	return rec, nil
}

// AtomicCreateDir creates a directory (mkdir) in a single etcd transaction.
// Same pattern as AtomicCreateFile but with nlink=2 (. and ..) and S_IFDIR mode.
func (s *Store) AtomicCreateDir(ctx context.Context, parent uint64, name string, ino uint64, mode uint32, uid, gid uint32) (*InodeRecord, error) {
	now := timeNow()

	rec := &InodeRecord{
		Ino:     ino,
		Size:    4096,
		Blocks:  0,
		Mode:    mode | ModeDir,
		Nlink:   InitialNlink(mode | ModeDir),
		UID:     uid,
		GID:     gid,
		Blksize: 4096,
		Atime:   now,
		Mtime:   now,
		Ctime:   now,
	}

	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(DirentKey(parent, name)), "=", 0),
		clientv3.Compare(clientv3.CreateRevision(InodeKey(ino)), "=", 0),
	}

	ops := []clientv3.Op{
		PutDirent(parent, name, ino),
		clientv3.OpPut(InodeKey(ino), string(EncodeInode(rec))),
	}

	ok, err := s.Txn(ctx, cmps, ops, nil)
	if err != nil {
		return nil, fmt.Errorf("atomic mkdir %d/%q: %w", parent, name, err)
	}
	if !ok {
		return nil, fmt.Errorf("atomic mkdir %d/%q: %w", parent, name, ErrExists)
	}
	return rec, nil
}

// AtomicUnlink removes a directory entry and decrements the inode's nlink.
// If nlink reaches 0, the inode is also deleted.
// All in one atomic transaction.
func (s *Store) AtomicUnlink(ctx context.Context, parent uint64, name string) error {
	// First, find the inode
	ino, err := s.LookupDirent(ctx, parent, name)
	if err != nil {
		return fmt.Errorf("atomic unlink %d/%q: %w", parent, name, err)
	}
	if ino == 0 {
		return fmt.Errorf("atomic unlink %d/%q: not found", parent, name)
	}

	rec, err := s.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return fmt.Errorf("atomic unlink %d/%q: inode %d not found", parent, name, ino)
	}

	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(DirentKey(parent, name)), ">", 0),
	}

	thenOps := []clientv3.Op{
		DeleteDirent(parent, name),
	}

	rec.Nlink--
	if rec.Nlink > 0 {
		thenOps = append(thenOps, clientv3.OpPut(InodeKey(ino), string(EncodeInode(rec))))
	} else {
		thenOps = append(thenOps, clientv3.OpDelete(InodeKey(ino)))
	}

	ok, err := s.Txn(ctx, cmps, thenOps, nil)
	if err != nil {
		return fmt.Errorf("atomic unlink %d/%q: %w", parent, name, err)
	}
	if !ok {
		return fmt.Errorf("atomic unlink %d/%q: entry removed by concurrent operation", parent, name)
	}
	return nil
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
	victim, err := s.GetInode(ctx, victimIno)
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
		}
		ops = append(ops, s.unlinkInodeOp(victim))
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
func (s *Store) unlinkInodeOp(rec *InodeRecord) clientv3.Op {
	if rec.Nlink > 1 {
		reduced := *rec
		reduced.Nlink--
		return clientv3.OpPut(InodeKey(rec.Ino), string(EncodeInode(&reduced)))
	}
	return clientv3.OpDelete(InodeKey(rec.Ino))
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

// AtomicRmRf recursively deletes a directory tree by deleting the dirent prefix
// in a single etcd DeleteRange operation.
func (s *Store) AtomicRmRf(ctx context.Context, parent uint64) (int64, error) {
	deleted, err := s.DeletePrefix(ctx, DirentPrefix(parent))
	if err != nil {
		return 0, fmt.Errorf("rm -rf %d: %w", parent, err)
	}
	return deleted, nil
}

// ---- rename constants ----
const (
	RenameNoReplace = 1 << 0
	RenameExchange  = 1 << 1
)

// ---- helpers ----

var timeNow = time.Now

func init() { _ = timeNow } // ensure import used
