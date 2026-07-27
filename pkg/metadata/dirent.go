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
		Nlink:   1, // regular file starts with 1 link
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
		// Determine which condition failed
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
		Nlink:   2,                    // . and ..
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

// AtomicRename atomically renames a file within the same directory.
// old and new must be in the same parent directory.
// If the new name already exists, the transaction succeeds with RENAME_NOREPLACE check.
func (s *Store) AtomicRename(ctx context.Context, oldParent uint64, oldName string, newParent uint64, newName string, ino uint64, flags uint32) error {
	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(DirentKey(oldParent, oldName)), ">", 0), // old must exist
	}

	if flags&RenameNoReplace != 0 {
		cmps = append(cmps,
			clientv3.Compare(clientv3.CreateRevision(DirentKey(newParent, newName)), "=", 0)) // new must not exist
	}

	thenOps := []clientv3.Op{
		DeleteDirent(oldParent, oldName),
		PutDirent(newParent, newName, ino),
	}

	ok, err := s.Txn(ctx, cmps, thenOps, nil)
	if err != nil {
		return fmt.Errorf("atomic rename %d/%q → %d/%q: %w", oldParent, oldName, newParent, newName, err)
	}
	if !ok {
		return fmt.Errorf("atomic rename %d/%q → %d/%q: source missing or target exists", oldParent, oldName, newParent, newName)
	}
	return nil
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
