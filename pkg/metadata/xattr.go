package metadata

import (
	"context"
	"fmt"
	"sort"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Extended attribute limits, matching the kernel's own.
//
// They are enforced here rather than only in the FUSE layer because etcd is
// the thing being protected: an attribute is a value in the metadata store,
// replicated through Raft to every member, and an unbounded one is a way to
// push the cluster into the quota that docs/TODO.md item 41 already had to
// defend.  The kernel applies the same two numbers, so nothing legitimate is
// refused by them.
const (
	MaxXattrNameLen  = 255
	MaxXattrValueLen = 65536
)

// setxattr flags, matching the kernel's XATTR_CREATE and XATTR_REPLACE.
const (
	XattrCreate  = 1
	XattrReplace = 2
)

// validXattrName rejects names the key schema cannot represent unambiguously,
// and names outside the kernel's length bound.
//
// A '/' would make ParseXattrKey split in the wrong place, and a NUL would
// terminate the name early in the C daemon's listxattr buffer, which is
// NUL-separated — either one lets a caller address an attribute that is not
// the one it named.
func validXattrName(name string) bool {
	if name == "" || len(name) > MaxXattrNameLen {
		return false
	}
	return !strings.ContainsAny(name, "/\x00")
}

// GetXattr reads one extended attribute. Returns ErrNoData if the inode has no
// attribute by that name.
func (s *Store) GetXattr(ctx context.Context, ino uint64, name string) ([]byte, error) {
	if !validXattrName(name) {
		return nil, ErrInvalid
	}
	value, err := s.Get(ctx, XattrKey(ino, name))
	if err != nil {
		return nil, fmt.Errorf("get xattr %s: %w", name, err)
	}
	if value == nil {
		return nil, ErrNoData
	}
	return value, nil
}

// SetXattr writes one extended attribute.
//
// flags carries the kernel's XattrCreate and XattrReplace, which are the whole
// reason this is a transaction rather than a Put: both are defined against
// whether the attribute already exists, so the check and the write have to be
// one atomic step or two nodes racing on XattrCreate could both believe they
// created it.
func (s *Store) SetXattr(ctx context.Context, ino uint64, name string, value []byte, flags uint32) error {
	if !validXattrName(name) {
		return ErrInvalid
	}
	if len(value) > MaxXattrValueLen {
		return ErrTooBig
	}

	key := XattrKey(ino, name)
	put := clientv3.OpPut(key, string(value))

	// The inode must exist, and must still exist when the write lands:
	// otherwise a setxattr racing an unlink leaves an attribute behind whose
	// inode is gone, which the range delete in unlinkInodeOps has already run
	// past and will never revisit.
	exists := clientv3.Compare(clientv3.CreateRevision(InodeKey(ino)), ">", 0)

	var cmps []clientv3.Cmp
	switch {
	case flags&XattrCreate != 0:
		cmps = []clientv3.Cmp{exists, clientv3.Compare(clientv3.CreateRevision(key), "=", 0)}
	case flags&XattrReplace != 0:
		cmps = []clientv3.Cmp{exists, clientv3.Compare(clientv3.CreateRevision(key), ">", 0)}
	default:
		cmps = []clientv3.Cmp{exists}
	}

	ok, err := s.Txn(ctx, cmps, []clientv3.Op{put}, nil)
	if err != nil {
		return fmt.Errorf("set xattr %s: %w", name, err)
	}
	if ok {
		return nil
	}

	// The transaction reports only that some comparison failed, so which error
	// the caller deserves has to be established separately. Distinguishing a
	// missing inode from a flag violation matters: ENOENT and EEXIST send a
	// caller down completely different paths.
	inode, err := s.Get(ctx, InodeKey(ino))
	if err != nil {
		return fmt.Errorf("set xattr %s: %w", name, err)
	}
	if inode == nil {
		return ErrNotFound
	}
	if flags&XattrCreate != 0 {
		return ErrExists
	}
	return ErrNoData
}

// RemoveXattr deletes one extended attribute. Returns ErrNoData if the inode
// has no attribute by that name, which is what removexattr(2) requires.
func (s *Store) RemoveXattr(ctx context.Context, ino uint64, name string) error {
	if !validXattrName(name) {
		return ErrInvalid
	}
	key := XattrKey(ino, name)
	ok, err := s.Txn(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), ">", 0)},
		[]clientv3.Op{clientv3.OpDelete(key)}, nil)
	if err != nil {
		return fmt.Errorf("remove xattr %s: %w", name, err)
	}
	if !ok {
		return ErrNoData
	}
	return nil
}

// ListXattrs returns the names of every extended attribute on an inode, sorted
// so that two calls with no intervening change agree.
func (s *Store) ListXattrs(ctx context.Context, ino uint64) ([]string, error) {
	kvs, err := s.GetPrefix(ctx, XattrPrefix(ino))
	if err != nil {
		return nil, fmt.Errorf("list xattrs: %w", err)
	}
	names := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		if _, name, ok := ParseXattrKey(string(kv.Key)); ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}
