// Package metadata implements the etcd-backed metadata store.
//
// Every structural mutation (inode, dirent, lock, arena) goes through an
// etcd transaction. The etcd key schema is:
//
//	inode:<ino>                   → serialised InodeRecord
//	dirent:<parent_ino>/<name>    → <ino> (uint64, big-endian)
//	lock:<ino>                    → serialised LockRecord
//	arena:<node_id>               → serialised ArenaRecord
//	arena_alloc_log               → append-only allocation log key
//	membership:<node_id>          → lease-backed liveness key
//	gen:<node_id>                 → fencing generation counter
//	inode_alloc_counter           → next inode allocation block
//
// Keys are deliberately short to minimise B-tree memory pressure in etcd.
// The "/" separator in dirent keys is a naming convention, not a structural
// hierarchy — etcd stores keys in flat lexicographic order.
package metadata

import (
	"encoding/binary"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Key prefixes.
const (
	PrefixInode      = "inode:"
	PrefixDirent     = "dirent:"
	PrefixLock       = "lock:"
	PrefixArena      = "arena:"
	PrefixExtent     = "extent:"
	PrefixMembership = "membership:"
	PrefixGen        = "gen:"
	PrefixArenaLog   = "arena_alloc_log"

	KeyInodeAllocCounter = "inode_alloc_counter"
)

// Reserved inode numbers.
//
// RootIno matches FUSE_ROOT_ID from the kernel FUSE protocol: the C daemon
// answers getattr/lookup for it locally, and seed-etcd writes the root
// directory record to inode:1.  Inode 0 is never valid.  Allocators must
// therefore start handing out numbers at FirstUsableIno.
const (
	RootIno        uint64 = 1
	FirstUsableIno uint64 = 2
)

// InodeRecord is the serialised value stored at inode:<ino>.
type InodeRecord struct {
	Ino        uint64
	Size       uint64
	Blocks     uint64
	Mode       uint32
	Nlink      uint32
	UID        uint32
	GID        uint32
	Rdev       uint32
	Blksize    uint32
	Generation uint64
	Atime      time.Time
	Mtime      time.Time
	Ctime      time.Time

	// Extents are stored in separate keys (extent:<ino>/<chunk>)
	// to stay under the 1.5 MiB etcd value limit.
}

// LockRecord is the serialised value stored at lock:<ino>.
type LockRecord struct {
	Mode    string   `json:"mode"` // "shared" or "exclusive"
	Holders []string `json:"holders"`
}

// ArenaRecord is the serialised value stored at arena:<node_id>.
type ArenaRecord struct {
	NodeID    string
	DiskStart uint64
	DiskEnd   uint64
	FreeList  []uint64 // sorted offsets of free blocks within the arena
}

// MembershipRecord holds per-node liveness metadata.
type MembershipRecord struct {
	NodeID      string
	ClusterName string
	JoinedAt    time.Time
	Address     string
}

// Inode mode constants matching Linux stat.h
const (
	S_IFMT      = uint32(0170000) // type mask
	ModeDir     = uint32(0040000) // S_IFDIR
	ModeSymlink = uint32(0120000) // S_IFLNK
	ModeFile    = uint32(0100000) // S_IFREG

	DirentTypeDir     = 4  // DT_DIR
	DirentTypeFile    = 8  // DT_REG
	DirentTypeSymlink = 10 // DT_LNK
)

// Key helpers — build etcd keys from components.

func InodeKey(ino uint64) string {
	return fmt.Sprintf("%s%d", PrefixInode, ino)
}

func InodeSymlinkKey(ino uint64) string {
	return fmt.Sprintf("symlink:%d", ino)
}

func DirentKey(parent uint64, name string) string {
	return fmt.Sprintf("%s%d/%s", PrefixDirent, parent, name)
}

func DirentPrefix(parent uint64) string {
	return fmt.Sprintf("%s%d/", PrefixDirent, parent)
}

func LockKey(ino uint64) string {
	return fmt.Sprintf("%s%d", PrefixLock, ino)
}

func ArenaKey(nodeID string) string {
	return fmt.Sprintf("%s%s", PrefixArena, nodeID)
}

func MembershipKey(nodeID string) string {
	return fmt.Sprintf("%s%s", PrefixMembership, nodeID)
}

func GenKey(nodeID string) string {
	return fmt.Sprintf("%s%s", PrefixGen, nodeID)
}

// EncodeUint64 encodes a uint64 as a big-endian byte slice for use as an etcd value.
func EncodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// DecodeUint64 decodes a big-endian byte slice to a uint64.
func DecodeUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// Op helpers — convenient etcd operation constructors for transactions.

// PutInode attaches a value to an inode key.
func PutInode(ino uint64, value []byte) clientv3.Op {
	return clientv3.OpPut(InodeKey(ino), string(value))
}

// PutDirent creates a directory entry pointing to an inode.
func PutDirent(parent uint64, name string, ino uint64) clientv3.Op {
	return clientv3.OpPut(DirentKey(parent, name), string(EncodeUint64(ino)))
}

// DeleteDirent removes a directory entry.
func DeleteDirent(parent uint64, name string) clientv3.Op {
	return clientv3.OpDelete(DirentKey(parent, name))
}

// RangeDirent reads a single directory entry.
func RangeDirent(parent uint64, name string) clientv3.Op {
	return clientv3.OpGet(DirentKey(parent, name))
}

// RangeDirents scans all entries in a directory (prefix scan).
func RangeDirents(parent uint64) clientv3.Op {
	return clientv3.OpGet(DirentPrefix(parent), clientv3.WithPrefix())
}

// DeleteDirentPrefix removes all entries under a directory (used by rm -rf).
func DeleteDirentPrefix(parent uint64) clientv3.Op {
	return clientv3.OpDelete(DirentPrefix(parent), clientv3.WithPrefix())
}

// PutMembership creates a lease-bound membership key.
func PutMembership(nodeID string, value []byte, leaseID clientv3.LeaseID) clientv3.Op {
	return clientv3.OpPut(MembershipKey(nodeID), string(value), clientv3.WithLease(leaseID))
}
