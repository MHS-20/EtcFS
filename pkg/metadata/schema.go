// Package metadata implements the etcd-backed metadata store.
//
// Every structural mutation (inode, dirent, lock, arena) goes through an
// etcd transaction. The etcd key schema is:
//
//	inode:<ino>                   → serialised InodeRecord
//	dirent:<parent_ino>/<name>    → <ino> (uint64, big-endian)
//	lock:<ino>/<mode>/<lease_id>  → holder's node ID, one key per holder
//	arena:<node_id>/<arena_id>    → <arena_id> (uint64, big-endian)
//	free_arena:<arena_id>         → arena returned to the global pool
//	extent:<ino>/<chunk>          → <log_off>,<disk_off>,<length>,<generation>,<sequence>
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
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Key prefixes.
const (
	PrefixInode      = "inode:"
	PrefixDirent     = "dirent:"
	PrefixXattr      = "xattr:"
	PrefixQuota      = "quota:"
	PrefixLock       = "lock:"
	PrefixArena      = "arena:"
	PrefixExtent     = "extent:"
	PrefixFreeArena  = "free_arena:"
	PrefixMembership = "membership:"
	PrefixGen        = "gen:"
	PrefixArenaLog   = "arena_alloc_log"

	// Fencing control plane — see fence.go.
	PrefixFencePending = "fence_pending:"
	PrefixFenceClaim   = "fence_claim:"
	PrefixFenceDone    = "fence_done:"

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

// LockRecord is the lock state of one inode, assembled from its holder keys.
type LockRecord struct {
	Mode    string   // "shared" or "exclusive"
	Holders []string // node IDs currently holding it
}

// MembershipRecord is the JSON value stored at membership:<node_id>.
//
// InstanceID is the cloud instance backing the node, recorded so the fencing
// controller can detach the shared volume from a node that has already expired
// and can no longer be asked.  It is empty off a cloud instance, and absent
// entirely from a record written by an older node — external fencing then
// degrades to a generation bump without a detach.
type MembershipRecord struct {
	NodeID     string    `json:"node_id"`
	Cluster    string    `json:"cluster"`
	JoinedAt   time.Time `json:"joined_at"`
	InstanceID string    `json:"instance_id"`
}

// Inode mode constants matching Linux stat.h
const (
	S_IFMT      = uint32(0170000) // type mask
	ModeDir     = uint32(0040000) // S_IFDIR
	ModeSymlink = uint32(0120000) // S_IFLNK
	ModeFile    = uint32(0100000) // S_IFREG

	S_ISUID = uint32(0004000)
	S_ISGID = uint32(0002000)
	S_IXGRP = uint32(0000010)

	DirentTypeDir     = 4  // DT_DIR
	DirentTypeFile    = 8  // DT_REG
	DirentTypeSymlink = 10 // DT_LNK
)

// ClearSetIDOnWrite returns mode with the set-user-ID and set-group-ID bits a
// write by uid must drop.
//
// Otherwise an unprivileged user who may write a setuid binary can change what
// it does while it keeps running as its owner. Root keeps the bits, the way a
// process holding CAP_FSETID does on a local filesystem: that is what lets an
// installer write a setuid binary without having to re-set the mode. The
// set-group-ID bit means mandatory locking rather than privilege on a file
// with no group-execute bit, so it is cleared only when that bit is set.
func ClearSetIDOnWrite(mode, uid uint32) uint32 {
	if uid == 0 {
		return mode
	}
	mode &^= S_ISUID
	if mode&S_IXGRP != 0 {
		mode &^= S_ISGID
	}
	return mode
}

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

// ParseDirentKey splits a dirent key back into its parent inode and name.
// Returns ok=false for a key that is not a well-formed dirent key.
//
// A name may contain any byte but '/' and NUL, and the parent is everything
// before the first '/', so the split is unambiguous even for a name that itself
// contains a slash-like sequence.
func ParseDirentKey(key string) (parent uint64, name string, ok bool) {
	rest, found := strings.CutPrefix(key, PrefixDirent)
	if !found {
		return 0, "", false
	}
	parentStr, name, found := strings.Cut(rest, "/")
	if !found {
		return 0, "", false
	}
	parent, err := strconv.ParseUint(parentStr, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return parent, name, true
}

// Extended attribute keys.
//
//	xattr:<ino>/<name>
//
// Deliberately the same shape as a dirent key, and for the same reason: one
// key per attribute makes a single attribute readable, writable and removable
// without rewriting the others, and makes "every attribute of this inode" a
// prefix scan.  A single packed value per inode would turn every setxattr into
// a read-modify-write of the whole set, which is both slower and a lost-update
// race between two nodes setting different attributes at once.
//
// An attribute name may contain any byte but '/' and NUL — the same constraint
// a dirent name carries — so the split below is unambiguous.
func XattrKey(ino uint64, name string) string {
	return fmt.Sprintf("%s%d/%s", PrefixXattr, ino, name)
}

// XattrPrefix covers every attribute of one inode.
func XattrPrefix(ino uint64) string {
	return fmt.Sprintf("%s%d/", PrefixXattr, ino)
}

// ParseXattrKey splits an xattr key back into its inode and attribute name.
func ParseXattrKey(key string) (ino uint64, name string, ok bool) {
	rest, found := strings.CutPrefix(key, PrefixXattr)
	if !found {
		return 0, "", false
	}
	inoStr, name, found := strings.Cut(rest, "/")
	if !found {
		return 0, "", false
	}
	ino, err := strconv.ParseUint(inoStr, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return ino, name, true
}

// QuotaKey names the limits attached to a directory that is a quota root.
func QuotaKey(ino uint64) string {
	return fmt.Sprintf("%s%d", PrefixQuota, ino)
}

// ParseQuotaKey returns the inode a quota key belongs to.
func ParseQuotaKey(key string) (ino uint64, ok bool) {
	rest, found := strings.CutPrefix(key, PrefixQuota)
	if !found {
		return 0, false
	}
	ino, err := strconv.ParseUint(rest, 10, 64)
	if err != nil {
		return 0, false
	}
	return ino, true
}

// Lock keys.
//
// One key per holder, with the mode in the key rather than the value:
//
//	lock:<ino>/<mode>/<holder>
//
// A shared lock has many holders at once, so a single key cannot represent it —
// one key each is what lets a holder release without dropping the lock for the
// rest.  The holder token is the session lease that backs the key paired with a
// counter, so two holders on the same node stay distinct even though they share
// one lease.  Putting the mode in the key lets a transaction ask "is any writer
// holding this?" as a range comparison, with no value to parse.
func LockKey(ino uint64, mode LockMode, holder string) string {
	return fmt.Sprintf("%s%d/%s/%s", PrefixLock, ino, mode, holder)
}

// LockPrefix covers every holder of an inode's lock, in any mode.
func LockPrefix(ino uint64) string {
	return fmt.Sprintf("%s%d/", PrefixLock, ino)
}

// LockModePrefix covers every holder of one mode.
func LockModePrefix(ino uint64, mode LockMode) string {
	return fmt.Sprintf("%s%d/%s/", PrefixLock, ino, mode)
}

// ParseLockKey returns the mode a lock key was taken in.
func ParseLockKey(key string, ino uint64) (mode LockMode, ok bool) {
	rest, found := strings.CutPrefix(key, LockPrefix(ino))
	if !found {
		return "", false
	}
	modeStr, _, found := strings.Cut(rest, "/")
	if !found {
		return "", false
	}
	switch LockMode(modeStr) {
	case LockShared, LockExclusive:
		return LockMode(modeStr), true
	}
	return "", false
}

// ArenaOwnerKey names the record proving nodeID owns arenaID.
//
// One key per arena, not one per node: a node acquires a further arena
// whenever its current ones cannot satisfy a write, and a single
// arena:<node_id> record would be overwritten by that second acquisition,
// leaving the first arena owned by nobody — never re-adopted on restart and
// never returned to the free pool.
func ArenaOwnerKey(nodeID string, arenaID uint64) string {
	return fmt.Sprintf("%s%s/%d", PrefixArena, nodeID, arenaID)
}

// ArenaNodePrefix scans every arena owned by one node.
func ArenaNodePrefix(nodeID string) string {
	return fmt.Sprintf("%s%s/", PrefixArena, nodeID)
}

// ParseArenaKey splits an ownership key back into its node and arena.
//
// Split on the last "/" so a node ID containing one still parses; ok is false
// for any key that is not an ownership record.
func ParseArenaKey(key string) (nodeID string, arenaID uint64, ok bool) {
	rest, found := strings.CutPrefix(key, PrefixArena)
	if !found {
		return "", 0, false
	}
	slash := strings.LastIndex(rest, "/")
	if slash < 1 {
		return "", 0, false
	}
	id, err := strconv.ParseUint(rest[slash+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return rest[:slash], id, true
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
