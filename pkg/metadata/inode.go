package metadata

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Inode operations: create, get, update, delete.
//
// Every inode mutation is conditioned on the writer's fencing generation
// being current.  A stale generation (self-fenced or externally fenced)
// causes the transaction to fail.

// InitialNlink is the link count an inode of the given mode starts with: two
// for a directory, which is referred to both by its own "." and by its entry in
// its parent, and one for everything else.
//
// The caller creating the directory entry is what makes that count true, so a
// record stored without one is inconsistent from the moment it is written.
func InitialNlink(mode uint32) uint32 {
	if mode&S_IFMT == ModeDir {
		return 2
	}
	return 1
}

// NewInodeRecord builds the record a freshly created inode of this mode starts
// life with.  It is not written anywhere: the caller commits it alongside the
// directory entry that makes its link count true.
func NewInodeRecord(ino uint64, mode, uid, gid uint32) *InodeRecord {
	now := timeNow()
	return &InodeRecord{
		Ino:     ino,
		Mode:    mode,
		Nlink:   InitialNlink(mode),
		UID:     uid,
		GID:     gid,
		Blksize: 4096,
		Atime:   now,
		Mtime:   now,
		Ctime:   now,
	}
}

// CreateInode creates a new inode record in etcd.
// The caller must have already reserved an inode number.
// Returns the inode record on success.
func (s *Store) CreateInode(ctx context.Context, ino uint64, mode uint32, uid, gid uint32) (*InodeRecord, error) {
	rec := NewInodeRecord(ino, mode, uid, gid)
	value := EncodeInode(rec)
	op := clientv3.OpPut(InodeKey(ino), string(value))

	// Condition: inode key must not exist (CreateRevision == 0)
	cmp := clientv3.Compare(clientv3.CreateRevision(InodeKey(ino)), "=", 0)

	ok, err := s.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{op}, nil)
	if err != nil {
		return nil, fmt.Errorf("create inode %d: %w", ino, err)
	}
	if !ok {
		return nil, fmt.Errorf("create inode %d: already exists", ino)
	}

	return rec, nil
}

// GetInode retrieves an inode record from etcd.
// Returns nil if the inode does not exist.
func (s *Store) GetInode(ctx context.Context, ino uint64) (*InodeRecord, error) {
	value, err := s.Get(ctx, InodeKey(ino))
	if err != nil {
		return nil, fmt.Errorf("get inode %d: %w", ino, err)
	}
	if value == nil {
		return nil, nil
	}
	return DecodeInode(value), nil
}

// GetInodeRev retrieves an inode record together with the revision it was last
// written at, which a later transaction compares against to prove the record
// has not changed in between.  Returns a nil record if the inode is gone.
func (s *Store) GetInodeRev(ctx context.Context, ino uint64) (*InodeRecord, int64, error) {
	kvs, _, err := s.GetRevision(ctx, InodeKey(ino))
	if err != nil {
		return nil, 0, fmt.Errorf("get inode %d: %w", ino, err)
	}
	if len(kvs) == 0 {
		return nil, 0, nil
	}
	return DecodeInode(kvs[0].Value), kvs[0].ModRevision, nil
}

// InodeUnchanged compares an inode against the revision it was read at.
//
// Every read-modify-write of an inode record has to carry this. Without it a
// transaction only proves the inode exists, not that it still holds the value
// the new one was computed from, so two concurrent updates each read the same
// record and the second silently overwrites the first.
func InodeUnchanged(ino uint64, modRev int64) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(InodeKey(ino)), "=", modRev)
}

// casAttempts bounds a read-modify-write retry. Losing the comparison means
// something changed underneath, so the work is redone against fresh state; the
// bound keeps sustained contention from spinning forever.
const casAttempts = 20

// retryCAS runs attempt until it commits. attempt reports false when its
// comparisons did not hold, which is contention rather than failure.
func retryCAS(ctx context.Context, what string, attempt func() (bool, error)) error {
	for i := 0; i < casAttempts; i++ {
		ok, err := attempt()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if err := casBackoff(ctx, i); err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
	}
	return fmt.Errorf("%s: contended beyond %d attempts (%w)", what, casAttempts, ErrConflict)
}

// casBackoff pauses before redoing a contended attempt.
//
// The jitter is what makes the retry converge: callers that lost the same race
// started their retry at the same moment, so a fixed delay puts them back in
// lockstep to collide again on the next tick. Sixteen concurrent hard links to
// one inode exhausted the whole attempt budget that way.
//
// The wait honours ctx, so a caller whose deadline has already passed stops
// here instead of sitting out the full delay first.
func casBackoff(ctx context.Context, attempt int) error {
	const maxDelay = 200 * time.Millisecond
	delay := time.Duration(1<<attempt) * time.Millisecond
	if delay > maxDelay || delay <= 0 {
		delay = maxDelay
	}
	delay = delay/2 + time.Duration(rand.Int63n(int64(delay/2)+1))

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ---- serialisation ----

// EncodeInode serialises an InodeRecord to a binary format.
// Format: fixed-length fields in big-endian byte order, 72 bytes total.
func EncodeInode(rec *InodeRecord) []byte {
	buf := make([]byte, 72)
	pos := 0

	binary.BigEndian.PutUint64(buf[pos:], rec.Ino)
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:], rec.Size)
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:], rec.Blocks)
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:], rec.Mode)
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], rec.Nlink)
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], rec.UID)
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], rec.GID)
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], rec.Rdev)
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], rec.Blksize)
	pos += 4
	binary.BigEndian.PutUint64(buf[pos:], uint64(rec.Atime.Unix()))
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:], uint64(rec.Mtime.Unix()))
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:], uint64(rec.Ctime.Unix()))

	return buf
}

// DecodeInode deserialises binary data back to an InodeRecord.
func DecodeInode(data []byte) *InodeRecord {
	if len(data) < 72 {
		return nil
	}

	rec := &InodeRecord{}
	pos := 0

	rec.Ino = binary.BigEndian.Uint64(data[pos:])
	pos += 8
	rec.Size = binary.BigEndian.Uint64(data[pos:])
	pos += 8
	rec.Blocks = binary.BigEndian.Uint64(data[pos:])
	pos += 8
	rec.Mode = binary.BigEndian.Uint32(data[pos:])
	pos += 4
	rec.Nlink = binary.BigEndian.Uint32(data[pos:])
	pos += 4
	rec.UID = binary.BigEndian.Uint32(data[pos:])
	pos += 4
	rec.GID = binary.BigEndian.Uint32(data[pos:])
	pos += 4
	rec.Rdev = binary.BigEndian.Uint32(data[pos:])
	pos += 4
	rec.Blksize = binary.BigEndian.Uint32(data[pos:])
	pos += 4
	rec.Atime = time.Unix(int64(binary.BigEndian.Uint64(data[pos:])), 0)
	pos += 8
	rec.Mtime = time.Unix(int64(binary.BigEndian.Uint64(data[pos:])), 0)
	pos += 8
	rec.Ctime = time.Unix(int64(binary.BigEndian.Uint64(data[pos:])), 0)

	return rec
}
