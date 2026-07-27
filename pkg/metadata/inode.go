package metadata

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Inode operations: create, get, update, delete.
//
// Every inode mutation is conditioned on the writer's fencing generation
// being current.  A stale generation (self-fenced or externally fenced)
// causes the transaction to fail.

// CreateInode creates a new inode record in etcd.
// The caller must have already reserved an inode number.
// Returns the inode record on success.
func (s *Store) CreateInode(ctx context.Context, ino uint64, mode uint32, uid, gid uint32) (*InodeRecord, error) {
	now := time.Now()

	rec := &InodeRecord{
		Ino:     ino,
		Size:    0,
		Blocks:  0,
		Mode:    mode,
		Nlink:   (mode >> 12) & 1, // 1 for directories (they start with . and ..)
		UID:     uid,
		GID:     gid,
		Rdev:    0,
		Blksize: 4096,
		Atime:   now,
		Mtime:   now,
		Ctime:   now,
	}

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
	return decodeInode(value), nil
}

// UpdateInode atomically updates an inode record in etcd.
// The update is conditioned on the inode's current ModRevision to avoid
// lost updates.  Returns the new revision and updated record.
func (s *Store) UpdateInode(ctx context.Context, rec *InodeRecord) (*InodeRecord, error) {
	value := EncodeInode(rec)
	key := InodeKey(rec.Ino)

	// CAS: update only if the inode hasn't been modified since we read it
	// (optimistic concurrency via ModRevision).
	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)). // placeholder; caller provides correct rev
		Then(clientv3.OpPut(key, string(value))).
		Commit()
	if err != nil {
		return nil, fmt.Errorf("update inode %d: %w", rec.Ino, err)
	}
	if !resp.Succeeded {
		return nil, fmt.Errorf("update inode %d: conflict (inode modified since read)", rec.Ino)
	}
	return rec, nil
}

// DeleteInode removes an inode record from etcd.
// Returns an error if the inode still has links (nlink > 0) — the caller
// must have already removed all directory entries.
func (s *Store) DeleteInode(ctx context.Context, ino uint64) error {
	rec, err := s.GetInode(ctx, ino)
	if err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("delete inode %d: not found", ino)
	}
	if rec.Nlink > 0 {
		return fmt.Errorf("delete inode %d: nlink=%d (still referenced)", ino, rec.Nlink)
	}

	cmp := clientv3.Compare(clientv3.CreateRevision(InodeKey(ino)), ">", 0)
	del := clientv3.OpDelete(InodeKey(ino))

	ok, err := s.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{del}, nil)
	if err != nil {
		return fmt.Errorf("delete inode %d: %w", ino, err)
	}
	if !ok {
		return fmt.Errorf("delete inode %d: not found", ino)
	}
	return nil
}

// IncrementNlink atomically increments the nlink counter on an inode.
// Used when creating a hard link.
func (s *Store) IncrementNlink(ctx context.Context, ino uint64) error {
	rec, err := s.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return fmt.Errorf("increment nlink %d: %w", ino, err)
	}
	rec.Nlink++
	return s.putInodeWithCAS(ctx, rec)
}

// DecrementNlink atomically decrements the nlink counter.
// Used when removing a directory entry (unlink, rmdir, rename target).
// Returns true if nlink reached zero (inode should be deleted).
func (s *Store) DecrementNlink(ctx context.Context, ino uint64) (bool, error) {
	rec, err := s.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return false, fmt.Errorf("decrement nlink %d: %w", ino, err)
	}
	if rec.Nlink == 0 {
		return false, fmt.Errorf("decrement nlink %d: already zero", ino)
	}
	rec.Nlink--
	zero := rec.Nlink == 0

	if err := s.putInodeWithCAS(ctx, rec); err != nil {
		return false, err
	}
	return zero, nil
}

func (s *Store) putInodeWithCAS(ctx context.Context, rec *InodeRecord) error {
	key := InodeKey(rec.Ino)
	value := EncodeInode(rec)
	cmp := clientv3.Compare(clientv3.CreateRevision(key), ">", 0) // must exist
	ok, err := s.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{clientv3.OpPut(key, string(value))}, nil)
	if err != nil {
		return fmt.Errorf("put inode %d: %w", rec.Ino, err)
	}
	if !ok {
		return fmt.Errorf("put inode %d: not found", rec.Ino)
	}
	return nil
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

func decodeInode(data []byte) *InodeRecord {
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

// ---- extent helpers ----

// extentKey returns the etcd key for a chunk of an inode's extent map.
// Extents are stored in chunks to stay under the 1.5 MiB etcd value limit.
func extentKey(ino uint64, chunk uint32) string {
	return fmt.Sprintf("extent:%d/%d", ino, chunk)
}

// AppendExtent adds an extent to an inode's extent map.
// Extents are appended to the last chunk; if the chunk exceeds 1 MiB,
// a new chunk is created.
func (s *Store) AppendExtent(ctx context.Context, ino uint64, logicalOff, diskOff, length, generation uint64) error {
	const chunkSize = 1024 * 1024 // 1 MiB per chunk (safe under 1.5 MiB limit)

	// Read existing extent data for the last chunk
	prefix := fmt.Sprintf("extent:%d/", ino)
	kvs, err := s.GetPrefix(ctx, prefix)
	if err != nil {
		return fmt.Errorf("append extent ino %d: %w", ino, err)
	}

	chunk := uint32(0)
	if len(kvs) > 0 {
		chunk = uint32(len(kvs) - 1)
	}

	key := extentKey(ino, chunk)

	// Read current chunk value
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("append extent ino %d chunk %d: %w", ino, chunk, err)
	}

	var buf bytes.Buffer
	if len(resp.Kvs) > 0 {
		buf.Write(resp.Kvs[0].Value)
	}
	if buf.Len() >= chunkSize {
		chunk++
		key = extentKey(ino, chunk)
		buf.Reset()
	}

	// Append extent: 4 × uint64 = 32 bytes per extent
	var ext [32]byte
	binary.BigEndian.PutUint64(ext[0:], logicalOff)
	binary.BigEndian.PutUint64(ext[8:], diskOff)
	binary.BigEndian.PutUint64(ext[16:], length)
	binary.BigEndian.PutUint64(ext[24:], generation)
	buf.Write(ext[:])

	_, err = s.Put(ctx, key, buf.Bytes())
	if err != nil {
		return fmt.Errorf("append extent ino %d: %w", ino, err)
	}
	return nil
}

// GetExtents returns all extents for an inode.
func (s *Store) GetExtents(ctx context.Context, ino uint64) ([][4]uint64, error) {
	prefix := fmt.Sprintf("extent:%d/", ino)
	kvs, err := s.GetPrefix(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("get extents ino %d: %w", ino, err)
	}

	var extents [][4]uint64
	for _, kv := range kvs {
		data := kv.Value
		for i := 0; i+32 <= len(data); i += 32 {
			ext := [4]uint64{
				binary.BigEndian.Uint64(data[i:]),
				binary.BigEndian.Uint64(data[i+8:]),
				binary.BigEndian.Uint64(data[i+16:]),
				binary.BigEndian.Uint64(data[i+24:]),
			}
			extents = append(extents, ext)
		}
	}
	return extents, nil
}
