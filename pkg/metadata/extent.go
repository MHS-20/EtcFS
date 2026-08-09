package metadata

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
)

// Extent maps a logical byte range of an inode onto a range of the shared
// block device.
//
// Extents live under one key per extent, `extent:<ino>/<chunk>`, with the
// value encoded as `<logical_off>,<disk_off>,<length>,<generation>`.  The
// generation is the writer's fencing generation at commit time; the scrubber
// cross-checks it to spot writes from a node that was fenced mid-flight.
//
// This file is the single owner of that encoding.  Nothing outside it should
// format or parse an extent key or value by hand.
type Extent struct {
	// Key is the etcd key the extent was read from.  Empty for an extent
	// that has not been stored yet; never part of the encoded value.
	Key string

	LogOff  uint64
	DiskOff uint64
	Length  uint64
	Gen     uint64
}

// ExtentKey returns the etcd key for one extent of an inode.
func ExtentKey(ino, chunk uint64) string {
	return fmt.Sprintf("%s%d/%d", PrefixExtent, ino, chunk)
}

// ExtentPrefix returns the etcd key prefix covering all extents of an inode.
func ExtentPrefix(ino uint64) string {
	return fmt.Sprintf("%s%d/", PrefixExtent, ino)
}

// ParseExtentKey splits an extent key into its inode and chunk numbers.
// Returns ok=false for a key that is not a well-formed extent key.
func ParseExtentKey(key string) (ino, chunk uint64, ok bool) {
	rest, found := strings.CutPrefix(key, PrefixExtent)
	if !found {
		return 0, 0, false
	}
	inoStr, chunkStr, found := strings.Cut(rest, "/")
	if !found {
		return 0, 0, false
	}
	ino, err := strconv.ParseUint(inoStr, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	chunk, err = strconv.ParseUint(chunkStr, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return ino, chunk, true
}

// InoFromExtentKey returns just the inode number of an extent key, or 0 if
// the key is malformed.  Convenience for callers that don't need the chunk.
func InoFromExtentKey(key string) uint64 {
	ino, _, _ := ParseExtentKey(key)
	return ino
}

// Encode renders the extent in its stored form.  Key is not part of the value.
func (e Extent) Encode() string {
	return fmt.Sprintf("%d,%d,%d,%d", e.LogOff, e.DiskOff, e.Length, e.Gen)
}

// Ino returns the inode the extent belongs to, derived from its key.
func (e Extent) Ino() uint64 { return InoFromExtentKey(e.Key) }

// End returns the exclusive logical end offset of the extent.
func (e Extent) End() uint64 { return e.LogOff + e.Length }

// WithinDisk reports whether the extent lies entirely inside the device byte
// range [start, end).  Used to attribute an extent to an arena.
func (e Extent) WithinDisk(start, end uint64) bool {
	return e.DiskOff >= start && e.DiskOff+e.Length <= end
}

// DecodeExtent parses a stored extent key/value pair.  Returns ok=false if the
// value is not four comma-separated integers, so a corrupt record is skipped
// rather than silently read as a zero-length extent at disk offset 0.
func DecodeExtent(key string, value []byte) (Extent, bool) {
	parts := strings.Split(string(value), ",")
	if len(parts) != 4 {
		return Extent{}, false
	}
	var fields [4]uint64
	for i, p := range parts {
		v, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			return Extent{}, false
		}
		fields[i] = v
	}
	return Extent{
		Key: key, LogOff: fields[0], DiskOff: fields[1],
		Length: fields[2], Gen: fields[3],
	}, true
}

// DecodeExtents decodes a batch of extent key/values, skipping malformed ones,
// and returns them ordered by logical offset.
//
// The ordering matters: etcd returns keys in lexicographic order, so an inode
// with more than ten extents comes back as chunk 0, 1, 10, 11, 2, … — reading
// that back in key order reassembles the file wrong.
func DecodeExtents(kvs []*mvccpb.KeyValue) []Extent {
	extents := make([]Extent, 0, len(kvs))
	for _, kv := range kvs {
		if e, ok := DecodeExtent(string(kv.Key), kv.Value); ok {
			extents = append(extents, e)
		}
	}
	sort.Slice(extents, func(i, j int) bool { return extents[i].LogOff < extents[j].LogOff })
	return extents
}

// GetExtents returns all extents of an inode, ordered by logical offset.
func (s *Store) GetExtents(ctx context.Context, ino uint64) ([]Extent, error) {
	kvs, err := s.GetPrefix(ctx, ExtentPrefix(ino))
	if err != nil {
		return nil, fmt.Errorf("get extents ino %d: %w", ino, err)
	}
	return DecodeExtents(kvs), nil
}

// AllExtents returns every extent in the filesystem, ordered by logical
// offset within no particular inode.  Used by the whole-filesystem scanners
// (fsck, scrubber, arena reconstruction).
func (s *Store) AllExtents(ctx context.Context) ([]Extent, error) {
	kvs, err := s.GetPrefix(ctx, PrefixExtent)
	if err != nil {
		return nil, fmt.Errorf("get all extents: %w", err)
	}
	return DecodeExtents(kvs), nil
}

// NextExtentChunk returns the chunk number a new extent for this inode should
// be written under.
//
// It is one past the highest chunk currently stored, not the number of stored
// extents: truncate deletes extents from the middle and the end, and deriving
// the next chunk from a count would hand back a chunk number that is still
// live and silently overwrite it.
func (s *Store) NextExtentChunk(ctx context.Context, ino uint64) (uint64, error) {
	kvs, err := s.GetPrefix(ctx, ExtentPrefix(ino))
	if err != nil {
		return 0, fmt.Errorf("next extent chunk ino %d: %w", ino, err)
	}
	next := uint64(0)
	for _, kv := range kvs {
		if _, chunk, ok := ParseExtentKey(string(kv.Key)); ok && chunk >= next {
			next = chunk + 1
		}
	}
	return next, nil
}

// AppendExtent stores a new extent for an inode under the next free chunk.
func (s *Store) AppendExtent(ctx context.Context, ino, logicalOff, diskOff, length, generation uint64) error {
	chunk, err := s.NextExtentChunk(ctx, ino)
	if err != nil {
		return err
	}
	ext := Extent{LogOff: logicalOff, DiskOff: diskOff, Length: length, Gen: generation}
	if _, err := s.Put(ctx, ExtentKey(ino, chunk), []byte(ext.Encode())); err != nil {
		return fmt.Errorf("append extent ino %d: %w", ino, err)
	}
	return nil
}
