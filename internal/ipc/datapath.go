package ipc

import (
	"context"
	"unsafe"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/blockio"
	"github.com/MHS-20/EtcFS/pkg/metadata"
	wal "github.com/MHS-20/EtcFS/pkg/walgo"
)

// Data path: everything that touches the shared block device.
//
// A write is durable on the device *before* its extent is committed to etcd,
// and the commit is guarded by this node's fencing generation.  A fenced node
// therefore leaves bytes on the device that nothing references, which is the
// safe direction to fail in — the alternative, a referenced extent written by
// a node that has lost its lease, is corruption.

// ioBuffer returns a buffer of at least n bytes usable for device I/O, along
// with the function that releases it.
//
// With O_DIRECT the buffer must be sector-aligned in both address and length,
// which needs an explicit allocation; without it a plain slice will do.  The
// returned buffer may be longer than n when rounded up to a sector.
func (s *Service) ioBuffer(n int) (b []byte, free func()) {
	if s.dev != nil && s.dev.IsDirect() {
		ss := s.dev.SectorSize()
		alignedLen := (n + ss - 1) / ss * ss
		if aligned, err := blockio.AlignedBuffer(alignedLen, ss); err == nil {
			return aligned, func() { _ = blockio.FreeBuffer(aligned) }
		}
		// Fall through: an unaligned buffer will be rejected by the device,
		// but the caller's error path is better than a panic here.
	}
	return make([]byte, n), func() {}
}

// directSafe reports whether buf can be handed to the device as-is, i.e. it is
// sector-aligned in both address and length.  Always true without O_DIRECT.
func (s *Service) directSafe(buf []byte) bool {
	if s.dev == nil || !s.dev.IsDirect() || len(buf) == 0 {
		return true
	}
	ss := uintptr(s.dev.SectorSize())
	return uintptr(len(buf))%ss == 0 && uintptr(unsafe.Pointer(&buf[0]))%ss == 0
}

// WRITE payload: [u64:ino][u64:offset][u32:data_len][data]
// Response: [i32:error][u32:written]
func (s *Service) handleWrite(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(-22), nil
	}
	ino, rest := readU64(payload)
	offset, rest := readU64(rest)
	dataLen, rest := readU32(rest)
	data := rest[:dataLen]

	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(-2), nil
	}

	if s.dev != nil {
		return s.handleWriteBlock(ctx, ino, offset, data, rec)
	}

	// No block device — update size only (metadata-only mode)
	newEnd := offset + uint64(dataLen)
	if newEnd > rec.Size {
		rec.Size = newEnd
		_, _ = s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
	}

	return writtenResp(dataLen), nil
}

func (s *Service) handleWriteBlock(ctx context.Context, ino uint64, offset uint64,
	data []byte, rec *metadata.InodeRecord) ([]byte, error) {

	dataLen := len(data)
	if dataLen == 0 {
		return writtenResp(0), nil
	}

	// A self-fenced node must not touch the shared device at all.  The
	// generation guard below is the authoritative check, but refusing here
	// avoids writing bytes we already know will never be referenced.
	if s.IsFenced() {
		s.log.Error("write: rejected, node has self-fenced", "ino", ino)
		return int32Resp(-5), nil
	}

	release, err := s.lockInode(ctx, ino, metadata.LockExclusive)
	if err != nil {
		return int32Resp(-11), nil // EAGAIN after retries
	}
	defer release()

	diskOff, err := s.allocateBlocks(ctx, uint64(dataLen))
	if err != nil {
		s.log.Warn("write: cannot allocate blocks", "ino", ino, "error", err)
		return int32Resp(-28), nil
	}

	// Under O_DIRECT the payload has to be copied into a sector-aligned
	// buffer; the extra tail bytes past dataLen are written but never
	// referenced by the extent.
	writeData := data
	if !s.directSafe(data) {
		aligned, free := s.ioBuffer(dataLen)
		defer free()
		if !s.directSafe(aligned) {
			s.alloc.Free(diskOff, uint64(dataLen))
			return int32Resp(-12), nil // ENOMEM
		}
		copy(aligned, data)
		writeData = aligned
	}

	n, werr := s.dev.WriteAt(writeData, int64(diskOff))
	if werr != nil {
		s.alloc.Free(diskOff, uint64(dataLen))
		s.log.Warn("write: block device write failed", "error", werr)
		return int32Resp(-5), nil
	}

	_ = s.dev.FlushDevice()

	gen := s.writeGeneration(ctx)

	if s.wal != nil {
		_ = s.wal.Append(&wal.Entry{
			Ino:        ino,
			LogicalOff: offset,
			DiskOff:    diskOff,
			Length:     uint64(dataLen),
			Generation: gen,
		})
	}

	if err := s.dev.SyncRange(int64(diskOff), int64(n)); err != nil {
		s.log.Warn("write: sync failed", "error", err)
	}

	// Read the range straight back.  The bytes are discarded: the point is the
	// round trip to the device, which is what makes the write visible to the
	// other attachers of an EBS Multi-Attach volume.
	readback, freeReadback := s.ioBuffer(dataLen)
	_, _ = s.dev.ReadAt(readback, int64(diskOff))
	freeReadback()

	chunk, cherr := s.store.NextExtentChunk(ctx, ino)
	if cherr != nil {
		s.alloc.Free(diskOff, uint64(dataLen))
		s.log.Warn("write: cannot determine extent chunk", "ino", ino, "error", cherr)
		return int32Resp(-5), nil
	}
	ext := metadata.Extent{
		LogOff: offset, DiskOff: diskOff, Length: uint64(dataLen), Gen: gen,
	}

	// Commit the extent and any size change together, guarded by this node's
	// fencing generation.  Data is already durable on the device; if the guard
	// rejects the commit the bytes stay unreferenced and the blocks go back to
	// the arena.
	ops := []clientv3.Op{clientv3.OpPut(metadata.ExtentKey(ino, chunk), ext.Encode())}
	if newEnd := ext.End(); newEnd > rec.Size {
		rec.Size = newEnd
		ops = append(ops, clientv3.OpPut(metadata.InodeKey(ino), string(metadata.EncodeInode(rec))))
	}

	committed, cerr := s.commitGuarded(ops)
	if cerr != nil {
		s.alloc.Free(diskOff, uint64(dataLen))
		s.log.Warn("write: metadata commit failed", "ino", ino, "error", cerr)
		return int32Resp(-5), nil
	}
	if !committed {
		s.alloc.Free(diskOff, uint64(dataLen))
		s.log.Error("write: rejected, node has been fenced",
			"ino", ino, "start_generation", s.startGen)
		return int32Resp(-5), nil
	}

	if s.wal != nil {
		_ = s.wal.MarkCommitted(ino, offset)
	}

	return writtenResp(uint32(dataLen)), nil
}

// allocateBlocks reserves device space, expanding into a fresh arena if the
// arenas this node already holds cannot satisfy the request.
func (s *Service) allocateBlocks(ctx context.Context, size uint64) (uint64, error) {
	if s.alloc.ArenaCount() > 0 {
		if diskOff, err := s.alloc.Allocate(size); err == nil {
			return diskOff, nil
		}
	}
	if _, err := s.alloc.AcquireArena(ctx); err != nil {
		return 0, err
	}
	return s.alloc.Allocate(size)
}

// writeGeneration returns the fencing generation to stamp on an extent.
// Generation 0 means "never fenced"; extents are stamped from 1 so that a
// missing stamp stays distinguishable from a genuine generation.
func (s *Service) writeGeneration(ctx context.Context) uint64 {
	gen, _ := s.store.GetMyGeneration(ctx)
	if gen < 1 {
		return 1
	}
	return gen
}

// READ payload: [u64:ino][u64:offset][u32:size]
// Response: [i32:error][u32:data_len][data_bytes]
func (s *Service) handleRead(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) < 20 {
		return int32Resp(-22), nil
	}
	ino, rest := readU64(payload)
	offset, rest := readU64(rest)
	size, _ := readU32(rest)

	s.log.Debug("READ", "ino", ino, "offset", offset, "size", size)

	if s.dev == nil {
		return int32Resp(-5), nil
	}

	// A shared lock keeps a concurrent writer off the range while it is read.
	// Best effort: a read that cannot take the lock still returns data, since
	// the extent list it works from is itself a consistent etcd snapshot.
	if release, err := s.lockInode(ctx, ino, metadata.LockShared); err == nil {
		defer release()
	}

	_ = s.dev.FlushDevice()

	var extents []metadata.Extent
	s.retryKV(func(ictx context.Context) error {
		var gerr error
		extents, gerr = s.store.GetExtents(ictx, ino)
		return gerr
	})
	s.log.Debug("READ extents", "ino", ino, "count", len(extents))
	if len(extents) == 0 {
		return dataResp(nil), nil
	}

	data, free := s.ioBuffer(int(size))
	defer free()
	data = data[:size]

	bytesRead := uint32(0)
	rem := size

	for _, ext := range extents {
		eStart, eEnd := ext.LogOff, ext.End()

		// Sparse region before this extent: leave the buffer zeroed and skip
		// past the hole.
		if offset >= eEnd || offset+uint64(rem) <= eStart {
			if offset < eStart && offset+uint64(rem) > eStart {
				gapLen := min(eStart-offset, uint64(rem))
				bytesRead += uint32(gapLen)
				rem -= uint32(gapLen)
				offset += gapLen
			}
			continue
		}

		readStart := uint64(0)
		if offset > eStart {
			readStart = offset - eStart
		}
		readLen := min(ext.Length-readStart, uint64(rem))

		dst := data[bytesRead : bytesRead+uint32(readLen)]
		n, err := s.readInto(dst, int64(ext.DiskOff+readStart))
		if err != nil {
			break
		}

		bytesRead += uint32(n)
		rem -= uint32(readLen)
		if rem == 0 {
			break
		}
		offset = eStart + readLen
	}

	return dataResp(data[:bytesRead]), nil
}

// readInto fills dst from the device at diskOff, bouncing through an aligned
// buffer when dst itself cannot be the target of an O_DIRECT read.
func (s *Service) readInto(dst []byte, diskOff int64) (int, error) {
	if s.directSafe(dst) {
		return s.dev.ReadAt(dst, diskOff)
	}

	bounce, free := s.ioBuffer(len(dst))
	defer free()

	if _, err := s.dev.ReadAt(bounce, diskOff); err != nil {
		return 0, err
	}
	return copy(dst, bounce), nil
}

// truncate drops or shortens every extent of an inode that lies beyond
// newSize, returning the freed device ranges to the arena.
func (s *Service) truncate(ctx context.Context, ino uint64, newSize uint64) {
	extents, err := s.store.GetExtents(ctx, ino)
	if err != nil {
		s.log.Warn("truncate: cannot read extents", "ino", ino, "error", err)
		return
	}
	for _, ext := range extents {
		switch {
		case ext.LogOff >= newSize:
			_ = s.store.Delete(ctx, ext.Key)
			if s.dev != nil {
				s.alloc.Free(ext.DiskOff, ext.Length)
			}
		case ext.End() > newSize:
			keepLen := newSize - ext.LogOff
			shortened := ext
			shortened.Length = keepLen
			_, _ = s.store.Put(ctx, ext.Key, []byte(shortened.Encode()))
			if s.dev != nil {
				s.alloc.Free(ext.DiskOff+keepLen, ext.Length-keepLen)
			}
		}
	}
}
