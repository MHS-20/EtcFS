package ipc

import (
	"context"
	"strings"
)

// Extended attribute handlers.
//
// The size negotiation these carry out is the awkward part of the xattr
// interface and it is deliberately settled here rather than in the C daemon:
// getxattr and listxattr are called first with size 0, to learn how many bytes
// the answer needs, and then again with a buffer of that size.  The backend
// therefore always returns the whole value and the size alongside it, and the
// C side decides which of the two the kernel asked for.  Doing it this way
// costs one extra round trip's worth of bytes on the sizing call and keeps a
// single encoding of the answer; splitting it would mean two code paths that
// have to agree on the length exactly, which is the bug this shape avoids.

// privilegedXattrPrefix reports whether a name lives in a namespace the
// unprivileged caller may not write.
//
// trusted.* is root-only by definition — it is the namespace the kernel
// reserves for things an unprivileged process must not be able to see or
// forge. security.* is left writable because that is the namespace SELinux and
// IMA labels live in, and refusing it here would break the labelled mounts
// that are one of the reasons to want xattrs at all; the kernel's own LSM
// hooks police its contents, which this filesystem is in no position to
// second-guess.
func privilegedXattrPrefix(name string) bool {
	return strings.HasPrefix(name, "trusted.")
}

// xattrPublish is the name a writer sets to hand a file over to another node
// at device speed.  See Service.publishInode.
//
// An extended attribute rather than a new operation because it needs no new
// wire opcode, no C-side handler and no library: `setfattr`, os.setxattr and
// every language's equivalent already reach it, which is what an application
// in a pipeline actually has to hand.  The user namespace because the caller
// is an ordinary process, not an operator.
const xattrPublish = "user.etcfs.publish"

// SETXATTR payload: [u64:ino][u32:name_len][name][u32:value_len][value][u32:flags][u32:uid][u32:gid]
// Response: [i32:error]
func (s *Service) handleSetxattr(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino, name, value := r.u64(), r.str(), r.blob()
	flags, uid := r.u32(), r.u32()
	if !r.ok {
		return int32Resp(errInval), nil
	}
	if privilegedXattrPrefix(name) && uid != 0 {
		return int32Resp(errPerm), nil
	}

	// An action, not an attribute: it is carried out and deliberately not
	// stored, so it never appears in a listing and reading it back gives
	// ENODATA.  Storing it would leave a file permanently carrying the record
	// of a handoff that happened once.
	if name == xattrPublish {
		if err := s.publishInode(ctx, ino); err != nil {
			s.log.Warn("publish: cannot hand the inode over", "ino", ino, "error", err)
			return int32Resp(errIO), nil
		}
		return okResp(), nil
	}

	if err := s.store.SetXattr(ctx, ino, name, value, flags); err != nil {
		return int32Resp(errnoFor(err, errIO)), nil
	}
	return okResp(), nil
}

// publishInode hands a file over to whichever node reads it next: it puts this
// node's buffered writes on the device and in etcd, and then gives up the
// inode's cached lock key without waiting for anyone to ask for it.
//
// Handing off without this costs a full recall round trip. The lock key
// outlives the operation that took it, so a producer that has finished writing
// still holds it; the consumer on another node has to write a want key, wait
// for the producer's revocation watch to see it, and only then acquire — three
// etcd round trips plus the minimum hold time, paid on the consumer's critical
// path. Publishing moves all of that to the producer, before the consumer
// arrives, so what the consumer finds is a free lock and extents already
// committed. It then reads the same physical blocks the producer wrote: only
// the extent map crossed the network.
//
// The device is not flushed here. A shared device is opened with O_DIRECT, so
// the write to it was the publication; the buffered fallback exists only for an
// unshared device, where there is no other node to hand anything to.
//
// A write racing this on the same node lands in a buffer this call has already
// published, and is left for the next flush — publishing is a statement that
// the writer has finished, and it is the caller's to make truthfully.
func (s *Service) publishInode(ctx context.Context, ino uint64) error {
	if err := s.flushInode(ctx, ino); err != nil {
		return err
	}
	return s.yieldCachedLock(ino, "publish")
}

// GETXATTR payload: [u64:ino][u32:name_len][name]
// Response: [i32:error][u32:value_len][value_bytes]
func (s *Service) handleGetxattr(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino, name := r.u64(), r.str()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	value, err := s.store.GetXattr(ctx, ino, name)
	if err != nil {
		return int32Resp(errnoFor(err, errIO)), nil
	}

	var b buf
	b.w32(0)
	b.w32(uint32(len(value)))
	b.b = append(b.b, value...)
	return b.b, nil
}

// LISTXATTR payload: [u64:ino]
// Response: [i32:error][u32:names_len][NUL-terminated names]
//
// The names are returned in the kernel's own listxattr(2) form — each name
// followed by a NUL — so the C daemon hands the buffer straight to
// fuse_reply_buf without re-encoding it.
func (s *Service) handleListxattr(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino := r.u64()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	names, err := s.store.ListXattrs(ctx, ino)
	if err != nil {
		return int32Resp(errnoFor(err, errIO)), nil
	}

	size := 0
	for _, n := range names {
		size += len(n) + 1
	}
	packed := make([]byte, 0, size)
	for _, n := range names {
		packed = append(packed, n...)
		packed = append(packed, 0)
	}

	var b buf
	b.w32(0)
	b.w32(uint32(len(packed)))
	b.b = append(b.b, packed...)
	return b.b, nil
}

// REMOVEXATTR payload: [u64:ino][u32:name_len][name][u32:uid][u32:gid]
// Response: [i32:error]
func (s *Service) handleRemovexattr(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino, name, uid := r.u64(), r.str(), r.u32()
	if !r.ok {
		return int32Resp(errInval), nil
	}
	if privilegedXattrPrefix(name) && uid != 0 {
		return int32Resp(errPerm), nil
	}

	if err := s.store.RemoveXattr(ctx, ino, name); err != nil {
		return int32Resp(errnoFor(err, errIO)), nil
	}
	return okResp(), nil
}
