package blockio

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	blkSSZGet    = 0x1268
	blkGetSize64 = 0x80081272
)

type Device struct {
	fd         int
	path       string
	sectorSize int
	totalSize  int64
}

func Open(path string) (*Device, error) {
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_DIRECT, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	d := &Device{fd: fd, path: path}

	if err := d.queryGeometry(); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}

	return d, nil
}

func (d *Device) queryGeometry() error {
	d.sectorSize = 512

	var sec uint32
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(d.fd), blkSSZGet,
		uintptr(unsafe.Pointer(&sec)))
	if errno == 0 && sec > 0 {
		d.sectorSize = int(sec)
	}

	var bs uint64
	_, _, errno = syscall.Syscall(syscall.SYS_IOCTL, uintptr(d.fd), blkGetSize64,
		uintptr(unsafe.Pointer(&bs)))
	if errno == 0 {
		d.totalSize = int64(bs)
	} else {
		var stat unix.Stat_t
		if err := unix.Fstat(d.fd, &stat); err != nil {
			return fmt.Errorf("stat: %w", err)
		}
		d.totalSize = stat.Size
	}

	return nil
}

func (d *Device) SectorSize() int  { return d.sectorSize }
func (d *Device) TotalSize() int64 { return d.totalSize }
func (d *Device) Path() string     { return d.path }

func (d *Device) ReadAt(buf []byte, offset int64) (int, error) {
	if offset%int64(d.sectorSize) != 0 || len(buf)%d.sectorSize != 0 {
		return 0, fmt.Errorf("misaligned read: offset=%d len=%d sector=%d",
			offset, len(buf), d.sectorSize)
	}
	return unix.Pread(d.fd, buf, offset)
}

func (d *Device) WriteAt(buf []byte, offset int64) (int, error) {
	if offset%int64(d.sectorSize) != 0 || len(buf)%d.sectorSize != 0 {
		return 0, fmt.Errorf("misaligned write: offset=%d len=%d sector=%d",
			offset, len(buf), d.sectorSize)
	}
	return unix.Pwrite(d.fd, buf, offset)
}

func (d *Device) SyncRange(offset int64, length int64) error {
	align := int64(d.sectorSize)
	offAligned := offset & ^(align - 1)
	lenAligned := length + (offset - offAligned)
	_, _, errno := syscall.Syscall6(syscall.SYS_SYNC_FILE_RANGE, uintptr(d.fd),
		uintptr(offAligned), uintptr(lenAligned),
		syncFileRangeWrite|syncFileRangeWaitAfter, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

const (
	syncFileRangeWrite     uintptr = 2
	syncFileRangeWaitAfter uintptr = 4
)

func (d *Device) Close() error {
	return syscall.Close(d.fd)
}

func AlignedBuffer(size int, align int) ([]byte, error) {
	alloc := (size + align - 1) / align * align
	if alloc < os.Getpagesize() {
		alloc = os.Getpagesize()
	}
	b, err := unix.Mmap(-1, 0, alloc, unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANONYMOUS|unix.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func FreeBuffer(buf []byte) error {
	return unix.Munmap(buf)
}
