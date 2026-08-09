package metadata

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEncodeDecodeInode(t *testing.T) {
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)

	rec := &InodeRecord{
		Ino:     42,
		Size:    4096,
		Blocks:  8,
		Mode:    0644,
		Nlink:   1,
		UID:     1000,
		GID:     1000,
		Rdev:    0,
		Blksize: 4096,
		Atime:   now,
		Mtime:   now,
		Ctime:   now,
	}

	encoded := EncodeInode(rec)
	assert.Len(t, encoded, 72, "encoded inode should be 72 bytes")

	decoded := DecodeInode(encoded)
	assert.NotNil(t, decoded)
	assert.Equal(t, rec.Ino, decoded.Ino)
	assert.Equal(t, rec.Size, decoded.Size)
	assert.Equal(t, rec.Blocks, decoded.Blocks)
	assert.Equal(t, rec.Mode, decoded.Mode)
	assert.Equal(t, rec.Nlink, decoded.Nlink)
	assert.Equal(t, rec.UID, decoded.UID)
	assert.Equal(t, rec.GID, decoded.GID)
	assert.Equal(t, rec.Rdev, decoded.Rdev)
	assert.Equal(t, rec.Blksize, decoded.Blksize)
	assert.Equal(t, rec.Atime.Unix(), decoded.Atime.Unix())
	assert.Equal(t, rec.Mtime.Unix(), decoded.Mtime.Unix())
	assert.Equal(t, rec.Ctime.Unix(), decoded.Ctime.Unix())
}

func TestDecodeInodeTooShort(t *testing.T) {
	assert.Nil(t, DecodeInode([]byte{1, 2, 3}))
	assert.Nil(t, DecodeInode(nil))
}

func TestEncodeInodeRoundTrip(t *testing.T) {
	original := &InodeRecord{
		Ino:     9999999,
		Size:    1 << 40,
		Mode:    0755 | uint32(1<<31), // directory
		Nlink:   42,
		UID:     65534,
		GID:     65534,
		Blksize: 4096,
	}
	encoded := EncodeInode(original)
	decoded := DecodeInode(encoded)
	assert.NotNil(t, decoded)
	assert.Equal(t, original.Ino, decoded.Ino)
	assert.Equal(t, original.Mode, decoded.Mode)
	assert.Equal(t, original.Nlink, decoded.Nlink)
}

func TestExtractNameFromKey(t *testing.T) {
	assert.Equal(t, "hello", extractNameFromKey("dirent:1/hello", 1))
	assert.Equal(t, "file.txt", extractNameFromKey("dirent:42/file.txt", 42))
	assert.Equal(t, "nested/path", extractNameFromKey("dirent:100/nested/path", 100))
}

// Every inode is written with a link count that matches the entry pointing at
// it. The old formula, (mode>>12)&1, returned 0 for directories, regular files
// and symlinks alike — only FIFOs came out right — so every symlink, device
// node and hardlink target was stored as if nothing referenced it.
func TestInitialNlink(t *testing.T) {
	cases := []struct {
		name string
		mode uint32
		want uint32
	}{
		{"directory", ModeDir | 0755, 2},
		{"regular file", ModeFile | 0644, 1},
		{"symlink", ModeSymlink | 0777, 1},
		{"fifo", 0010000 | 0644, 1},
		{"character device", 0020000 | 0644, 1},
		{"block device", 0060000 | 0644, 1},
		{"socket", 0140000 | 0644, 1},
		{"mode with no type bits", 0644, 1},
	}
	for _, c := range cases {
		if got := InitialNlink(c.mode); got != c.want {
			t.Errorf("%s: InitialNlink(%#o) = %d, want %d", c.name, c.mode, got, c.want)
		}
	}
}
