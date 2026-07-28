package blockio

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestOpen(t *testing.T) {
	f, err := os.CreateTemp("", "blockio-test-*")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	require.NoError(t, f.Truncate(4096))
	f.Close()

	dev, err := Open(f.Name())
	require.NoError(t, err)
	defer dev.Close()

	assert.Greater(t, dev.SectorSize(), 0)
	assert.Greater(t, dev.TotalSize(), int64(0))
}

func TestReadWriteAligned(t *testing.T) {
	f, err := os.CreateTemp("", "blockio-test-*")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	require.NoError(t, f.Truncate(4096*10))
	f.Close()

	dev, err := Open(f.Name())
	require.NoError(t, err)
	defer dev.Close()

	ss := dev.SectorSize()
	require.Greater(t, ss, 0)

	buf, err := AlignedBuffer(ss, ss)
	require.NoError(t, err)
	defer FreeBuffer(buf)

	pattern := buf[:ss]
	for i := range pattern {
		pattern[i] = byte(i%256) ^ 0xAA
	}

	n, err := dev.WriteAt(pattern, int64(ss))
	require.NoError(t, err)
	assert.Equal(t, ss, n)

	readBuf, err := AlignedBuffer(ss, ss)
	require.NoError(t, err)
	defer FreeBuffer(readBuf)

	n, err = dev.ReadAt(readBuf[:ss], int64(ss))
	require.NoError(t, err)
	assert.Equal(t, ss, n)
	assert.Equal(t, pattern, readBuf[:ss])
}

func TestSyncRange(t *testing.T) {
	f, err := os.CreateTemp("", "blockio-test-*")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	require.NoError(t, f.Truncate(4096*10))
	f.Close()

	dev, err := Open(f.Name())
	require.NoError(t, err)
	defer dev.Close()

	ss := dev.SectorSize()
	buf, err := AlignedBuffer(ss, ss)
	require.NoError(t, err)
	defer FreeBuffer(buf)

	_, err = dev.WriteAt(buf[:ss], 0)
	require.NoError(t, err)

	err = dev.SyncRange(0, int64(ss))
	assert.NoError(t, err)
}

func TestAlignedBuffer(t *testing.T) {
	ss := 512
	buf, err := AlignedBuffer(ss, ss)
	require.NoError(t, err)
	defer FreeBuffer(buf)

	assert.GreaterOrEqual(t, len(buf), ss)
	assert.Equal(t, 0, len(buf)%ss)
}

func TestUnmapFreesBuffer(t *testing.T) {
	buf, err := AlignedBuffer(4096, 4096)
	require.NoError(t, err)
	err = FreeBuffer(buf)
	assert.NoError(t, err)
}

func TestSectorSizeNonZero(t *testing.T) {
	dev, err := Open("/dev/zero")
	if err != nil {
		t.Skip("cannot open /dev/zero")
	}
	defer dev.Close()
	assert.Greater(t, dev.SectorSize(), 0)
}

func init() { _ = unix.MAP_ANONYMOUS }
