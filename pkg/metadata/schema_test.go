package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInodeKey(t *testing.T) {
	assert.Equal(t, "inode:1", InodeKey(1))
	assert.Equal(t, "inode:42", InodeKey(42))
	assert.Equal(t, "inode:18446744073709551615", InodeKey(^uint64(0)))
}

func TestDirentKey(t *testing.T) {
	assert.Equal(t, "dirent:1/hello", DirentKey(1, "hello"))
	assert.Equal(t, "dirent:42/file.txt", DirentKey(42, "file.txt"))
}

func TestDirentPrefix(t *testing.T) {
	assert.Equal(t, "dirent:1/", DirentPrefix(1))
}

func TestLockKey(t *testing.T) {
	assert.Equal(t, "lock:99", LockKey(99))
}

func TestArenaOwnerKey(t *testing.T) {
	assert.Equal(t, "arena:node-1/7", ArenaOwnerKey("node-1", 7))
}

func TestArenaNodePrefix(t *testing.T) {
	assert.Equal(t, "arena:node-1/", ArenaNodePrefix("node-1"))
}

func TestParseArenaKey(t *testing.T) {
	node, id, ok := ParseArenaKey("arena:node-1/7")
	assert.True(t, ok)
	assert.Equal(t, "node-1", node)
	assert.Equal(t, uint64(7), id)

	node, id, ok = ParseArenaKey("arena:etcfuse-node-3/42")
	assert.True(t, ok)
	assert.Equal(t, "etcfuse-node-3", node)
	assert.Equal(t, uint64(42), id)

	_, _, ok = ParseArenaKey("arena:node-1")
	assert.False(t, ok, "no arena ID")

	_, _, ok = ParseArenaKey("free_arena:7")
	assert.False(t, ok, "wrong prefix")

	_, _, ok = ParseArenaKey("arena:node-1/notanumber")
	assert.False(t, ok, "non-numeric arena ID")
}

func TestMembershipKey(t *testing.T) {
	assert.Equal(t, "membership:etcfuse-node-3", MembershipKey("etcfuse-node-3"))
}

func TestGenKey(t *testing.T) {
	assert.Equal(t, "gen:etcfuse-node-1", GenKey("etcfuse-node-1"))
}

func TestEncodeDecodeUint64(t *testing.T) {
	tests := []uint64{0, 1, 42, 255, 65535, 1<<32 - 1, 1 << 63, ^uint64(0)}
	for _, v := range tests {
		assert.Equal(t, v, DecodeUint64(EncodeUint64(v)), "round-trip for %d", v)
	}
}

func TestPutDirentOp(t *testing.T) {
	op := PutDirent(1, "hello", 42)
	assert.Equal(t, "dirent:1/hello", string(op.KeyBytes()))
	assert.Equal(t, EncodeUint64(42), op.ValueBytes())
}

func TestDeleteDirentPrefixOp(t *testing.T) {
	op := DeleteDirentPrefix(1)
	assert.Equal(t, "dirent:1/", string(op.KeyBytes()))
	assert.True(t, op.IsDelete())
}
