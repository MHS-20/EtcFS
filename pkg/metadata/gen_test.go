package metadata

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenKeyFormat(t *testing.T) {
	assert.Equal(t, "gen:node-1", GenKey("node-1"))
	assert.Equal(t, "gen:etcfuse-node-3", GenKey("etcfuse-node-3"))
}

func TestWithGenerationGuard(t *testing.T) {
	cmp := WithGenerationGuard("node-1", 5)

	assert.Equal(t, "gen:node-1", string(cmp.KeyBytes()))
	// The Cmp checks that the value of gen:node-1 equals "5"
	assert.NotNil(t, cmp)
}

func TestNodeInodeAlloc(t *testing.T) {
	alloc := NewNodeInodeAlloc("test-node")

	assert.Equal(t, uint64(0), alloc.Available())

	// Simulate reserve
	alloc.start = 1000000
	alloc.end = 1001000
	alloc.next = 1000000

	assert.Equal(t, uint64(1000), alloc.Available())

	ino, err := alloc.Allocate()
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%d", uint64(1000000)), fmt.Sprintf("%d", ino))

	ino, err = alloc.Allocate()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1000001), ino)
}
