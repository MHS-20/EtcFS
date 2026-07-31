package metadata

import (
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
