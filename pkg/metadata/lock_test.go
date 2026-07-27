package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLockKeyFormat(t *testing.T) {
	assert.Equal(t, "lock:1", LockKey(1))
	assert.Equal(t, "lock:999", LockKey(999))
}

func TestLockModes(t *testing.T) {
	assert.Equal(t, LockMode("shared"), LockShared)
	assert.Equal(t, LockMode("exclusive"), LockExclusive)
	assert.NotEqual(t, LockShared, LockExclusive)
}
