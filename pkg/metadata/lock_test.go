package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLockKeyFormat(t *testing.T) {
	assert.Equal(t, "lock:1/exclusive/7", LockKey(1, LockExclusive, 7))
	assert.Equal(t, "lock:999/shared/42", LockKey(999, LockShared, 42))
	assert.Equal(t, "lock:1/", LockPrefix(1))
	assert.Equal(t, "lock:1/shared/", LockModePrefix(1, LockShared))
}

// The mode lives in the key, so a transaction can ask "is any writer holding
// this?" as a range comparison with no value to parse.
func TestParseLockKey(t *testing.T) {
	mode, ok := ParseLockKey("lock:5/exclusive/12", 5)
	assert.True(t, ok)
	assert.Equal(t, LockExclusive, mode)

	mode, ok = ParseLockKey("lock:5/shared/12", 5)
	assert.True(t, ok)
	assert.Equal(t, LockShared, mode)

	// An inode's prefix must not match a longer inode number sharing its digits.
	_, ok = ParseLockKey("lock:50/shared/1", 5)
	assert.False(t, ok)

	for _, k := range []string{"lock:5/bogus/1", "lock:5/exclusive", "inode:5", ""} {
		_, ok := ParseLockKey(k, 5)
		assert.False(t, ok, k)
	}
}

func TestLockModes(t *testing.T) {
	assert.Equal(t, LockMode("shared"), LockShared)
	assert.Equal(t, LockMode("exclusive"), LockExclusive)
	assert.NotEqual(t, LockShared, LockExclusive)
}
