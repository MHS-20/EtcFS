package metadata

import (
	"strings"
	"testing"
)

// An xattr key must round-trip, including for the awkward names a real caller
// produces: SELinux labels carry dots and colons, and nothing about them may
// change the inode the key parses back to.
func TestXattrKeyRoundTrip(t *testing.T) {
	cases := []struct {
		ino  uint64
		name string
	}{
		{1, "user.comment"},
		{42, "security.selinux"},
		{1 << 40, "trusted.overlay.opaque"},
		{7, "user.a:b=c"},
	}
	for _, c := range cases {
		ino, name, ok := ParseXattrKey(XattrKey(c.ino, c.name))
		if !ok {
			t.Errorf("ParseXattrKey(%q) failed", XattrKey(c.ino, c.name))
			continue
		}
		if ino != c.ino || name != c.name {
			t.Errorf("round trip gave (%d, %q), want (%d, %q)", ino, name, c.ino, c.name)
		}
	}
}

// A key that is not an xattr key must be rejected rather than parsed into a
// plausible-looking inode: ParseXattrKey runs over whatever a prefix scan
// returned, and a dirent key sharing the shape would silently be treated as an
// attribute.
func TestParseXattrKeyRejectsOtherKeys(t *testing.T) {
	for _, key := range []string{
		"dirent:1/name",
		"xattr:notanumber/name",
		"xattr:1",
		"inode:1",
		"",
	} {
		if _, _, ok := ParseXattrKey(key); ok {
			t.Errorf("ParseXattrKey(%q) accepted a key it should not", key)
		}
	}
}

// The name validator is the only thing standing between a caller and a key it
// could use to address a different attribute, so it gets its own check: a '/'
// would move the split point, and a NUL would end the name early in the
// C daemon's NUL-separated listxattr buffer.
func TestValidXattrName(t *testing.T) {
	atMax := strings.Repeat("a", MaxXattrNameLen)
	valid := []string{"user.x", "security.selinux", "a", atMax}
	for _, n := range valid {
		if !validXattrName(n) {
			t.Errorf("validXattrName(%q) = false, want true", n)
		}
	}

	invalid := []string{"", "user/x", "user\x00x", atMax + "a"}
	for _, n := range invalid {
		if validXattrName(n) {
			t.Errorf("validXattrName(%q) = true, want false", n)
		}
	}
}

// Removing an inode must take its attributes with it. A leftover attribute is
// not merely a leaked key: inode numbers are reused, so the next inode to take
// this number would inherit someone else's labels.
func TestUnlinkInodeOpsDeletesXattrs(t *testing.T) {
	cases := []struct {
		name string
		rec  *InodeRecord
	}{
		{"regular file", &InodeRecord{Ino: 5, Mode: ModeFile | 0644, Nlink: 1}},
		{"directory", &InodeRecord{Ino: 6, Mode: ModeDir | 0755, Nlink: 2}},
		{"symlink", &InodeRecord{Ino: 7, Mode: ModeSymlink | 0777, Nlink: 1}},
	}
	var s Store
	for _, c := range cases {
		want := XattrPrefix(c.rec.Ino)
		found := false
		for _, op := range s.unlinkInodeOps(c.rec) {
			if op.IsDelete() && string(op.KeyBytes()) == want {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: unlinkInodeOps did not delete %q", c.name, want)
		}
	}
}

// A hard link that still has other names keeps the inode, so it must keep the
// attributes too — they belong to the inode, not to the name being removed.
func TestUnlinkInodeOpsKeepsXattrsWhileLinksRemain(t *testing.T) {
	var s Store
	rec := &InodeRecord{Ino: 8, Mode: ModeFile | 0644, Nlink: 2}
	for _, op := range s.unlinkInodeOps(rec) {
		if op.IsDelete() && string(op.KeyBytes()) == XattrPrefix(rec.Ino) {
			t.Fatal("unlinkInodeOps deleted the attributes of an inode that still has a link")
		}
	}
}
