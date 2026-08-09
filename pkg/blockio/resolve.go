package blockio

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// sysfsBlock is the sysfs directory listing block devices; a variable so tests
// can point it at a fixture tree.
var sysfsBlock = "/sys/block"

// ResolvePath returns the current device path of the volume with the given
// cloud volume ID (e.g. "vol-0abcdef1234567890"), matching it against the
// serial numbers exposed in sysfs.
//
// Device paths are not stable: an EBS volume detached and reattached to a Nitro
// instance can come back under a different NVMe name, so the path must be
// resolved from the volume ID on every start rather than remembered. The serial
// reported by the device is the volume ID with its dashes stripped.
func ResolvePath(volumeID string) (string, error) {
	want := normalizeSerial(volumeID)
	if want == "" {
		return "", fmt.Errorf("empty volume ID")
	}

	entries, err := os.ReadDir(sysfsBlock)
	if err != nil {
		return "", fmt.Errorf("list %s: %w", sysfsBlock, err)
	}

	for _, e := range entries {
		if normalizeSerial(readSerial(e.Name())) != want {
			continue
		}
		// sysfs escapes '/' in device names as '!' (e.g. cciss!c0d0).
		return "/dev/" + strings.ReplaceAll(e.Name(), "!", "/"), nil
	}

	return "", fmt.Errorf("no block device with serial matching volume %s", volumeID)
}

// readSerial returns the serial of a sysfs block device, or "" if it has none.
// NVMe namespaces carry it on the parent controller, other transports on the
// device itself.
func readSerial(name string) string {
	for _, p := range []string{
		filepath.Join(sysfsBlock, name, "device", "serial"),
		filepath.Join(sysfsBlock, name, "serial"),
	} {
		if b, err := os.ReadFile(p); err == nil {
			if s := strings.TrimSpace(string(b)); s != "" {
				return s
			}
		}
	}
	return ""
}

func normalizeSerial(s string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(s), "-", ""))
}
