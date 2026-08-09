package blockio

import (
	"os"
	"path/filepath"
	"testing"
)

// fakeSysfs builds a /sys/block-shaped tree: devices maps a device name to its
// serial ("" means the device exposes none).
func fakeSysfs(t *testing.T, devices map[string]string) {
	t.Helper()
	root := t.TempDir()
	for name, serial := range devices {
		dir := filepath.Join(root, name, "device")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if serial == "" {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, "serial"), []byte(serial+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	old := sysfsBlock
	sysfsBlock = root
	t.Cleanup(func() { sysfsBlock = old })
}

func TestResolvePath(t *testing.T) {
	fakeSysfs(t, map[string]string{
		"nvme0n1": "vol0rootvolume000",
		"nvme2n1": "vol0abcdef1234567890",
		"xvda":    "",
	})

	got, err := ResolvePath("vol-0abcdef1234567890")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != "/dev/nvme2n1" {
		t.Fatalf("got %q, want /dev/nvme2n1", got)
	}
}

func TestResolvePathUnknownVolume(t *testing.T) {
	fakeSysfs(t, map[string]string{"nvme0n1": "vol0rootvolume000"})

	if _, err := ResolvePath("vol-0missing000000000"); err == nil {
		t.Fatal("expected an error for a volume that is not attached")
	}
}
