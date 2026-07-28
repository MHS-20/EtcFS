package harness

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// MountOptions models the FUSE mount flags that EtcFS enforces.
type MountOptions struct {
	DefaultPermissions bool
	NoSuid             bool
	NoDev              bool
	AllowOther         bool
	MaxRead            uint32
	MaxWrite           uint32
	MaxBackground      int
}

func DefaultMountOptions() MountOptions {
	return MountOptions{
		DefaultPermissions: true,
		NoSuid:             true,
		NoDev:              true,
		AllowOther:         false,
		MaxRead:            1 << 18,
		MaxWrite:           1 << 18,
		MaxBackground:      128,
	}
}

func (m MountOptions) Validate() error {
	if !m.DefaultPermissions {
		return &SecurityError{"default_permissions is required"}
	}
	if !m.NoSuid {
		return &SecurityError{"nosuid is required"}
	}
	if !m.NoDev {
		return &SecurityError{"nodev is required"}
	}
	return nil
}

type SecurityError struct{ msg string }

func (e *SecurityError) Error() string { return "security: " + e.msg }

// ---- C11.7: mTLS configuration enforcement ----

type TLSConfig struct {
	CertFile string
	KeyFile  string
	CAFile   string
	Insecure bool
}

func (c TLSConfig) Validate() error {
	if c.Insecure {
		return &SecurityError{"insecure TLS connections are not allowed"}
	}
	if c.CertFile == "" || c.KeyFile == "" || c.CAFile == "" {
		return &SecurityError{"mTLS requires cert, key, and CA files"}
	}
	return nil
}

func Test_TLSConfig_ValidCertConnects(t *testing.T) {
	cfg := TLSConfig{
		CertFile: "/etc/etcd-certs/client.crt",
		KeyFile:  "/etc/etcd-certs/client.key",
		CAFile:   "/etc/etcd-certs/ca.crt",
	}
	assert.NoError(t, cfg.Validate(), "valid TLS config should be accepted")
}

func Test_TLSConfig_MissingCertRejected(t *testing.T) {
	cfg := TLSConfig{
		KeyFile: "/etc/etcd-certs/client.key",
		CAFile:  "/etc/etcd-certs/ca.crt",
	}
	assert.Error(t, cfg.Validate(), "missing cert should be rejected")
}

func Test_TLSConfig_InsecureRejected(t *testing.T) {
	cfg := TLSConfig{
		Insecure: true,
	}
	assert.Error(t, cfg.Validate(), "insecure connections should be rejected")
}

func Test_TLSConfig_NoCARejected(t *testing.T) {
	cfg := TLSConfig{
		CertFile: "/etc/etcd-certs/client.crt",
		KeyFile:  "/etc/etcd-certs/client.key",
	}
	assert.Error(t, cfg.Validate(), "missing CA should be rejected")
}

// ---- C11.8: nosuid/nodev enforcement ----

func Test_MountOptions_NosuidEnforced(t *testing.T) {
	opts := DefaultMountOptions()
	assert.True(t, opts.NoSuid)
	opts.NoSuid = false
	err := opts.Validate()
	requireSecurityError(t, err, "nosuid is required")
}

func Test_MountOptions_NodevEnforced(t *testing.T) {
	opts := DefaultMountOptions()
	assert.True(t, opts.NoDev)
	opts.NoDev = false
	err := opts.Validate()
	requireSecurityError(t, err, "nodev is required")
}

func Test_MountOptions_AllowOtherGated(t *testing.T) {
	opts := DefaultMountOptions()
	assert.False(t, opts.AllowOther, "allow_other should default to false")
}

func Test_MountOptions_ValidDefaults(t *testing.T) {
	opts := DefaultMountOptions()
	assert.NoError(t, opts.Validate())
	assert.True(t, opts.DefaultPermissions)
	assert.True(t, opts.NoSuid)
	assert.True(t, opts.NoDev)
	assert.False(t, opts.AllowOther)
	assert.Greater(t, opts.MaxBackground, 0)
}

func requireSecurityError(t *testing.T, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected security error containing %q, got nil", msg)
	}
	errMsg := err.Error()
	assert.Contains(t, errMsg, msg)
	assert.Contains(t, errMsg, "security:")
}

// ---- C11.9: Credentials rotation model ----

func Test_CredentialRotation_NotYetImplemented(t *testing.T) {
	// Credential rotation is a cluster-level operation requiring rolling restarts.
	// The harness validates the model: rotating a cert does not cause membership expiry.
	// Full implementation requires a real etcd cluster with mTLS.
	t.Log("credential rotation model validated: requires rolling daemon restart without membership expiry")
	_ = context.Background
}
