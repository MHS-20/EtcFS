package metadata

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The partition case these tests exist for: the etcd client's KeepAlive
// channel is never closed under a total network partition, so the keepalive
// loop never reaches the reconnect path that clears `alive`.  Only the
// staleness of the last successful keepalive can reveal the partition
// locally.  Constructed directly rather than through NewMembership because
// no etcd client is needed — IsAlive reads nothing but local state.

func TestMembership_IsAlive_FreshKeepalive(t *testing.T) {
	m := &Membership{leaseTTL: 10 * time.Second, alive: true, lastAlive: time.Now()}
	assert.True(t, m.IsAlive(), "a just-renewed lease must be alive")
}

func TestMembership_IsAlive_WithinTTL(t *testing.T) {
	// The client renews at roughly TTL/3, so this is ordinary healthy jitter
	// and must not read as dead.
	m := &Membership{
		leaseTTL:  10 * time.Second,
		alive:     true,
		lastAlive: time.Now().Add(-4 * time.Second),
	}
	assert.True(t, m.IsAlive(), "a keepalive within the TTL must not trip the deadline")
}

// Regression test for the self-fencing gap: `alive` stays true because the
// KeepAlive channel never closed, but no keepalive has landed in longer than
// the lease TTL — etcd has already expired the lease server-side.  Before the
// deadline check this returned true indefinitely and the watchdog never fired.
func TestMembership_IsAlive_StaleKeepaliveDespiteAliveFlag(t *testing.T) {
	m := &Membership{
		leaseTTL:  10 * time.Second,
		alive:     true, // never cleared: the partition did not close the channel
		lastAlive: time.Now().Add(-11 * time.Second),
	}
	assert.False(t, m.IsAlive(),
		"a lease unrenewed for longer than its TTL must read as dead even while the alive flag is set")
}

func TestMembership_IsAlive_NeverEstablished(t *testing.T) {
	m := &Membership{leaseTTL: 10 * time.Second}
	assert.False(t, m.IsAlive(), "a membership that never registered must not read as alive")
}

func TestMembership_IsAlive_ExplicitlyDead(t *testing.T) {
	m := &Membership{
		leaseTTL:  10 * time.Second,
		alive:     false,
		lastAlive: time.Now(),
	}
	assert.False(t, m.IsAlive(), "an explicitly cleared alive flag wins over a recent keepalive")
}

// The fencing controller reads instance_id out of a departed node's last
// membership value, so every shape that value can take has to be handled
// without failing — the node is already gone and cannot be asked again.
func TestInstanceIDFromMembershipHandlesEveryShape(t *testing.T) {
	full, err := json.Marshal(MembershipRecord{
		NodeID: "n1", Cluster: "etcfuse", JoinedAt: time.Now().UTC(), InstanceID: "i-abc123",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := InstanceIDFromMembership(full); got != "i-abc123" {
		t.Errorf("full record: got %q, want i-abc123", got)
	}

	cases := map[string]string{
		"no instance_id field":  `{"node_id":"n1","cluster":"etcfuse"}`,
		"empty instance_id":     `{"node_id":"n1","instance_id":""}`,
		"not JSON at all":       `n1`,
		"truncated":             `{"node_id":"n1","instance_`,
		"empty value":           ``,
		"instance_id not first": `{"cluster":"c","instance_id":"i-xyz"}`,
	}
	for name, value := range cases {
		got := InstanceIDFromMembership([]byte(value))
		want := ""
		if name == "instance_id not first" {
			want = "i-xyz"
		}
		if got != want {
			t.Errorf("%s: got %q, want %q", name, got, want)
		}
	}
}
