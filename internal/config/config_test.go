package config

import (
	"testing"
	"time"
)

// The constraint the daemon's timing rests on: a request must be able to fail
// on its own deadline before the self-fencing watchdog takes the process down
// under it. A 3s lease TTL inverts the two, and used to be accepted.
func TestSelfFenceWindowClearsTheRequestTimeout(t *testing.T) {
	for _, ttl := range []time.Duration{time.Second, 3 * time.Second, 5 * time.Second} {
		if SelfFenceWindow(ttl) > RequestTimeout {
			t.Errorf("lease TTL %s should be rejected: window %s exceeds the %s request timeout",
				ttl, SelfFenceWindow(ttl), RequestTimeout)
		}
	}
	for _, ttl := range []time.Duration{6 * time.Second, 10 * time.Second, time.Minute} {
		if SelfFenceWindow(ttl) <= RequestTimeout {
			t.Errorf("lease TTL %s should be accepted: window %s is under the %s request timeout",
				ttl, SelfFenceWindow(ttl), RequestTimeout)
		}
	}
}
