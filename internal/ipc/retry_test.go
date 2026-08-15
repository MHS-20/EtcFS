package ipc

import (
	"testing"
	"time"
)

// The delay has to stay inside a quarter of its nominal value in either
// direction: wide enough to break a cluster-wide lockstep, narrow enough that
// the attempt budgets sized against the ramp still reach the request deadline.
func TestRetryDelayIsJitteredAroundTheRamp(t *testing.T) {
	for attempt := 0; attempt < contendedAttempts; attempt++ {
		base := retryBaseDelay + time.Duration(attempt)*retryStep
		low, high := base-base/4, base+base/4

		distinct := map[time.Duration]bool{}
		for i := 0; i < 200; i++ {
			d := retryDelay(attempt)
			if d < low || d > high {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt, d, low, high)
			}
			distinct[d] = true
		}
		// An unjittered schedule is the failure being guarded against, and it
		// shows up here as one value repeated.
		if len(distinct) < 2 {
			t.Fatalf("attempt %d: delay never varied (%v)", attempt, distinct)
		}
	}
}
