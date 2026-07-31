package metadata

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Shared monotonic counters.
//
// Inode numbers and arena IDs are both handed out from a single etcd key
// incremented under CAS.  Contention is low enough that a per-node reservation
// range is not worth its extra failure modes: a node that dies mid-range
// silently strands every number it had reserved.

// NextCounter atomically reserves the next value of a monotonically increasing
// counter key, returning the reserved value.
//
// floor is the lowest value that may ever be handed out; a counter that is
// missing or still below it starts there.  The key stores the *next* value to
// hand out, so a reader can always take the stored value as-is.
//
// The CAS is retried on contention with exponential backoff plus jitter —
// without jitter, callers that lose a race tend to have started their retry
// in lockstep and collide again on the same tick, which is what let 16
// concurrent callers exhaust an 8-attempt budget with only 9 successes in
// TestIntegration_CounterIsUniqueUnderConcurrency.  A missing key is compared
// on CreateRevision rather than value, because a value comparison against a
// key that does not exist never matches.
func (s *Store) NextCounter(ctx context.Context, key string, floor uint64) (uint64, error) {
	for attempt := 0; attempt < 20; attempt++ {
		v, err := s.Get(ctx, key)
		if err != nil {
			return 0, err
		}

		var guard clientv3.Cmp
		stored := uint64(0)
		if v == nil {
			guard = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		} else {
			stored = DecodeUint64(v)
			guard = clientv3.Compare(clientv3.Value(key), "=", string(v))
		}

		reserved := stored
		if reserved < floor {
			reserved = floor
		}

		ok, err := s.Txn(ctx, []clientv3.Cmp{guard},
			[]clientv3.Op{clientv3.OpPut(key, string(EncodeUint64(reserved+1)))}, nil)
		if err != nil {
			return 0, err
		}
		if ok {
			return reserved, nil
		}
		backoff := 1 << attempt
		if backoff > 200 {
			backoff = 200
		}
		time.Sleep(time.Duration(backoff/2+rand.Intn(backoff/2+1)) * time.Millisecond)
	}
	return 0, fmt.Errorf("counter %s: contended beyond retry limit", key)
}
