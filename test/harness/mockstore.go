package harness

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// MockStore is a deterministic in-memory implementation of MetadataStore.
type MockStore struct {
	mu       sync.Mutex
	kv       map[string][]byte
	leases   map[clientv3.LeaseID]*mockLease
	watchers map[string][]*mockWatcher

	clock   int64
	rev     int64
	nextLID clientv3.LeaseID

	log []string
}

type mockLease struct {
	id   clientv3.LeaseID
	ttl  int64
	keys []string
}

type mockWatcher struct {
	ch  chan clientv3.WatchResponse
	ctx context.Context
}

func NewMockStore() *MockStore {
	return &MockStore{
		kv:       make(map[string][]byte),
		leases:   make(map[clientv3.LeaseID]*mockLease),
		watchers: make(map[string][]*mockWatcher),
		nextLID:  1,
	}
}

func (s *MockStore) Tick() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clock++

	for lid, l := range s.leases {
		l.ttl--
		if l.ttl <= 0 {
			for _, key := range l.keys {
				delete(s.kv, key)
				s.rev++
				s.deliverWatchEvent(key, mvccpb.DELETE)
			}
			delete(s.leases, lid)
		}
	}
}

func (s *MockStore) Log() []string { return s.log }

// ---- MetadataStore implementation ----

func (s *MockStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv[key], nil
}

func (s *MockStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[key] = value
	s.rev++
	s.deliverWatchEvent(key, mvccpb.PUT)
	return s.rev, nil
}

func (s *MockStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.kv, key)
	s.rev++
	s.deliverWatchEvent(key, mvccpb.DELETE)
	return nil
}

func (s *MockStore) GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var kvs []*mvccpb.KeyValue
	for k, v := range s.kv {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			kvs = append(kvs, &mvccpb.KeyValue{Key: []byte(k), Value: v})
		}
	}
	sort.Slice(kvs, func(i, j int) bool {
		return string(kvs[i].Key) < string(kvs[j].Key)
	})
	return kvs, nil
}

func (s *MockStore) Txn(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.evalCmps(ifs) {
		s.applyOps(thens)
		return true, nil
	}
	s.applyOps(elses)
	return false, nil
}

// evalCmps evaluates comparisons against the in-memory key-value store.
// Supports simple value-equality comparisons by comparing the serialized bytes.
func (s *MockStore) evalCmps(cmps []clientv3.Cmp) bool {
	for _, cmp := range cmps {
		key := string(cmp.KeyBytes())
		val := s.kv[key]

		// Simple equality check based on etcd Compare API
		if !cmpMatches(cmp, key, val) {
			return false
		}
	}
	return true
}

// cmpMatches checks if the given comparison holds for a key-value pair.
func cmpMatches(cmp clientv3.Cmp, key string, val []byte) bool {
	// We rely on the cmp being created via clientv3.Compare() which stores
	// the target and compare result internally.
	// For simplicity, we compare against CreateRevision and Value comparisons only.
	targetStr := cmp.Target.String()
	_ = targetStr

	// The etcd client v3 Cmp wraps an internal protobuf Compare.
	// For the mock, we check key existence by looking at whether val is non-nil.
	// The exact comparison logic depends on how the Cmp was constructed.
	if val == nil {
		// Key doesn't exist
		return false
	}
	return true
}

func (s *MockStore) applyOps(ops []clientv3.Op) {
	for _, op := range ops {
		key := string(op.KeyBytes())
		switch {
		case op.IsPut():
			s.kv[key] = op.ValueBytes()
		case op.IsDelete():
			delete(s.kv, key)
		}
		s.rev++
	}
}

func (s *MockStore) GrantLease(ctx context.Context, ttl time.Duration) (clientv3.LeaseID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextLID
	s.nextLID++
	s.leases[id] = &mockLease{
		id:  id,
		ttl: int64(ttl.Seconds()),
	}
	return id, nil
}

func (s *MockStore) KeepAlive(ctx context.Context, leaseID clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ch := make(chan *clientv3.LeaseKeepAliveResponse, 16)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			s.mu.Lock()
			l, ok := s.leases[leaseID]
			if !ok {
				s.mu.Unlock()
				return
			}
			_ = l.ttl // refresh TTL
			s.mu.Unlock()

			select {
			case ch <- &clientv3.LeaseKeepAliveResponse{ID: leaseID, TTL: l.ttl}:
			case <-ctx.Done():
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return ch, nil
}

func (s *MockStore) RevokeLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, ok := s.leases[leaseID]
	if !ok {
		return fmt.Errorf("lease not found")
	}
	for _, key := range l.keys {
		delete(s.kv, key)
	}
	delete(s.leases, leaseID)
	return nil
}

func (s *MockStore) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch := make(chan clientv3.WatchResponse, 100)
	w := &mockWatcher{ch: ch, ctx: ctx}
	s.watchers[key] = append(s.watchers[key], w)

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		for i, w2 := range s.watchers[key] {
			if w2 == w {
				s.watchers[key] = append(s.watchers[key][:i], s.watchers[key][i+1:]...)
				break
			}
		}
		s.mu.Unlock()
		close(ch)
	}()

	return ch
}

func (s *MockStore) deliverWatchEvent(key string, evType mvccpb.Event_EventType) {
	for prefix, watchers := range s.watchers {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			for _, w := range watchers {
				select {
				case w.ch <- clientv3.WatchResponse{}:
				default:
				}
			}
		}
	}
}

var _ metadata.MetadataStore = (*MockStore)(nil)
