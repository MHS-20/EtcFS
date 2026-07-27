// Package watch manages etcd watch multiplexing for EtcFS.
//
// Each EtcFS node watches multiple directory prefixes to detect remote
// mutations (creates, deletes, renames) and invalidate kernel caches.
// A naive approach — one etcd watcher per watched prefix — causes watch
// amplification: N nodes × D watched directories = N×D watchers, which
// can overwhelm etcd.
//
// The multiplexer consolidates watches: it maintains a small set of
// etcd watchers (one per gRPC stream) and fan-out to registered callbacks.
// This is the same pattern the Kubernetes API server's watch cache uses
// between etcd and kubelets.
//
// Phase 0: types defined.  Implementation in Phase 2.
package watch

import (
	"context"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Event represents a directory mutation detected by a watch.
type Event struct {
	Type     EventType
	Parent   uint64
	Name     string
	Ino      uint64 // target inode (for rename: new parent's ino)
	Revision int64
}

// EventType categorises a watch event.
type EventType int

const (
	EventCreate EventType = iota
	EventDelete
	EventModify
)

// Callback is invoked when a watched directory prefix changes.
type Callback func(Event)

// Mux manages watch subscriptions and fan-out.
type Mux struct {
	mu      sync.RWMutex
	etcd    *clientv3.Client
	watches map[string][]Callback // prefix → callbacks
}

// NewMux creates a watch multiplexer.
func NewMux(etcd *clientv3.Client) *Mux {
	return &Mux{
		etcd:    etcd,
		watches: make(map[string][]Callback),
	}
}

// Watch registers a callback for changes to the given prefix.
func (m *Mux) Watch(prefix string, cb Callback) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.watches[prefix] = append(m.watches[prefix], cb)
}

// Unwatch removes a callback for the given prefix.
func (m *Mux) Unwatch(prefix string, cb Callback) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cbs := m.watches[prefix]
	for i, existing := range cbs {
		if &existing == &cb {
			m.watches[prefix] = append(cbs[:i], cbs[i+1:]...)
			return
		}
	}
}

// Run starts the background watch loop.  Blocks until ctx is cancelled.
func (m *Mux) Run(ctx context.Context) {
	// Phase 2: consolidate registered prefixes into a minimal set of
	// etcd watchers, dispatch events to registered callbacks, handle
	// disconnection and list-then-watch recovery.
	<-ctx.Done()
}
