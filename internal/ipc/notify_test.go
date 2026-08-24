package ipc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/internal/config"
)

// A client that keeps its socket open and never answers costs a full ack
// timeout per lock release, on a path that is one socket and one thread. After
// enough of them in a row it is declared unresponsive and acknowledged
// messages fail immediately instead.
func TestNotifyBreakerTripsOnRepeatedAckTimeouts(t *testing.T) {
	n := &notifyServer{ackTimeout: 20 * time.Millisecond}

	// A fresh pipe per attempt: a timeout drops the connection, and the wedged
	// client this models reconnects afterwards. The breaker has to count across
	// those reconnections or it never reaches its limit.
	wedge := func() {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = client.Close() })
		go func() { _, _ = client.Read(make([]byte, 12)) }() // read, never answer
		n.set(server)
	}

	for i := 1; i < notifyBreakerTrips; i++ {
		wedge()
		if err := n.send([]byte("msg"), true); !errors.Is(err, errNoNotifyClient) {
			t.Fatalf("attempt %d: got %v, want a lost client", i, err)
		}
	}

	wedge()
	err := n.send([]byte("msg"), true)
	if !errors.Is(err, errNotifyClientUnresponsive) {
		t.Fatalf("the breaker did not trip after %d timeouts: %v", notifyBreakerTrips, err)
	}

	// While tripped, an acknowledged message must fail without waiting: a
	// release that still paid the timeout would be no better off.
	wedge()
	start := time.Now()
	err = n.send([]byte("msg"), true)
	if !errors.Is(err, errNotifyClientUnresponsive) {
		t.Fatalf("after tripping, got %v, want the client reported unresponsive", err)
	}
	if waited := time.Since(start); waited > n.ackTimeout {
		t.Fatalf("waited %v for a send that should have failed immediately", waited)
	}

	// An unacknowledged message is unaffected: nothing waits on it, so there is
	// nothing for the breaker to protect.
	if err := n.send([]byte("msg"), false); err != nil {
		t.Fatalf("unacknowledged send failed while the breaker was tripped: %v", err)
	}
}

// The dirent watch is the only thing that invalidates a cached name, a cached
// absence, or a cached directory listing on this node. etcd ends a watch for
// reasons that have nothing to do with this process stopping — a compaction
// past the watched revision, most often — so a drain that stops at the first
// closed channel leaves the daemon serving from caches nothing can ever
// invalidate again. It must re-open instead.
func TestDirentWatchIsReopenedWhenItEnds(t *testing.T) {
	s := &Service{log: config.NewLogger(0)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opens := make(chan struct{}, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.watchDirents(ctx, func() clientv3.WatchChan {
			opens <- struct{}{}
			ch := make(chan clientv3.WatchResponse)
			close(ch) // a watch that has already ended
			return ch
		})
	}()

	// Two opens is the whole property: the first is the initial watch, the
	// second only happens because the loop noticed the first had ended.
	for i := 0; i < 2; i++ {
		select {
		case <-opens:
		case <-time.After(2 * time.Second):
			t.Fatalf("the watch was opened %d times; it stopped after the channel closed", i)
		}
	}

	// And it stops when the daemon does, rather than looping past shutdown.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the watch loop outlived its context")
	}
}
