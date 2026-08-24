package ipc

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
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

// Two notifications written back to back share one read on a stream socket.
// The name used to carry no length, so the reader could only take "the rest of
// what arrived" as the name — which swallowed the message behind it and left
// every later header being read from the middle of one. Nothing recovered from
// that: acknowledged messages stopped being recognised, the release waiting on
// one timed out, and the connection was dropped for good, which switched the
// kernel's page cache off for the life of the mount.
//
// The reader below is the C one in pkg/fuse/fuse.c, in the only terms that
// matter: header, declared length, exactly that many bytes.
func TestNotifyMessagesAreSelfDelimiting(t *testing.T) {
	type msg struct {
		typ  uint32
		ino  uint64
		name string
	}
	want := []msg{
		{notifyInvalEntry, 42, "first-name"},
		{notifyInvalEntry, 42, "a-much-longer-second-name"},
		{notifyInvalInode, 7, ""},
		{notifyInvalEntry, 1, strings.Repeat("x", notifyMaxName)},
	}

	var stream []byte
	for _, m := range want {
		stream = append(stream, notifyMsg(m.typ, m.ino, m.name)...)
	}

	var got []msg
	for len(stream) > 0 {
		if len(stream) < notifyHeaderLen {
			t.Fatalf("%d bytes left over, less than one header", len(stream))
		}
		nlen := binary.BigEndian.Uint32(stream[12:16])
		if nlen > notifyMaxName {
			t.Fatalf("name length %d exceeds the reader's buffer, so the stream is out of step", nlen)
		}
		if uint32(len(stream)) < notifyHeaderLen+nlen {
			t.Fatalf("message declares %d name bytes but only %d remain", nlen, len(stream)-notifyHeaderLen)
		}
		got = append(got, msg{
			typ:  binary.BigEndian.Uint32(stream[0:4]),
			ino:  binary.BigEndian.Uint64(stream[4:12]),
			name: string(stream[notifyHeaderLen : notifyHeaderLen+nlen]),
		})
		stream = stream[notifyHeaderLen+nlen:]
	}

	if len(got) != len(want) {
		t.Fatalf("recovered %d messages from the stream, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("message %d: got {%d %d %q}, want {%d %d %q}",
				i, got[i].typ, got[i].ino, got[i].name, want[i].typ, want[i].ino, want[i].name)
		}
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
