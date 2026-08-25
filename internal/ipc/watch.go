package ipc

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Resumable prefix watches.
//
// Three things in this daemon are driven by a cluster-wide etcd watch: name
// invalidation, attribute invalidation, and a peer asking for a cached lock
// back.  All three had the same shape and the same hole — a single `range` over
// the channel etcd handed back, which ends for reasons that have nothing to do
// with this process stopping.  A leader change, a dropped connection or a
// compaction past the watched revision closes the channel, and the loop simply
// finished: the daemon went on serving with its invalidations silently ended
// for the rest of its life.
//
// So a watch is re-opened here, and — the part that matters for how long a
// cache may be trusted — re-opened *from the revision after the last one
// delivered*.  etcd replays what happened in the gap, so an ordinary
// reconnection loses nothing at all and the timeouts stop being the only thing
// standing between a stale name and a wrong answer.
//
// One case cannot be replayed: etcd compacting past the revision being resumed
// from, which discards the history the replay would have read.  That is
// reported as a gap rather than papered over, because it is the one moment when
// a cache built on this watch has to be thrown away and the timeouts really are
// all that is left.

// rewatchDelay keeps a watch that fails immediately and repeatedly from
// becoming a busy loop against etcd.
const rewatchDelay = 100 * time.Millisecond

// watcher is one prefix watch and what to do with what it delivers.
type watcher struct {
	// what names the watch in logs.
	what string
	// prefix is the key space watched.
	prefix string
	// event handles one change.  Called in delivery order, on one goroutine.
	event func(*clientv3.Event)
	// gap is called when the watch had to skip forward — etcd compacted past
	// the revision being resumed from — so changes were missed rather than
	// replayed.  Whatever this watch keeps up to date is stale from here.
	gap func()
	// opened is called each time the watch is established, including the first.
	// A cache that is only sound while this watch is delivering is armed here.
	opened func()
	// open, when set, supplies the watch channel instead of the store, given the
	// revision to resume from (zero for "from current").  A test sets it to
	// drive the re-opening without an etcd.
	open func(rev int64) clientv3.WatchChan
}

// channel opens the watch at the revision to resume from.
func (s *Service) channel(ctx context.Context, w watcher, resume int64) clientv3.WatchChan {
	if w.open != nil {
		return w.open(resume)
	}
	opts := []clientv3.OpOption{clientv3.WithPrefix()}
	if resume > 0 {
		opts = append(opts, clientv3.WithRev(resume))
	}
	return s.store.Watch(ctx, w.prefix, opts...)
}

// runWatch delivers a watcher's events until ctx ends, re-opening the watch
// from where it stopped whenever etcd ends it.
func (s *Service) runWatch(ctx context.Context, w watcher) {
	// Zero means "from current", which is right only for the very first open:
	// there is nothing before it to have missed.
	var resume int64

	for ctx.Err() == nil {
		ch := s.channel(ctx, w, resume)
		if w.opened != nil {
			w.opened()
		}

		compacted := false
		for resp := range ch {
			if resp.Canceled {
				// A compaction past the resume point is the one cancellation
				// that loses history; every other reason is answered by opening
				// again from the same place.
				if resp.CompactRevision > 0 {
					compacted = true
				}
				break
			}
			for _, ev := range resp.Events {
				w.event(ev)
			}
			// The header's revision covers events this response did not carry
			// as well, so resuming from it is what makes a quiet watch resume
			// cheaply rather than replay from where it last saw traffic.
			resume = resp.Header.Revision + 1
		}

		if ctx.Err() != nil {
			return
		}

		if compacted {
			s.log.Error("the "+w.what+" watch could not be resumed: etcd has compacted past "+
				"the changes it missed. Everything it keeps fresh on this node is stale "+
				"until it times out", "prefix", w.prefix, "resume_revision", resume)
			resume = 0
			if w.gap != nil {
				w.gap()
			}
		} else {
			s.log.Warn("the "+w.what+" watch ended; re-establishing it from where it stopped",
				"prefix", w.prefix, "resume_revision", resume)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(rewatchDelay):
		}
	}
}
