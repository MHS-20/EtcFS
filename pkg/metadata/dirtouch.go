package metadata

import (
	"context"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Directory timestamp write-behind.
//
// POSIX requires the mtime and ctime of a directory to move whenever an entry
// is added to or removed from it.  That is a second transaction on top of the
// one the namespace mutation itself commits, and it cannot be folded into it:
// doing so would pin the parent's record in every create and unlink, so two
// nodes making unrelated entries in one directory would abort each other.  See
// touchDir.
//
// So an unpacking tar used to pay two Raft commits per file — one to publish
// the file, one to say that the directory it went into had changed — and the
// second one for the *same* directory, over and over.  Coalescing them is what
// this is: a directory marked changed here is written once per interval however
// many entries it gained in between, and a stream of creates into one directory
// costs one timestamp commit per interval rather than one per create.
//
// The trade is that the timestamp is up to one interval late in etcd.  It is
// not late on the node that made the change — PendingDirTime answers from the
// queue, so this node's own stat of the directory is immediate — and a peer's
// listing of it is invalidated by the dirent watch as it always was, which
// fires on the entry itself rather than on the timestamp.  What a peer sees
// late is the timestamp alone, by at most the interval, which is the same trade
// deferred extent publication already makes for a file's size.
//
// Nothing here relaxes the ordering between data and metadata: the entry and
// the inode are published by the transaction that returns to the caller, and
// only the parent's clock trails it.

// dirTouch is the queue of directories whose timestamps are owed a commit.
type dirTouch struct {
	store    *Store
	interval time.Duration
	log      Logger

	mu    sync.Mutex
	dirty map[uint64]time.Time
}

// StartDirTouchBatching coalesces directory timestamp updates, writing each
// changed directory at most once per interval instead of once per entry added
// to or removed from it.  A zero or negative interval leaves every update
// committed before its operation returns.
//
// The sweep stops with ctx.  Whatever is still queued then is written by
// FlushDirTimes, which shutdown calls before the node gives up its locks.
func (s *Store) StartDirTouchBatching(ctx context.Context, interval time.Duration, log Logger) {
	if interval <= 0 {
		return
	}
	t := &dirTouch{store: s, interval: interval, log: log, dirty: map[uint64]time.Time{}}
	s.dirTouchMu.Lock()
	s.dirTouch = t
	s.dirTouchMu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				t.flush(ctx, 0)
			}
		}
	}()
}

// dirTouches returns the queue, or nil when updates are written through.
func (s *Store) dirTouches() *dirTouch {
	s.dirTouchMu.Lock()
	defer s.dirTouchMu.Unlock()
	return s.dirTouch
}

// PendingDirTime returns the timestamp a directory has been given but not yet
// published, so this node answers its own stat of the directory with the change
// it just made rather than with what etcd still holds.
func (s *Store) PendingDirTime(ino uint64) (time.Time, bool) {
	t := s.dirTouches()
	if t == nil {
		return time.Time{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	at, queued := t.dirty[ino]
	return at, queued
}

// FlushDirTimes publishes queued directory timestamps: one directory when ino
// is non-zero, every queued one when it is zero.
//
// fsync and fsyncdir call it for the directory they name, and shutdown calls it
// for all of them.  A directory nothing has queued costs nothing.
func (s *Store) FlushDirTimes(ctx context.Context, ino uint64) {
	if t := s.dirTouches(); t != nil {
		t.flush(ctx, ino)
	}
}

// flush writes the queued timestamps out.  Entries are taken off the queue
// before they are committed, so a mutation that lands during the commit queues
// the directory again rather than being dropped with it.
func (t *dirTouch) flush(ctx context.Context, only uint64) {
	t.mu.Lock()
	due := make(map[uint64]time.Time, len(t.dirty))
	for ino, at := range t.dirty {
		if only == 0 || ino == only {
			due[ino] = at
			delete(t.dirty, ino)
		}
	}
	t.mu.Unlock()

	for ino, at := range due {
		if err := t.store.commitDirTime(ctx, ino, at); err != nil {
			if t.log != nil {
				t.log.Error("directory timestamp not published; it will be retried",
					"ino", ino, "error", err)
			}
			// Requeued rather than dropped, and only if nothing newer has
			// arrived: the next sweep retries it, and a directory whose
			// timestamp is merely late is better than one stuck in the past.
			t.requeue(ino, at)
		}
	}
}

// requeue puts a failed timestamp back, unless a later one is already waiting.
func (t *dirTouch) requeue(ino uint64, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if queued, found := t.dirty[ino]; !found || queued.Before(at) {
		t.dirty[ino] = at
	}
}

// queue marks a directory changed at time at, to be published by the next
// sweep.  A directory already queued keeps the later of the two timestamps.
func (t *dirTouch) queue(ino uint64, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if queued, found := t.dirty[ino]; !found || queued.Before(at) {
		t.dirty[ino] = at
	}
}

// commitDirTime moves a directory's mtime and ctime to at.
//
// The timestamp is only ever moved forward.  A queued touch is by definition
// older than anything committed after it was queued — a mkdir folds the
// parent's new mtime into its own transaction, for one — and writing it anyway
// would take the directory's clock backwards, which is worse than the lateness
// this queue trades for.
func (s *Store) commitDirTime(ctx context.Context, ino uint64, at time.Time) error {
	return retryCAS(ctx, "publish dir time", func() (bool, error) {
		rec, rev, err := s.GetInodeRev(ctx, ino)
		if err != nil {
			return false, err
		}
		if rec == nil || !rec.Mtime.Before(at) {
			return true, nil
		}
		rec.Mtime = at
		rec.Ctime = at
		return s.Txn(ctx,
			[]clientv3.Cmp{InodeUnchanged(ino, rev)},
			[]clientv3.Op{clientv3.OpPut(InodeKey(ino), string(EncodeInode(rec)))}, nil)
	})
}
