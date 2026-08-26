package metadata

import (
	"context"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Inode timestamp write-behind.
//
// Two kinds of timestamp change are queued here rather than committed before
// the operation that made them returns: a directory's clock moving because an
// entry was added or removed, and the times a setattr assigns outright.
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
//
// The times a setattr assigns are queued for the same reason and on the same
// terms.  `tar` sets each file's timestamps after writing it, one call per
// file, and each used to be a Raft commit of its own; a timestamp is also the
// one attribute with no enforcement meaning, so a peer seeing it an interval
// late costs nothing a permission check depends on.  Mode and ownership are
// deliberately *not* queued: a peer enforces access against what etcd holds,
// so deferring a change that takes permissions away would leave that peer
// granting them until the queue drained.

// timeUpdate is what one inode owes its record: a directory bump, an explicit
// assignment, or both.
//
// The bump is a floor — it only ever moves a clock forward, since a queued
// touch is by definition older than anything committed after it was queued.
// The assignment is a value, which utimes may legitimately set into the past.
type timeUpdate struct {
	// bump is a directory touch's timestamp, zero when none is queued.
	bump time.Time

	// atime, mtime and ctime are assigned outright; set says which of them
	// this update carries.
	atime, mtime, ctime time.Time
	set                 timeFields
}

type timeFields uint8

const (
	setAtime timeFields = 1 << iota
	setMtime
	setCtime
)

// mergeAssignment folds a later assignment into this update, field by field:
// the newer value of each named field wins, and a pending directory bump is
// dropped because the assignment happened after it and states the clock
// outright.
func (u *timeUpdate) mergeAssignment(next timeUpdate) {
	if next.set&setAtime != 0 {
		u.atime, u.set = next.atime, u.set|setAtime
	}
	if next.set&setMtime != 0 {
		u.mtime, u.set = next.mtime, u.set|setMtime
	}
	if next.set&setCtime != 0 {
		u.ctime, u.set = next.ctime, u.set|setCtime
	}
	u.bump = time.Time{}
}

// apply writes this update into a record and reports whether anything moved.
// The assignment goes on first and the bump after it, so a directory that
// gained an entry after its times were set still ends up with the later clock —
// and the bump, being forward-only, is a no-op when it did not.
func (u timeUpdate) apply(rec *InodeRecord) bool {
	changed := false
	if u.set&setAtime != 0 && !rec.Atime.Equal(u.atime) {
		rec.Atime, changed = u.atime, true
	}
	if u.set&setMtime != 0 && !rec.Mtime.Equal(u.mtime) {
		rec.Mtime, changed = u.mtime, true
	}
	// ctime forward only.  Unlike atime and mtime, which utimes may set into
	// the past, a ctime is always the moment the call that changed the inode
	// happened — so a queued one is older than anything committed after it was
	// queued, and writing it anyway would take the record's status clock
	// backwards.  That is what would otherwise happen when an unlink or a link
	// rewrites the record while these times waited.
	if u.set&setCtime != 0 && rec.Ctime.Before(u.ctime) {
		rec.Ctime, changed = u.ctime, true
	}
	if !u.bump.IsZero() && rec.Mtime.Before(u.bump) {
		rec.Mtime, rec.Ctime, changed = u.bump, u.bump, true
	}
	return changed
}

// dirTouch is the queue of directories whose timestamps are owed a commit.
type dirTouch struct {
	store    *Store
	interval time.Duration
	log      Logger

	mu    sync.Mutex
	dirty map[uint64]timeUpdate
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
	t := &dirTouch{store: s, interval: interval, log: log, dirty: map[uint64]timeUpdate{}}
	s.dirTouch.Store(t)

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
func (s *Store) dirTouches() *dirTouch { return s.dirTouch.Load() }

// QueueInodeTimes defers the times a setattr assigns, to be published by the
// next sweep.  Reports false when write-behind is off, which leaves the caller
// to commit them itself.
//
// Only the times: a caller with a mode or ownership change in the same setattr
// must commit, and must publish what is queued here first so that these older
// timestamps do not land on top of it.
func (s *Store) QueueInodeTimes(ino uint64, atime, mtime, ctime time.Time, setA, setM, setC bool) bool {
	t := s.dirTouches()
	if t == nil {
		return false
	}
	var u timeUpdate
	if setA {
		u.atime, u.set = atime, u.set|setAtime
	}
	if setM {
		u.mtime, u.set = mtime, u.set|setMtime
	}
	if setC {
		u.ctime, u.set = ctime, u.set|setCtime
	}
	if u.set == 0 {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	queued := t.dirty[ino]
	queued.mergeAssignment(u)
	t.dirty[ino] = queued
	return true
}

// PendingInodeTimes applies whatever an inode has queued to a copy of its
// record, so this node answers its own stat with the change it just made rather
// than with what etcd still holds.  Reports whether anything moved.
func (s *Store) PendingInodeTimes(rec *InodeRecord) (InodeRecord, bool) {
	out := *rec
	t := s.dirTouches()
	if t == nil {
		return out, false
	}
	t.mu.Lock()
	queued, found := t.dirty[rec.Ino]
	t.mu.Unlock()
	if !found {
		return out, false
	}
	return out, queued.apply(&out)
}

// FlushInodeTimes publishes queued timestamps: one inode when ino is non-zero,
// every queued one when it is zero.
//
// fsync and fsyncdir call it for the inode they name, shutdown calls it for all
// of them, and any operation about to rewrite a record synchronously calls it
// for that record — otherwise these timestamps, which are older, would be
// published on top of the change that overtook them.  An inode nothing has
// queued costs nothing.
func (s *Store) FlushInodeTimes(ctx context.Context, ino uint64) {
	if t := s.dirTouches(); t != nil {
		t.flush(ctx, ino)
	}
}

// flush writes the queued timestamps out.  Entries are taken off the queue
// before they are committed, so a mutation that lands during the commit queues
// the directory again rather than being dropped with it.
func (t *dirTouch) flush(ctx context.Context, only uint64) {
	t.mu.Lock()
	due := make(map[uint64]timeUpdate, len(t.dirty))
	for ino, u := range t.dirty {
		if only == 0 || ino == only {
			due[ino] = u
			delete(t.dirty, ino)
		}
	}
	t.mu.Unlock()

	for ino, u := range due {
		if err := t.store.commitInodeTimes(ctx, ino, u); err != nil {
			if t.log != nil {
				t.log.Error("inode timestamps not published; they will be retried",
					"ino", ino, "error", err)
			}
			// Requeued rather than dropped: the next sweep retries it, and a
			// clock that is merely late is better than one stuck in the past.
			// Anything queued since keeps its place, because it is newer.
			t.requeue(ino, u)
		}
	}
}

// requeue puts a failed update back underneath whatever has arrived since.
func (t *dirTouch) requeue(ino uint64, u timeUpdate) {
	t.mu.Lock()
	defer t.mu.Unlock()
	newer, found := t.dirty[ino]
	if !found {
		t.dirty[ino] = u
		return
	}
	u.mergeAssignment(newer)
	if newer.bump.After(u.bump) {
		u.bump = newer.bump
	}
	t.dirty[ino] = u
}

// queue marks a directory changed at time at, to be published by the next
// sweep.  A directory already queued keeps the later of the two timestamps.
func (t *dirTouch) queue(ino uint64, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	queued := t.dirty[ino]
	if queued.bump.Before(at) {
		queued.bump = at
	}
	t.dirty[ino] = queued
}

// commitInodeTimes writes an inode's queued timestamps.
//
// Read-modify-write under a comparison on the record's revision, rather than a
// put of a record this node kept: mode and ownership are not queued and a peer
// may have changed either while these times waited, and only the fields queued
// here may be overwritten.  An inode that has since been deleted takes the
// update to the grave, which is what it is for.
func (s *Store) commitInodeTimes(ctx context.Context, ino uint64, u timeUpdate) error {
	return retryCAS(ctx, "publish inode times", func() (bool, error) {
		rec, rev, err := s.GetInodeRev(ctx, ino)
		if err != nil {
			return false, err
		}
		if rec == nil || !u.apply(rec) {
			return true, nil
		}
		return s.Txn(ctx,
			[]clientv3.Cmp{InodeUnchanged(ino, rev)},
			[]clientv3.Op{clientv3.OpPut(InodeKey(ino), string(EncodeInode(rec)))}, nil)
	})
}
