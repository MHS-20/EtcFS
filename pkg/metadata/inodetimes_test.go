package metadata

import (
	"testing"
	"time"
)

// A timestamp utimes assigns may go into the past; a status change may not.
func TestTimeUpdateAppliesAssignmentsButKeepsCtimeForward(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	past := now.Add(-time.Hour)

	rec := &InodeRecord{Atime: now, Mtime: now, Ctime: now}
	u := timeUpdate{atime: past, mtime: past, ctime: past, set: setAtime | setMtime | setCtime}
	if !u.apply(rec) {
		t.Fatal("an assignment into the past should still move atime and mtime")
	}
	if !rec.Atime.Equal(past) || !rec.Mtime.Equal(past) {
		t.Errorf("atime/mtime not assigned: %v / %v", rec.Atime, rec.Mtime)
	}
	if !rec.Ctime.Equal(now) {
		t.Errorf("ctime went backwards to %v", rec.Ctime)
	}
}

// An explicit assignment states the clock outright, so a directory bump queued
// before it is superseded — and one queued after it still moves the clock on.
func TestAssignmentSupersedesAnEarlierBumpButNotALaterOne(t *testing.T) {
	base := time.Now().Truncate(time.Second)
	set := base.Add(-time.Hour)

	earlier := timeUpdate{bump: base}
	earlier.mergeAssignment(timeUpdate{mtime: set, set: setMtime})
	rec := &InodeRecord{Mtime: base, Ctime: base}
	earlier.apply(rec)
	if !rec.Mtime.Equal(set) {
		t.Errorf("the assignment should have won over the earlier bump: %v", rec.Mtime)
	}

	later := timeUpdate{mtime: set, set: setMtime}
	later.bump = base.Add(time.Minute)
	rec = &InodeRecord{Mtime: base, Ctime: base}
	later.apply(rec)
	if !rec.Mtime.Equal(base.Add(time.Minute)) {
		t.Errorf("a bump after the assignment should still move the clock: %v", rec.Mtime)
	}
}

// The queue answers this node's own stat with what it has not published yet.
func TestQueuedTimesAnswerALocalStat(t *testing.T) {
	s := &Store{}
	s.dirTouch.Store(&dirTouch{store: s, dirty: map[uint64]timeUpdate{}})

	when := time.Now().Add(-time.Hour).Truncate(time.Second)
	if !s.QueueInodeTimes(7, time.Time{}, when, time.Time{}, false, true, false) {
		t.Fatal("the queue should have accepted the assignment")
	}

	rec, moved := s.PendingInodeTimes(&InodeRecord{Ino: 7, Mtime: time.Now()})
	if !moved || !rec.Mtime.Equal(when) {
		t.Errorf("a queued mtime should be reported locally: moved=%v mtime=%v", moved, rec.Mtime)
	}
	if _, moved := s.PendingInodeTimes(&InodeRecord{Ino: 8}); moved {
		t.Error("an inode with nothing queued should report no change")
	}
}
