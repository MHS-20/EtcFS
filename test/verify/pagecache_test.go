package verify

import "testing"

func keyRelease(node string, ino uint64, call, ret int64) LockOp {
	return LockOp{Node: node, Ino: ino, Kind: lockReleaseExclusive, Call: call, Ret: ret}
}

func inval(node string, ino uint64, outcome byte, call, ret int64) PageInvalOp {
	return PageInvalOp{Node: node, Ino: ino, Outcome: outcome, Call: call, Ret: ret}
}

// The ordinary path: pages dropped, acknowledged, then the key given up.
func TestPageCacheInvalidationBeforeReleaseIsAccepted(t *testing.T) {
	v := CheckPageCache(
		[]LockOp{keyRelease("n1", 7, 30, 40)},
		[]PageInvalOp{inval("n1", 7, pageInvalDone, 10, 20)},
	)
	if len(v) != 0 {
		t.Fatalf("an invalidated inode was reported: %v", v)
	}
}

// A key yielded with the pages never invalidated hides the next holder's writes
// behind them, for good.
func TestPageCacheReleaseWithoutInvalidationIsReported(t *testing.T) {
	v := CheckPageCache(
		[]LockOp{keyRelease("n1", 7, 30, 40)},
		// Some other inode was invalidated, so this node does cache pages.
		[]PageInvalOp{inval("n1", 8, pageInvalDone, 10, 20)},
	)
	if len(v) != 1 {
		t.Fatalf("a key yielded with its pages still cached was not reported: %v", v)
	}
}

// The invalidation has to have finished before the release began, or the peer
// can take the inode while the pages are still there.
func TestPageCacheInvalidationAfterTheReleaseIsReported(t *testing.T) {
	v := CheckPageCache(
		[]LockOp{keyRelease("n1", 7, 30, 40)},
		[]PageInvalOp{inval("n1", 7, pageInvalDone, 35, 45)},
	)
	if len(v) != 1 {
		t.Fatalf("an invalidation that had not finished before the release was accepted: %v", v)
	}
}

// A failed invalidation must stop the release; a release after one is the
// daemon having yielded anyway.
func TestPageCacheFailedInvalidationIsReported(t *testing.T) {
	v := CheckPageCache(
		[]LockOp{keyRelease("n1", 7, 30, 40)},
		[]PageInvalOp{inval("n1", 7, pageInvalFailed, 10, 20)},
	)
	if len(v) != 1 {
		t.Fatalf("a key yielded after a failed invalidation was accepted: %v", v)
	}
}

// The FUSE session is gone, so its page cache went with it and there is nothing
// left to invalidate.
func TestPageCacheLostClientCountsAsInvalidated(t *testing.T) {
	v := CheckPageCache(
		[]LockOp{keyRelease("n1", 7, 30, 40)},
		[]PageInvalOp{inval("n1", 7, pageInvalNoClient, 10, 20)},
	)
	if len(v) != 0 {
		t.Fatalf("a release after the FUSE session died was reported: %v", v)
	}
}

// Each hold needs its own invalidation: the one that ended the previous hold
// says nothing about pages cached under this one.
func TestPageCacheSecondReleaseNeedsItsOwnInvalidation(t *testing.T) {
	v := CheckPageCache(
		[]LockOp{keyRelease("n1", 7, 30, 40), keyRelease("n1", 7, 70, 80)},
		[]PageInvalOp{inval("n1", 7, pageInvalDone, 10, 20)},
	)
	if len(v) != 1 {
		t.Fatalf("a second hold reused the first hold's invalidation: %v", v)
	}
	ok := CheckPageCache(
		[]LockOp{keyRelease("n1", 7, 30, 40), keyRelease("n1", 7, 70, 80)},
		[]PageInvalOp{inval("n1", 7, pageInvalDone, 10, 20), inval("n1", 7, pageInvalDone, 50, 60)},
	)
	if len(ok) != 0 {
		t.Fatalf("two properly invalidated holds were reported: %v", ok)
	}
}

// A daemon with page caching switched off never invalidates and never caches,
// so its releases are not evidence of anything.
func TestPageCacheNodeThatNeverCachesIsNotReported(t *testing.T) {
	v := CheckPageCache([]LockOp{keyRelease("n1", 7, 30, 40)}, nil)
	if len(v) != 0 {
		t.Fatalf("a node that caches no pages was reported: %v", v)
	}
}
