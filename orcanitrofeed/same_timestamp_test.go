package orcanitrofeed

import "testing"

func TestWalkSameTimestampIndex(t *testing.T) {
	times := map[uint64]uint64{
		10: 1000,
		11: 1000,
		12: 1000,
		13: 1001,
		14: 1001,
	}
	ht := func(n uint64) (uint64, bool) {
		ts, ok := times[n]
		return ts, ok
	}
	if got := WalkSameTimestampIndex(12, 1000, 32, ht); got != 2 {
		t.Fatalf("index at 12: got %d want 2", got)
	}
	if got := WalkSameTimestampIndex(13, 1001, 32, ht); got != 0 {
		t.Fatalf("index at 13: got %d want 0", got)
	}
	if got := WalkSameTimestampIndex(14, 1001, 32, ht); got != 1 {
		t.Fatalf("index at 14: got %d want 1", got)
	}
	// lookback cap: only see one prior same-time block
	if got := WalkSameTimestampIndex(12, 1000, 1, ht); got != 1 {
		t.Fatalf("lookback=1: got %d want 1", got)
	}
}

func TestSameTimestampTracker(t *testing.T) {
	times := map[uint64]uint64{5: 50, 6: 50, 7: 50, 8: 51}
	ht := func(n uint64) (uint64, bool) {
		ts, ok := times[n]
		return ts, ok
	}
	tr := NewSameTimestampTracker(32, ht)
	if got := tr.Advance(7, 50); got != 2 {
		t.Fatalf("warm-up: got %d want 2", got)
	}
	if got := tr.Advance(8, 51); got != 0 {
		t.Fatalf("reset: got %d want 0", got)
	}
	if got := tr.Advance(9, 51); got != 1 {
		t.Fatalf("inc: got %d want 1", got)
	}
}
