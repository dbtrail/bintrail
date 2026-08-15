package query

import (
	"testing"
	"time"
)

// TestBrowsePlanFromHours is the decision table for the unbounded-browse plan
// (#1353). The plan exists to hand topNSatisfiedLive a proof on the default
// browse (no since/until), so every "nil" row here is a fail-open: the merged
// live+archive read runs, slower but complete.
func TestBrowsePlanFromHours(t *testing.T) {
	h := func(daysAgo int, hour int) time.Time {
		base := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
		return base.AddDate(0, 0, -daysAgo).Add(time.Duration(hour) * time.Hour)
	}
	live := []time.Time{h(0, 10), h(0, 11), h(0, 12)}

	tests := []struct {
		name      string
		live      []time.Time
		archived  []time.Time
		wantStart time.Time // zero → want nil plan
		wantEnd   time.Time
		why       string
	}{
		{
			name: "archives strictly below the live floor",
			live: live, archived: []time.Time{h(2, 0), h(0, 9)},
			wantStart: h(0, 10), wantEnd: h(0, 13),
			why: "the rotation-boundary invariant holds, so one live range spans the live tier",
		},
		{
			name: "no archives at all",
			live: live, archived: nil,
			wantStart: h(0, 10), wantEnd: h(0, 13),
			why: "nothing registered can sit above the floor",
		},
		{
			name: "live hours with an interior hole",
			live: []time.Time{h(0, 8), h(0, 12)}, archived: []time.Time{h(0, 7)},
			wantStart: h(0, 8), wantEnd: h(0, 13),
			why: "every archived hour is below the floor, so the hole holds no archived data either — the span is about what archives CANNOT hold",
		},
		{
			name: "archived hour equals the oldest live hour",
			live: live, archived: []time.Time{h(0, 10)},
			why: "at-or-above the floor breaks the strictly-older invariant (rotate crash between archive and drop, restored index) — fail open",
		},
		{
			name: "archived hour interleaved inside the live span",
			live: live, archived: []time.Time{h(0, 11)},
			why: "an archived hour above the floor can outrank live rows on a DESC page",
		},
		{
			name: "archived hour above the newest live hour",
			live: live, archived: []time.Time{h(0, 20)},
			why: "archives newer than every live partition would be silently dropped by a skip",
		},
		{
			name: "no live partitions",
			live: nil, archived: []time.Time{h(2, 0)},
			why: "nothing provable — and an empty MySQLRanges plan would trip SkipMySQL past p_future rows",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := browsePlanFromHours(tt.live, tt.archived)
			if tt.wantStart.IsZero() {
				if got != nil {
					t.Fatalf("browsePlanFromHours = %+v, want nil — %s", got, tt.why)
				}
				return
			}
			if got == nil {
				t.Fatalf("browsePlanFromHours = nil, want a plan — %s", tt.why)
			}
			if len(got.GapHours) != 0 {
				t.Errorf("browse plan enumerated GapHours %v; an unbounded browse states no coverage contract", got.GapHours)
			}
			if len(got.MySQLRanges) != 1 {
				t.Fatalf("MySQLRanges = %v, want exactly one spanning range — topNSatisfiedLive requires it", got.MySQLRanges)
			}
			r := got.MySQLRanges[0]
			if !r.Start.Equal(tt.wantStart) || !r.End.Equal(tt.wantEnd) {
				t.Errorf("range = [%v, %v), want [%v, %v)", r.Start, r.End, tt.wantStart, tt.wantEnd)
			}
			if got.SkipMySQL() {
				t.Error("browse plan claims SkipMySQL; the live fetch must always run on a browse")
			}
		})
	}
}
