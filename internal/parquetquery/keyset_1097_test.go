package parquetquery

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// TestSinceLowerBoundHint_advancesWithCursor is the performance contract that
// makes paginated archive reads viable (#1097).
//
// The row-level keyset predicate alone would be correct but ruinous: archive
// files are listed, downloaded and scanned per source, so a cursor the FILE
// scoping ignores means every page re-lists and re-downloads the whole window
// and throws away everything before the cursor. Advancing the lower-bound hint
// with the cursor is what turns that into a forward sweep.
func TestSinceLowerBoundHint_advancesWithCursor(t *testing.T) {
	since := time.Date(2026, 7, 25, 3, 0, 0, 0, time.UTC)
	cursor := time.Date(2026, 7, 25, 9, 42, 17, 0, time.UTC)
	cursorHour := time.Date(2026, 7, 25, 9, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		opts query.Options
		want *time.Time
	}{
		{
			name: "no cursor keeps the plain Since",
			opts: query.Options{Since: &since},
			want: &since,
		},
		{
			name: "cursor ahead of Since wins, floored to the hour",
			opts: query.Options{Since: &since, AfterEvent: &query.EventCursor{Timestamp: cursor, EventID: 7}},
			want: &cursorHour,
		},
		{
			name: "cursor behind Since does not widen the scan",
			opts: query.Options{
				Since:      &cursor,
				AfterEvent: &query.EventCursor{Timestamp: since, EventID: 7},
			},
			want: &cursor,
		},
		{
			name: "cursor with no Since still scopes the listing",
			opts: query.Options{AfterEvent: &query.EventCursor{Timestamp: cursor, EventID: 7}},
			want: &cursorHour,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sinceLowerBoundHint(tc.opts)
			if got == nil {
				t.Fatalf("hint = nil, want %v", tc.want)
			}
			if !got.Equal(*tc.want) {
				t.Errorf("hint = %v, want %v", got.UTC(), tc.want.UTC())
			}
		})
	}
}

// TestSinceLowerBoundHint_cursorNeverOverridesSincePosMargin pins the #797
// interaction: when SincePos anchors the window, Since is widened by a full
// extra hour because event_timestamp is EXECUTION time and can file a
// position-later event under an earlier hour. A cursor may only tighten past
// that margin once the sweep has actually moved beyond it — never on the first
// page, where the widened bound is still the correct floor.
func TestSinceLowerBoundHint_cursorNeverOverridesSincePosMargin(t *testing.T) {
	since := time.Date(2026, 7, 25, 6, 30, 0, 0, time.UTC)
	widened := time.Date(2026, 7, 25, 5, 0, 0, 0, time.UTC) // hour-truncated, minus one hour

	opts := query.Options{Since: &since, SincePos: &query.BinlogPos{File: "mysql-bin.000009", Pos: 120}}
	if got := sinceLowerBoundHint(opts); got == nil || !got.Equal(widened) {
		t.Fatalf("without a cursor the #797 margin must stand: got %v, want %v", got, widened)
	}

	// A cursor still inside the margin must not pull the floor forward past it.
	inside := time.Date(2026, 7, 25, 5, 30, 0, 0, time.UTC)
	opts.AfterEvent = &query.EventCursor{Timestamp: inside, EventID: 1}
	if got := sinceLowerBoundHint(opts); got == nil || !got.Equal(widened) {
		t.Errorf("a cursor inside the #797 margin must not tighten the floor: got %v, want %v", got, widened)
	}
}
