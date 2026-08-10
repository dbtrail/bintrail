package parquetquery

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// TestUntilUpperBoundHint_descendsWithCursor is the mirror of
// TestSinceLowerBoundHint_advancesWithCursor for newest-first paging (#1297).
//
// Without this hint every page of a paged Events view re-lists and
// re-downloads the whole window's archive files and leans on the row-level
// predicate to throw them away — quadratic S3 traffic behind a Next button.
// The CEILING is the load-bearing detail: the cursor's own hour still holds
// the events immediately below the page break, and Hive scoping is
// hour-granular, so a floored hint would prune the very file the next page
// must read.
func TestUntilUpperBoundHint_descendsWithCursor(t *testing.T) {
	until := time.Date(2026, 7, 25, 18, 0, 0, 0, time.UTC)
	cursor := time.Date(2026, 7, 25, 9, 42, 17, 0, time.UTC)
	cursorHourEnd := time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		opts query.Options
		want *time.Time
	}{
		{
			name: "no cursor keeps the plain Until",
			opts: query.Options{Until: &until},
			want: &until,
		},
		{
			name: "cursor below Until wins, ceiled to the next hour",
			opts: query.Options{Until: &until, BeforeEvent: &query.EventCursor{Timestamp: cursor, EventID: 7}},
			want: &cursorHourEnd,
		},
		{
			name: "cursor above Until does not widen the scan",
			opts: query.Options{
				Until:       &cursor,
				BeforeEvent: &query.EventCursor{Timestamp: until, EventID: 7},
			},
			want: &cursor,
		},
		{
			name: "cursor with no Until still scopes the listing",
			opts: query.Options{BeforeEvent: &query.EventCursor{Timestamp: cursor, EventID: 7}},
			want: &cursorHourEnd,
		},
		{
			name: "no bounds at all stays nil",
			opts: query.Options{},
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := untilUpperBoundHint(tc.opts)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("hint = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("hint = nil, want %v", tc.want)
			}
			if !got.Equal(*tc.want) {
				t.Errorf("hint = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestBuildFilters_beforeEventKeyset pins the archive-side composite cut. It
// has to match the live-MySQL predicate exactly: a merged read that cut the
// two tiers at different points would drop or duplicate events precisely at a
// page boundary that straddles the rotation line.
func TestBuildFilters_beforeEventKeyset(t *testing.T) {
	cur := time.Date(2026, 7, 25, 14, 37, 5, 0, time.UTC)
	where, args := buildFilters(query.Options{
		Order:       "DESC",
		BeforeEvent: &query.EventCursor{Timestamp: cur, EventID: 4242},
	}, nil)

	found := false
	for _, w := range where {
		if w == "(event_timestamp < ? OR (event_timestamp = ? AND event_id < ?))" {
			found = true
		}
	}
	if !found {
		t.Errorf("missing the composite keyset cut, so archives would re-serve rows already shown: %v", where)
	}
	if len(args) != 3 {
		t.Fatalf("args = %v, want 3 (ts, ts, event_id)", args)
	}
	if args[2] != uint64(4242) {
		t.Errorf("event_id arg = %v, want 4242", args[2])
	}
}

// TestFetch_rejectsBackwardCursorWithASC mirrors the AfterEvent+DESC guard.
// The predicate can be emitted from this package, so the direction rule is
// enforced where it is emitted, not only on the surface that sets it today.
func TestFetch_rejectsBackwardCursorWithASC(t *testing.T) {
	opts := query.Options{
		Order:       "ASC",
		BeforeEvent: &query.EventCursor{Timestamp: time.Now(), EventID: 1},
	}
	// "" as the source would fail later anyway; the guard must fire FIRST, so a
	// nil error here means the guard is gone even if the call errors for
	// another reason below.
	_, err := Fetch(context.Background(), opts, "")
	if err == nil {
		t.Fatal("Fetch accepted a backward cursor with Order=ASC")
	}
	if !strings.Contains(err.Error(), "BeforeEvent is a backward keyset cursor") {
		t.Errorf("error should name the direction rule, got: %v", err)
	}
}

// TestScopeArchiveFiles_prunesAboveTheCursor pins the WIRING, not the hint.
//
// untilUpperBoundHint can be perfectly correct and still be ignored at the
// call site, and nothing else would notice: the row-level keyset predicate
// makes the exact cut either way, so results stay identical and every
// correctness test keeps passing while each newest-first page silently
// re-downloads the whole window's archive files. This is the test that fails
// when the hint stops being threaded through.
func TestScopeArchiveFiles_prunesAboveTheCursor(t *testing.T) {
	files := []string{
		"event_date=2026-07-25/event_hour=08/events.parquet",
		"event_date=2026-07-25/event_hour=09/events.parquet",
		"event_date=2026-07-25/event_hour=10/events.parquet",
		"event_date=2026-07-25/event_hour=12/events.parquet",
		"event_date=2026-07-25/event_hour=17/events.parquet",
	}
	cursor := time.Date(2026, 7, 25, 9, 42, 17, 0, time.UTC)
	until := time.Date(2026, 7, 25, 18, 0, 0, 0, time.UTC)

	// Without a cursor the whole window is in scope.
	if got := scopeArchiveFiles(files, query.Options{Until: &until}); len(got) != len(files) {
		t.Fatalf("cursor-less scoping pruned %d of %d files", len(files)-len(got), len(files))
	}

	got := scopeArchiveFiles(files, query.Options{
		Until:       &until,
		Order:       "DESC",
		BeforeEvent: &query.EventCursor{Timestamp: cursor, EventID: 7},
	})
	// Hours 08–10 stay: 09 holds the cursor, 08 is below it, and 10 is the
	// one-hour over-inclusion the ceiling deliberately buys. Hours 12 and 17
	// are entirely above the cursor and can hold nothing this page will return.
	want := files[:3]
	if len(got) != len(want) {
		t.Fatalf("scoped files = %v, want %v (the hint is not reaching the file pruner)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("scoped[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
