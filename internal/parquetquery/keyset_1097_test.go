package parquetquery

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/archive"
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
	// 05:30 floors to 05:00, i.e. exactly the widened bound — so this pins that
	// the margin is not LOST, and nothing more.
	inside := time.Date(2026, 7, 25, 5, 30, 0, 0, time.UTC)
	opts.AfterEvent = &query.EventCursor{Timestamp: inside, EventID: 1}
	if got := sinceLowerBoundHint(opts); got == nil || !got.Equal(widened) {
		t.Errorf("a cursor inside the #797 margin must not tighten the floor: got %v, want %v", got, widened)
	}

	// The case that actually discriminates: once the sweep has moved PAST the
	// margin, the cursor must win. Correct because every unreturned row sorts
	// at-or-after the cursor, so no row the margin was protecting is still
	// pending. A "fix" that made the SincePos margin unconditionally win would
	// keep re-listing the whole window on every page and would pass the
	// assertion above — this one catches it.
	beyond := time.Date(2026, 7, 25, 6, 15, 0, 0, time.UTC)
	beyondHour := time.Date(2026, 7, 25, 6, 0, 0, 0, time.UTC)
	opts.AfterEvent = &query.EventCursor{Timestamp: beyond, EventID: 9}
	if got := sinceLowerBoundHint(opts); got == nil || !got.Equal(beyondHour) {
		t.Errorf("a cursor past the #797 margin must tighten the floor: got %v, want %v", got, beyondHour)
	}
}

// TestFetch_afterEventKeysetPagesIdentically executes the DuckDB keyset
// predicate against a real Parquet file (#1097). Everything else in the suite
// asserts the SQL is *built* correctly; this asserts DuckDB *evaluates* it
// correctly — the timestamp binding, and above all the tie handling.
//
// The fixture deliberately puts four events in the SAME second. That is the
// case a timestamp-only cursor gets wrong in one of two silent ways: it either
// re-returns the whole second on the next page (duplicates) or skips its
// remainder (data loss). Paging through it in pages of two and comparing
// against the unpaged read is what proves the composite (timestamp, event_id)
// cut is exact.
func TestFetch_afterEventKeysetPagesIdentically(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	mkRow := func(id, ts, pk string) [2][]string {
		return [2][]string{
			{id, "mysql-bin.000001", "100", "200", ts, "", "", "mydb", "orders", "1", pk, "", "", `{"id":` + pk + `}`, "0", "", ""},
			{"0", "0", "0", "0", "0", "1", "1", "0", "0", "0", "0", "1", "1", "0", "0", "1", "1"},
		}
	}
	// ids 2..5 share one second; 1 and 6 bracket it.
	dir := writeArchiveFixture(t, archive.BinlogEventColumns, [][2][]string{
		mkRow("1", "2026-02-19 14:00:00", "1"),
		mkRow("2", "2026-02-19 14:00:01", "2"),
		mkRow("3", "2026-02-19 14:00:01", "3"),
		mkRow("4", "2026-02-19 14:00:01", "4"),
		mkRow("5", "2026-02-19 14:00:01", "5"),
		mkRow("6", "2026-02-19 14:00:02", "6"),
	})
	base := query.Options{Schema: "mydb", Table: "orders"}

	unpaged, err := Fetch(context.Background(), base, dir)
	if err != nil {
		t.Fatalf("unpaged Fetch: %v", err)
	}
	if len(unpaged) != 6 {
		t.Fatalf("fixture read back %d rows, want 6", len(unpaged))
	}

	var paged []query.ResultRow
	var cursor *query.EventCursor
	for range 10 { // bounded so a non-advancing cursor fails the test, not the CI job
		opts := base
		opts.Limit = 2
		opts.AfterEvent = cursor
		page, ferr := Fetch(context.Background(), opts, dir)
		if ferr != nil {
			t.Fatalf("paged Fetch: %v", ferr)
		}
		if len(page) == 0 {
			break
		}
		paged = append(paged, page...)
		last := page[len(page)-1]
		cursor = &query.EventCursor{Timestamp: last.EventTimestamp, EventID: last.EventID}
	}

	if len(paged) != len(unpaged) {
		t.Fatalf("paged read %d rows, unpaged read %d — the keyset cut duplicated or dropped rows",
			len(paged), len(unpaged))
	}
	for i := range unpaged {
		if paged[i].EventID != unpaged[i].EventID {
			t.Fatalf("row %d: paged event_id %d, unpaged %d — paging changed the result",
				i, paged[i].EventID, unpaged[i].EventID)
		}
	}
}
