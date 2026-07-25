package query

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// streamBase is a fixed instant so the fixtures below carry real, ordered
// timestamps — a keyset cursor is meaningless against zero-valued ones.
var streamBase = time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)

// streamRows builds n events one second apart with ascending event_ids, the
// shape MergeResults sorts into.
func streamRows(n int) []ResultRow {
	rows := make([]ResultRow, n)
	for i := range rows {
		rows[i] = ResultRow{
			EventID:        uint64(i + 1),
			EventTimestamp: streamBase.Add(time.Duration(i) * time.Second),
			SchemaName:     "mydb",
			TableName:      "orders",
			PKValues:       string(rune('a' + i)),
		}
	}
	return rows
}

// pageOf applies the keyset cursor and page limit the way a real source's SQL
// would, so the stub fetcher below behaves like MySQL/DuckDB rather than
// silently ignoring the pagination it is meant to exercise.
func pageOf(rows []ResultRow, opts Options) []ResultRow {
	var out []ResultRow
	for _, r := range rows {
		if opts.AfterEvent != nil {
			c := EventCursor{Timestamp: r.EventTimestamp, EventID: r.EventID}
			if !c.After(*opts.AfterEvent) {
				continue
			}
		}
		out = append(out, r)
		if opts.Limit > 0 && len(out) >= opts.Limit {
			break
		}
	}
	return out
}

// streamMockDB wires sqlmock to feed ResolveArchiveSources one local archive
// source (whose base dir must exist — the resolver os.Stat's it) and
// engine.Fetch zero live rows, liveFetches times. No DBName and no time range
// keeps the planner out of the way, as validate() allows.
func streamMockDB(t *testing.T, liveFetches int) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	base := filepath.Join(t.TempDir(), "bintrail_id=one")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("one", filepath.Join(base, "events.parquet"), nil, nil))
	for range liveFetches {
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
	}
	return db, mock
}

// TestFetchMergedStream_pagesWholeWindowExactlyOnce is the core contract: the
// window is delivered in ascending order, every event exactly once, across
// several pages — and the expensive prologue (archive discovery + planning)
// runs ONCE for the whole stream, not once per page. The sqlmock expectations
// are what pin the second half: exactly one archive_state query is armed, so a
// regression that re-resolved sources per page fails on an unexpected query.
func TestFetchMergedStream_pagesWholeWindowExactlyOnce(t *testing.T) {
	const total, batch = 5, 2
	// Pages of 2, 2, 1 — the last is short, which ends the stream without an
	// extra probe round trip.
	const wantPages = 3

	db, mock := streamMockDB(t, wantPages)
	fixture := streamRows(total)

	var seenCursors []*EventCursor
	fetcher := func(_ context.Context, opts Options, _ string) ([]ResultRow, error) {
		seenCursors = append(seenCursors, opts.AfterEvent)
		if opts.Limit != batch {
			t.Errorf("archive fetch got Limit=%d, want the page size %d", opts.Limit, batch)
		}
		return pageOf(fixture, opts), nil
	}

	var got []ResultRow
	pages := 0
	_, err := FetchMergedStream(context.Background(), db, New(db), FetchMergedOptions{
		AllowGaps:      false,
		ArchiveFetcher: fetcher,
	}, batch, func(page []ResultRow) error {
		pages++
		got = append(got, page...)
		return nil
	})
	if err != nil {
		t.Fatalf("FetchMergedStream: %v", err)
	}

	if pages != wantPages {
		t.Errorf("delivered %d pages, want %d", pages, wantPages)
	}
	if len(got) != total {
		t.Fatalf("delivered %d events, want %d (every event exactly once)", len(got), total)
	}
	for i, r := range got {
		if r.EventID != uint64(i+1) {
			t.Errorf("event %d has id %d, want %d — the stream must preserve ascending order", i, r.EventID, i+1)
		}
	}

	// The first page must be fetched with no cursor; every later page must
	// resume strictly after the previous page's last row.
	if seenCursors[0] != nil {
		t.Error("first page was fetched with a cursor; it must start at the window's beginning")
	}
	for i := 1; i < len(seenCursors); i++ {
		if seenCursors[i] == nil {
			t.Fatalf("page %d was fetched without a cursor", i)
		}
		wantID := uint64(i * batch)
		if seenCursors[i].EventID != wantID {
			t.Errorf("page %d resumed at event_id %d, want %d (the previous page's last row)",
				i, seenCursors[i].EventID, wantID)
		}
	}

	// Exactly one archive_state query was armed: a regression that re-resolved
	// sources (or re-ran the planner) per page would fail here, not silently
	// cost N times the prologue.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestFetchMergedStream_keepsPagingAfterASkippedSource pins the one case where
// a short page does NOT mean end-of-stream. Under AllowGaps a failing archive
// source is skipped with a warning, which shortens the page for a reason that
// has nothing to do with exhaustion — stopping there would silently drop the
// remaining events of every OTHER source too.
func TestFetchMergedStream_keepsPagingAfterASkippedSource(t *testing.T) {
	dir := t.TempDir()
	okBase := filepath.Join(dir, "bintrail_id=ok")
	brokenBase := filepath.Join(dir, "bintrail_id=broken")
	for _, d := range []string{okBase, brokenBase} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("ok", filepath.Join(okBase, "events.parquet"), nil, nil).
			AddRow("broken", filepath.Join(brokenBase, "events.parquet"), nil, nil))
	// Two live fetches: the short first page must NOT end the stream, so a
	// second round trip is expected before the empty page stops it.
	for range 2 {
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
	}

	fixture := streamRows(3)
	fetcher := func(_ context.Context, opts Options, src string) ([]ResultRow, error) {
		if strings.Contains(src, "broken") {
			return nil, errors.New("stub: broken archive (intentional)")
		}
		return pageOf(fixture, opts), nil
	}

	pages := 0
	total := 0
	if _, err := FetchMergedStream(context.Background(), db, New(db), FetchMergedOptions{
		AllowGaps:      true, // required for the skip to be non-fatal
		ArchiveFetcher: fetcher,
	}, 5, func(page []ResultRow) error {
		pages++
		total += len(page)
		return nil
	}); err != nil {
		t.Fatalf("FetchMergedStream: %v", err)
	}

	if pages != 1 || total != 3 {
		t.Errorf("delivered %d pages / %d events, want 1 / 3", pages, total)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations — the stream stopped at the short page instead of probing again: %v", err)
	}
}

// TestFetchMergedStream_refusesNonAdvancingCursor covers the failure mode that
// would otherwise be an infinite loop with no output: a source that ignores the
// keyset predicate and keeps returning the same rows. The stream must fail
// loudly instead of folding the same events forever.
func TestFetchMergedStream_refusesNonAdvancingCursor(t *testing.T) {
	db, _ := streamMockDB(t, 2)
	fixture := streamRows(2)

	// Deliberately ignores opts.AfterEvent — the regression this guards.
	stuck := func(_ context.Context, opts Options, _ string) ([]ResultRow, error) {
		return pageOf(fixture, Options{Limit: opts.Limit}), nil
	}

	_, err := FetchMergedStream(context.Background(), db, New(db), FetchMergedOptions{
		AllowGaps:      false,
		ArchiveFetcher: stuck,
	}, 2, func([]ResultRow) error { return nil })
	if err == nil {
		t.Fatal("expected a loud error when the cursor stops advancing, got nil (would loop forever)")
	}
	if !strings.Contains(err.Error(), "cursor did not advance") {
		t.Errorf("error should name the non-advancing cursor, got: %v", err)
	}
}

// TestFetchMergedStream_rejectsIncompatibleOptions pins the combinations that
// would silently return a DIFFERENT result set than FetchMerged rather than
// fail — each is refused before any DB work.
func TestFetchMergedStream_rejectsIncompatibleOptions(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cases := []struct {
		name    string
		opts    Options
		wantErr string
	}{
		{"descending order", Options{Order: "DESC"}, "ascending order only"},
		{"global limit", Options{Limit: 10}, "cannot be combined with paging"},
		{"per-PK limit", Options{LimitPerPK: 3}, "cannot be combined with paging"},
		{"preset cursor", Options{AfterEvent: &EventCursor{Timestamp: streamBase, EventID: 1}}, "must not be preset"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := FetchMergedStream(context.Background(), db, New(db), FetchMergedOptions{
				Opts:      tc.opts,
				NoArchive: true,
				AllowGaps: true,
			}, 10, func([]ResultRow) error {
				t.Error("fn must not be called for a rejected option set")
				return nil
			})
			if err == nil {
				t.Fatalf("expected %s to be rejected", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error should explain the refusal (%q), got: %v", tc.wantErr, err)
			}
		})
	}
}

// TestFetchMergedStream_propagatesCallbackError pins that a caller aborting
// mid-stream (a guard refusing an event, say) stops the walk and surfaces its
// own error unchanged, rather than being wrapped or swallowed.
func TestFetchMergedStream_propagatesCallbackError(t *testing.T) {
	db, _ := streamMockDB(t, 1)
	fixture := streamRows(4)
	sentinel := errors.New("caller refused this page")

	pages := 0
	_, err := FetchMergedStream(context.Background(), db, New(db), FetchMergedOptions{
		AllowGaps:      false,
		ArchiveFetcher: func(_ context.Context, o Options, _ string) ([]ResultRow, error) { return pageOf(fixture, o), nil },
	}, 2, func([]ResultRow) error {
		pages++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("callback error must propagate unchanged, got: %v", err)
	}
	if pages != 1 {
		t.Errorf("walk continued after the callback failed: %d pages", pages)
	}
}
