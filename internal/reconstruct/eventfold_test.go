package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// archiveFixtureDir returns a path shaped like an archive_state sample_local
// entry whose base directory exists, so ResolveArchiveSources keeps it.
func archiveFixtureDir(t *testing.T) string {
	t.Helper()
	base := filepath.Join(t.TempDir(), "bintrail_id=one")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	return filepath.Join(base, "events.parquet")
}

// foldBase is a fixed instant so the fixtures carry ordered timestamps.
var foldBase = time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC)

func foldEvent(id int, pk string, et event.EventType) query.ResultRow {
	return query.ResultRow{
		EventID:        uint64(id),
		EventTimestamp: foldBase.Add(time.Duration(id) * time.Second),
		SchemaName:     "mydb",
		TableName:      "orders",
		PKValues:       pk,
		EventType:      et,
		RowAfter:       map[string]any{"id": pk, "status": "s"},
	}
}

// TestFoldEventWindow_pagedOrchestration pins the four things foldEventWindow
// does around the fold, none of which had any coverage: build the decode state
// ONCE for the whole window, capture only the FIRST event, accumulate the total
// across pages, and latch the volume warning.
//
// Each is a silent failure if it regresses — correct output, wrong cost or a
// missing signal — so none of them would be caught by an output assertion.
func TestFoldEventWindow_pagedOrchestration(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Exactly ONE schema_snapshots read is armed. Moving newEventDecoder inside
	// the page loop — the regression the eventDecoder extraction exists to
	// prevent — issues one per page and fails here on an unexpected query.
	// It is silent otherwise: the output stays correct, the cost does not.
	mock.ExpectQuery("FROM schema_snapshots").WillReturnRows(
		sqlmock.NewRows([]string{"snapshot_id", "MIN(snapshot_time)"}))
	// One archive source (its base dir must exist — ResolveArchiveSources
	// os.Stat's it). engine.Fetch is stubbed empty, so every event arrives
	// through the injected fetcher below.
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("one", archiveFixtureDir(t), nil, nil))
	for range 3 {
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
	}

	fixture := []query.ResultRow{
		foldEvent(1, "1", event.EventInsert),
		foldEvent(2, "2", event.EventInsert),
		foldEvent(3, "1", event.EventUpdate), // same PK as event 1, a later page
		foldEvent(4, "3", event.EventInsert),
		foldEvent(5, "2", event.EventDelete), // same PK as event 2, a later page
	}
	fetcher := func(_ context.Context, opts query.Options, _ string) ([]query.ResultRow, error) {
		var out []query.ResultRow
		for _, r := range fixture {
			if opts.AfterEvent != nil {
				c := query.EventCursor{Timestamp: r.EventTimestamp, EventID: r.EventID}
				if !c.After(*opts.AfterEvent) {
					continue
				}
			}
			out = append(out, r)
			if opts.Limit > 0 && len(out) >= opts.Limit {
				break
			}
		}
		return out, nil
	}

	res, err := foldEventWindow(context.Background(), foldConfig{
		DB:        db,
		Engine:    query.New(db),
		Schema:    "mydb",
		Table:     "orders",
		PKCols:    pkColsIntID(),
		Opts:      query.Options{Schema: "mydb", Table: "orders"},
		AllowGaps: true,
		// A real fetcher is required: FetchMergedOptions.validate rejects nil
		// unless NoArchive is set. foldConfig does NOT default it.
		ArchiveFetcher: fetcher,
		BatchSize:      2,
	})
	if err != nil {
		t.Fatalf("foldEventWindow: %v", err)
	}

	if res.Total != int64(len(fixture)) {
		t.Errorf("Total = %d, want %d — the count must accumulate across pages, not reset per page",
			res.Total, len(fixture))
	}
	if res.First == nil || res.First.EventID != 1 {
		t.Errorf("First = %v, want the window's FIRST event — WarnBaselineFirstEventGap compares the "+
			"baseline anchor against it, so a later page's first event would hide a real gap", res.First)
	}
	// 3 distinct PKs, each holding its LAST event across page boundaries.
	if len(res.Changes) != 3 {
		t.Fatalf("Changes has %d entries, want 3 (one per distinct touched PK)", len(res.Changes))
	}
	if got := res.Changes["1"]; got == nil || got.EventID != 3 {
		t.Errorf("pk 1 = %v, want event 3 — a later page must win", got)
	}
	if got := res.Changes["2"]; got == nil || got.EventType != event.EventDelete {
		t.Errorf("pk 2 = %v, want the DELETE from the later page", got)
	}
	if !res.SawImage {
		t.Error("SawImage is false after folding events that carry images")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations — decode state was rebuilt per page: %v", err)
	}
}

// TestFoldEventWindow_volumeWarningLatches pins that the #654/#842 warning
// fires ONCE per table, not once per page. Unlatched it floods the log at
// exactly the moment the operator needs to read it; never firing loses the only
// signal that a window is large enough to matter.
func TestFoldEventWindow_volumeWarningLatches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM schema_snapshots").WillReturnRows(
		sqlmock.NewRows([]string{"snapshot_id", "MIN(snapshot_time)"}))
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("one", archiveFixtureDir(t), nil, nil))
	for range 4 {
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
	}

	var fixture []query.ResultRow
	for i := 1; i <= 6; i++ {
		fixture = append(fixture, foldEvent(i, string(rune('0'+i)), event.EventInsert))
	}
	fetcher := func(_ context.Context, opts query.Options, _ string) ([]query.ResultRow, error) {
		var out []query.ResultRow
		for _, r := range fixture {
			if opts.AfterEvent != nil {
				c := query.EventCursor{Timestamp: r.EventTimestamp, EventID: r.EventID}
				if !c.After(*opts.AfterEvent) {
					continue
				}
			}
			out = append(out, r)
			if opts.Limit > 0 && len(out) >= opts.Limit {
				break
			}
		}
		return out, nil
	}

	logs := captureWarns(t)
	if _, err := foldEventWindow(context.Background(), foldConfig{
		DB:                 db,
		Engine:             query.New(db),
		Schema:             "mydb",
		Table:              "orders",
		PKCols:             pkColsIntID(),
		Opts:               query.Options{Schema: "mydb", Table: "orders"},
		AllowGaps:          true,
		ArchiveFetcher:     fetcher,
		BatchSize:          2,
		WarnEventThreshold: 1, // crossed on the very first page
		Parallelism:        1,
	}); err != nil {
		t.Fatalf("foldEventWindow: %v", err)
	}

	if n := strings.Count(logs.String(), "very large event window"); n != 1 {
		t.Errorf("volume warning emitted %d times across 3 pages, want exactly 1", n)
	}
}
