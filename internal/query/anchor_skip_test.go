package query

import (
	"context"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestAnchorSatisfiedLive(t *testing.T) {
	ts := time.Date(2026, 8, 21, 20, 8, 36, 0, time.UTC)
	rows := []ResultRow{
		{EventID: 403440, EventTimestamp: ts},
		{EventID: 403445, EventTimestamp: ts},
	}

	tests := []struct {
		name string
		opts Options
		rows []ResultRow
		want bool
		why  string
	}{
		{
			name: "the anchored event came back live",
			opts: Options{EventAnchor: &EventCursor{Timestamp: ts, EventID: 403440}},
			rows: rows,
			want: true,
			why:  "event_id is the merge dedup key, so an archive can only hold a copy of it or nothing",
		},
		{
			name: "the anchored event is the other one in the same second",
			opts: Options{EventAnchor: &EventCursor{Timestamp: ts, EventID: 403445}},
			rows: rows,
			want: true,
			why:  "the anchor names an id, not a second — both events in a shared second resolve",
		},
		{
			name: "no anchor",
			opts: Options{PKValues: "42"},
			rows: rows,
			want: false,
			why:  "an unanchored request has no identity to prove anything about",
		},
		{
			name: "anchored event absent from the live page",
			opts: Options{EventAnchor: &EventCursor{Timestamp: ts, EventID: 999999}},
			rows: rows,
			want: false,
			why: "aged out into an archived partition; falling through to the archives is how it " +
				"is found, so this must NOT elide",
		},
		{
			name: "anchored event absent and the live page is empty",
			opts: Options{EventAnchor: &EventCursor{Timestamp: ts, EventID: 403440}},
			rows: nil,
			want: false,
			why:  "an empty live page proves nothing; the event may be entirely archived",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := anchorSatisfiedLive(tc.opts, tc.rows); got != tc.want {
				t.Errorf("anchorSatisfiedLive = %v, want %v — %s", got, tc.want, tc.why)
			}
		})
	}
}

// The predicate deliberately needs no QueryPlan: unlike its two siblings its
// proof is the anchor itself, not the layout of the index. Pinned because the
// obvious "consistency" edit is to add the same plan guards the others carry,
// and that would silently disable the skip on exactly the indexes this exists
// for — a nil plan is what fetchPage passes when archive coverage could not be
// evaluated, and the anchor's proof holds there anyway.
func TestAnchorSkipNeedsNoPlan(t *testing.T) {
	ts := time.Date(2026, 8, 21, 20, 8, 36, 0, time.UTC)
	opts := Options{EventAnchor: &EventCursor{Timestamp: ts, EventID: 7}}
	if !anchorSatisfiedLive(opts, []ResultRow{{EventID: 7, EventTimestamp: ts}}) {
		t.Error("anchorSatisfiedLive refused with no plan in sight. The anchor's proof does not " +
			"rest on ArchivesBelowLive or on a contiguous live range, and requiring one would " +
			"turn the skip off wherever archive coverage is unevaluable.")
	}
}

func TestFetchPageSkipsArchivesWhenTheAnchoredEventIsLive(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	live := sqlmock.NewRows([]string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	})
	anchorTS := now.Add(-2 * time.Hour)
	live.AddRow(uint64(7), "mysql-bin.000001", 100, 200, anchorTS,
		nil, nil, "shop", "orders", 3, "93921",
		nil, nil, nil, 1, nil, nil, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(live)

	var archiveCalls int
	src := mergeSources{
		archSources: []string{"s3://bucket/bintrail_id=x"},
		// Deliberately the layout the OTHER two predicates refuse: archives are
		// NOT known to sit below live and there is no contiguous range. The
		// anchor skip must fire anyway, or it collapses into perPKSatisfiedLive.
		plan: nil,
	}
	o := FetchMergedOptions{
		// The shape the console's Undo sends since #1411: one named event, no
		// per-PK cap.
		Opts: Options{
			Schema: "shop", Table: "orders", PKValues: "93921",
			EventAnchor: &EventCursor{Timestamp: anchorTS, EventID: 7},
			Limit:       1000, Order: "DESC",
		},
		AllowGaps: true,
		ArchiveFetcher: func(context.Context, Options, string) ([]ResultRow, error) {
			archiveCalls++
			return []ResultRow{{EventID: 999, PKValues: "93921", EventTimestamp: now.Add(-72 * time.Hour)}}, nil
		},
	}

	rows, _, _, _, elided, err := fetchPage(context.Background(), New(db), o, src)
	if err != nil {
		t.Fatalf("fetchPage: %v", err)
	}
	if archiveCalls != 0 {
		t.Errorf("archive fetcher called %d time(s); the request named one event and the live "+
			"index returned it, so no archive can change the result", archiveCalls)
	}
	if !elided {
		t.Error("archivesElided is false after skipping the archive sources — the surface " +
			"rendering this has no way to say the archives went unread (#1353)")
	}
	if len(rows) != 1 || rows[0].EventID != 7 {
		t.Errorf("rows = %+v, want just the anchored live event", rows)
	}
}

// The negative control the sibling predicates gained late: an anchor whose
// event is NOT live must read the archives. Without this the skip could be
// written as "anchor set ⇒ elide", which is fast, green on the test above, and
// silently returns an empty reversal for any event old enough to have been
// rotated out.
func TestFetchPageReadsArchivesWhenTheAnchoredEventIsNotLive(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	anchorTS := now.Add(-72 * time.Hour)
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}))

	var archiveCalls int
	src := mergeSources{
		archSources: []string{"s3://bucket/bintrail_id=x"},
		plan:        &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{{Start: now.Add(-24 * time.Hour), End: now.Add(time.Hour)}}},
	}
	o := FetchMergedOptions{
		Opts: Options{
			Schema: "shop", Table: "orders",
			EventAnchor: &EventCursor{Timestamp: anchorTS, EventID: 999},
			Limit:       1000, Order: "DESC",
		},
		AllowGaps: true,
		ArchiveFetcher: func(context.Context, Options, string) ([]ResultRow, error) {
			archiveCalls++
			return []ResultRow{{EventID: 999, PKValues: "93921", EventTimestamp: anchorTS}}, nil
		},
	}

	rows, _, _, _, elided, err := fetchPage(context.Background(), New(db), o, src)
	if err != nil {
		t.Fatalf("fetchPage: %v", err)
	}
	if archiveCalls != 1 {
		t.Errorf("archive fetcher called %d time(s), want 1 — the anchored event was not in the "+
			"live index, so the archives are the only place it can be found", archiveCalls)
	}
	if elided {
		t.Error("archivesElided is true on a read that DID read the archives")
	}
	if len(rows) != 1 || rows[0].EventID != 999 {
		t.Errorf("rows = %+v, want the archived event", rows)
	}
}

// The engine has to filter on the anchor, not merely accept it. Without the
// predicate the request degrades to its remaining filters — for Undo that is
// schema/table/pk plus a second-granular `until`, i.e. the whole row history —
// and the short-circuit above would then hand back that wider set as if it
// were the one event asked for. Fast and wrong.
func TestBuildQueryFiltersOnTheAnchor(t *testing.T) {
	ts := time.Date(2026, 8, 21, 20, 8, 36, 0, time.UTC)
	q, args := buildQuery(Options{
		Schema: "shop", Table: "orders",
		EventAnchor: &EventCursor{Timestamp: ts, EventID: 403440},
	})
	if !strings.Contains(q, "event_timestamp = ? AND event_id = ?") {
		t.Errorf("buildQuery emitted no anchor equality.\nSQL: %s", q)
	}
	// The partition-pruning bracket. An id-only predicate is correct and scans
	// every partition of binlog_events, which on the index this was written for
	// is the difference the anchor was supposed to make.
	if !strings.Contains(q, "TO_SECONDS(event_timestamp) >=") || !strings.Contains(q, "TO_SECONDS(event_timestamp) <") {
		t.Errorf("buildQuery emitted no hour bracket around the anchor, so MySQL prunes no "+
			"partitions and the anchored read scans the whole table.\nSQL: %s", q)
	}
	var sawID bool
	for _, a := range args {
		if id, ok := a.(uint64); ok && id == 403440 {
			sawID = true
		}
	}
	if !sawID {
		t.Errorf("the anchor's event_id never reached the bind args: %v", args)
	}
}
