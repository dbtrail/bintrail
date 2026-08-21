package query

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// pkRowsAt builds n rows for one pk_values, newest-first, a minute apart,
// with the oldest at `oldest`.
func pkRowsAt(pk string, n int, oldest time.Time) []ResultRow {
	out := make([]ResultRow, n)
	for i := range out {
		out[i] = ResultRow{
			EventID:        uint64(n - i),
			PKValues:       pk,
			EventTimestamp: oldest.Add(time.Duration(n-1-i) * time.Minute),
		}
	}
	return out
}

func TestPerPKSatisfiedLive(t *testing.T) {
	now := time.Now().UTC()
	live := hourRange(24*time.Hour, 0, now)
	inside := now.Add(-2 * time.Hour)
	below := now.Add(-30 * time.Hour) // older than the live range's start

	tests := []struct {
		name string
		opts Options
		rows []ResultRow
		plan *QueryPlan
		want bool
		why  string
	}{
		{
			name: "one named PK with its latest N already live",
			opts: Options{LimitPerPK: 1, PKValues: "42"},
			rows: pkRowsAt("42", 1, inside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: true,
			why:  "the trim keeps this row and discards every older archived one",
		},
		{
			name: "several named PKs, all satisfied",
			opts: Options{LimitPerPK: 2, PKValuesIn: []string{"1", "2"}},
			rows: append(pkRowsAt("1", 2, inside), pkRowsAt("2", 2, inside)...),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: true,
		},
		{
			name: "one PK short of its N",
			opts: Options{LimitPerPK: 2, PKValuesIn: []string{"1", "2"}},
			rows: append(pkRowsAt("1", 2, inside), pkRowsAt("2", 1, inside)...),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "the archives can still extend PK 2 — one short PK invalidates the whole skip",
		},
		{
			name: "a named PK absent from the live result",
			opts: Options{LimitPerPK: 1, PKValuesIn: []string{"1", "2"}},
			rows: pkRowsAt("1", 1, inside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "PK 2's entire history may be archived; skipping would drop the row, not trim it",
		},
		{
			name: "the empty PK name",
			opts: Options{LimitPerPK: 1, PKValuesIn: []string{""}},
			rows: pkRowsAt("", 1, inside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "merge.go buckets every empty-PK row under its own synthetic key, so the trim discards nothing there and the proof does not hold — see the #318 drift carve-out in LimitPerPK",
		},
		{
			name: "an alternate PK encoding is in play",
			opts: Options{LimitPerPK: 1, PKValues: "42", PKValuesAlt: "0x2A"},
			rows: pkRowsAt("42", 1, inside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "the same logical row can be stored under either spelling, and the trim partitions by the stored one — so its N is not well defined",
		},
		{
			name: "a different PK than the one named",
			opts: Options{LimitPerPK: 1, PKValues: "42"},
			rows: pkRowsAt("99", 3, inside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "PK 42 is absent; counting distinct keys instead of walking the named ones would let 99 stand in for it",
		},
		{
			name: "no PK named",
			opts: Options{LimitPerPK: 1},
			rows: pkRowsAt("42", 1, inside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "this is the unsound case: an archive-only pk_values is a legitimate result row, and nothing here names the set that would have to be complete",
		},
		{
			name: "no per-PK trim",
			opts: Options{PKValues: "42"},
			rows: pkRowsAt("42", 5, inside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "without the trim the archives are not discarded, they are the older half of the answer",
		},
		{
			name: "oldest kept row sits below the live range",
			opts: Options{LimitPerPK: 2, PKValues: "42"},
			rows: pkRowsAt("42", 2, below),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "the span above the kept rows is not provably live-covered",
		},
		{
			name: "two live ranges",
			opts: Options{LimitPerPK: 1, PKValues: "42"},
			rows: pkRowsAt("42", 1, inside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live, hourRange(72*time.Hour, 48*time.Hour, now)}},
			want: false,
			why:  "an archived hour could sit between them, above the kept row",
		},
		{
			name: "no plan",
			opts: Options{LimitPerPK: 1, PKValues: "42"},
			rows: pkRowsAt("42", 1, inside),
			plan: nil,
			want: false,
			why:  "no plan, no proof",
		},
		{
			name: "no live rows at all",
			opts: Options{LimitPerPK: 1, PKValues: "42"},
			rows: nil,
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "the whole history may be archived",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := perPKSatisfiedLive(tc.opts, tc.rows, tc.plan); got != tc.want {
				t.Errorf("perPKSatisfiedLive = %v, want %v — %s", got, tc.want, tc.why)
			}
		})
	}
}

// The predicate above is pure; this drives the real fetchPage, because a
// predicate that is never consulted passes every test it has. Mirrors
// TestFetchPageSkipsArchivesOnFilledTopN for the per-PK proof.
func TestFetchPageSkipsArchivesWhenEveryNamedPKIsSatisfiedLive(t *testing.T) {
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
	live.AddRow(uint64(7), "mysql-bin.000001", 100, 200, now.Add(-2*time.Hour),
		nil, nil, "shop", "orders", 3, "93921",
		nil, nil, nil, 1, nil, nil, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(live)

	var archiveCalls int
	src := mergeSources{
		archSources: []string{"s3://bucket/bintrail_id=x"},
		plan:        &QueryPlan{MySQLRanges: []TimeRange{{Start: now.Add(-24 * time.Hour), End: now.Add(time.Hour)}}},
	}
	o := FetchMergedOptions{
		// The shape the console sends for a scoped reversal: one PK, latest
		// one per row, no lower bound.
		Opts:      Options{LimitPerPK: 1, PKValues: "93921", Limit: 1000, Order: "DESC"},
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
		t.Errorf("archive fetcher called %d time(s); the named PK already had its latest event live, "+
			"so the archives could only contribute rows the per-PK trim discards", archiveCalls)
	}
	// Reported, not just skipped: a reversal that silently dropped registered
	// archives is the audit failure archivesElided exists to prevent (#1353).
	if !elided {
		t.Error("archivesElided is false after skipping the archive sources — the surface rendering " +
			"this has no way to say the archives went unread")
	}
	if len(rows) != 1 || rows[0].EventID != 7 {
		t.Errorf("rows = %+v, want just the live event", rows)
	}
}

// The other direction, and the one that must never be traded for speed: a
// named PK with nothing live has its whole history in the archives, so the
// skip is refused and the fetcher runs.
func TestFetchPageReadsArchivesWhenANamedPKHasNothingLive(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	empty := sqlmock.NewRows([]string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	})
	mock.ExpectQuery("SELECT").WillReturnRows(empty)

	var archiveCalls int
	src := mergeSources{
		archSources: []string{"s3://bucket/bintrail_id=x"},
		plan:        &QueryPlan{MySQLRanges: []TimeRange{{Start: now.Add(-24 * time.Hour), End: now.Add(time.Hour)}}},
	}
	o := FetchMergedOptions{
		Opts:      Options{LimitPerPK: 1, PKValues: "93921", Limit: 1000, Order: "DESC"},
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
	if archiveCalls != 1 {
		t.Errorf("archive fetcher called %d time(s), want 1 — the row's entire history is archived", archiveCalls)
	}
	if elided {
		t.Error("archivesElided is true on a path that read the archives")
	}
	if len(rows) != 1 || rows[0].EventID != 999 {
		t.Errorf("rows = %+v, want the archived event", rows)
	}
}

// The premise behind the empty-name refusal, asserted against the real trim
// rather than taken from its comment. If LimitPerPK ever stops carving empty
// PKs out, the refusal becomes needlessly conservative and this says so.
func TestLimitPerPKDoesNotTrimEmptyPKs(t *testing.T) {
	now := time.Now().UTC()
	rows := []ResultRow{
		{EventID: 1, PKValues: "", EventTimestamp: now.Add(-72 * time.Hour)},
		{EventID: 2, PKValues: "", EventTimestamp: now.Add(-71 * time.Hour)},
		{EventID: 3, PKValues: "", EventTimestamp: now.Add(-2 * time.Hour)},
	}
	if got := LimitPerPK(rows, 1); len(got) != 3 {
		t.Fatalf("LimitPerPK(empty PKs, 1) kept %d rows, want all 3 — the drift carve-out is what "+
			"makes perPKSatisfiedLive refuse the empty name; if it is gone, revisit that refusal", len(got))
	}
	// And the contrast, so the test is about the carve-out and not about
	// LimitPerPK being a no-op.
	named := []ResultRow{
		{EventID: 1, PKValues: "42", EventTimestamp: now.Add(-72 * time.Hour)},
		{EventID: 2, PKValues: "42", EventTimestamp: now.Add(-2 * time.Hour)},
	}
	if got := LimitPerPK(named, 1); len(got) != 1 {
		t.Fatalf("LimitPerPK(named PK, 1) kept %d rows, want 1", len(got))
	}
}
