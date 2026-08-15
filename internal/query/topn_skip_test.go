package query

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func hourRange(startAgo, endAgo time.Duration, now time.Time) TimeRange {
	return TimeRange{Start: now.Add(-startAgo), End: now.Add(-endAgo)}
}

// rowsAt builds n rows newest-first, one minute apart, ending at `oldest`.
func rowsAt(n int, oldest time.Time) []ResultRow {
	out := make([]ResultRow, n)
	for i := range out {
		out[i] = ResultRow{EventID: uint64(n - i), EventTimestamp: oldest.Add(time.Duration(n-1-i) * time.Minute)}
	}
	return out
}

func TestTopNSatisfiedLive(t *testing.T) {
	now := time.Now().UTC()
	live := hourRange(24*time.Hour, 0, now) // live covers the last 24h
	cutoffInside := now.Add(-2 * time.Hour)

	tests := []struct {
		name string
		opts Options
		rows []ResultRow
		plan *QueryPlan
		want bool
		why  string
	}{
		{
			name: "filled DESC page inside one live range",
			opts: Options{Limit: 100, Order: "DESC"},
			rows: rowsAt(100, cutoffInside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: true,
			why:  "the archives are all below the cutoff and cannot survive the limit",
		},
		{
			name: "short page",
			opts: Options{Limit: 100, Order: "DESC"},
			rows: rowsAt(40, cutoffInside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "live did not fill the limit, so the archives genuinely extend the result",
		},
		{
			name: "ASC asks for the oldest rows",
			opts: Options{Limit: 100, Order: "ASC"},
			rows: rowsAt(100, cutoffInside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "oldest-first is exactly where the archives live",
		},
		{
			name: "empty order defaults to ASC",
			opts: Options{Limit: 100},
			rows: rowsAt(100, cutoffInside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "OrderDirection defaults to ASC, which must not take the skip",
		},
		{
			name: "no limit",
			opts: Options{Order: "DESC"},
			rows: rowsAt(100, cutoffInside),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "without a limit every archived row is still wanted",
		},
		{
			name: "interleaved live coverage",
			opts: Options{Limit: 100, Order: "DESC"},
			rows: rowsAt(100, now.Add(-200*time.Hour)),
			plan: &QueryPlan{MySQLRanges: []TimeRange{
				hourRange(300*time.Hour, 250*time.Hour, now),
				hourRange(24*time.Hour, 0, now),
			}},
			want: false,
			why:  "two live ranges mean an archived hour can sit ABOVE the cutoff",
		},
		{
			name: "cutoff below the live range",
			opts: Options{Limit: 100, Order: "DESC"},
			rows: rowsAt(100, now.Add(-48*time.Hour)),
			plan: &QueryPlan{MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "the page reaches below live coverage, so the span above it is not provably live",
		},
		{
			name: "no plan",
			opts: Options{Limit: 100, Order: "DESC"},
			rows: rowsAt(100, cutoffInside),
			plan: nil,
			want: false,
			why:  "no plan, no proof",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := topNSatisfiedLive(tt.opts, tt.rows, tt.plan); got != tt.want {
				t.Errorf("topNSatisfiedLive = %v, want %v — %s", got, tt.want, tt.why)
			}
		})
	}
}

// The wiring, not just the predicate: a filled newest-first page must leave the
// archive fetcher UNCALLED. Deleting the skip from fetchPage has to fail here,
// or the predicate above is decorative.
func TestFetchPageSkipsArchivesOnFilledTopN(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const limit = 3
	// One statement: a wide SELECT joined against a narrow keys subquery (the
	// late-materialisation shape), so the mock returns the wide column list.
	live := sqlmock.NewRows([]string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	})
	for i := range limit {
		live.AddRow(uint64(limit-i), "mysql-bin.000001", 100, 200, now.Add(-time.Duration(i)*time.Minute),
			nil, nil, "shop", "orders", 2, "1",
			nil, nil, nil, 1, nil, nil,
			nil)
	}
	mock.ExpectQuery("SELECT").WillReturnRows(live)

	var archiveCalls int
	src := mergeSources{
		archSources: []string{"s3://bucket/bintrail_id=x"},
		plan:        &QueryPlan{MySQLRanges: []TimeRange{{Start: now.Add(-24 * time.Hour), End: now.Add(time.Hour)}}},
	}
	o := FetchMergedOptions{
		Opts:      Options{Limit: limit, Order: "DESC"},
		AllowGaps: true,
		ArchiveFetcher: func(context.Context, Options, string) ([]ResultRow, error) {
			archiveCalls++
			return []ResultRow{{EventID: 999, EventTimestamp: now.Add(-72 * time.Hour)}}, nil
		},
	}

	rows, _, _, _, _, err := fetchPage(context.Background(), New(db), o, src)
	if err != nil {
		t.Fatalf("fetchPage: %v", err)
	}
	if archiveCalls != 0 {
		t.Errorf("archive fetcher called %d time(s); a filled newest-first page must not touch S3", archiveCalls)
	}
	if len(rows) != limit {
		t.Errorf("rows = %d, want %d", len(rows), limit)
	}
	for _, r := range rows {
		if r.EventID == 999 {
			t.Error("an archived row survived a page the live half already filled")
		}
	}
}

var _ = sql.ErrNoRows
