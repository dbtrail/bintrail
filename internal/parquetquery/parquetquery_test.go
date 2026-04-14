package parquetquery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// ─── buildGlob (local paths only — S3 uses listS3Parquet) ───────────────────

func TestBuildGlob(t *testing.T) {
	tests := []struct {
		source string
		want   string
	}{
		{"/data/archives", "/data/archives/**/*.parquet"},
		{"/data/archives/", "/data/archives/**/*.parquet"},
		{"/data/archives/bintrail_id=abc-123", "/data/archives/bintrail_id=abc-123/**/*.parquet"},
		{"/data/archives/events_14.parquet", "/data/archives/events_14.parquet"},
	}
	for _, tc := range tests {
		got := buildGlob(tc.source)
		if got != tc.want {
			t.Errorf("buildGlob(%q) = %q, want %q", tc.source, got, tc.want)
		}
	}
}

// ─── parseS3Source ───────────────────────────────────────────────────────────

func TestParseS3Source(t *testing.T) {
	tests := []struct {
		source     string
		wantBucket string
		wantPrefix string
		wantErr    bool
	}{
		{"s3://my-bucket/events/bintrail_id=abc/", "my-bucket", "events/bintrail_id=abc/", false},
		{"s3://my-bucket/events/bintrail_id=abc", "my-bucket", "events/bintrail_id=abc/", false},
		{"s3://my-bucket/", "my-bucket", "", false},
		{"s3://my-bucket", "my-bucket", "", false},
		{"s3:///prefix", "", "", true},
	}
	for _, tc := range tests {
		bucket, prefix, err := parseS3Source(tc.source)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseS3Source(%q) error = %v, wantErr %v", tc.source, err, tc.wantErr)
			continue
		}
		if err != nil {
			continue
		}
		if bucket != tc.wantBucket {
			t.Errorf("parseS3Source(%q) bucket = %q, want %q", tc.source, bucket, tc.wantBucket)
		}
		if prefix != tc.wantPrefix {
			t.Errorf("parseS3Source(%q) prefix = %q, want %q", tc.source, prefix, tc.wantPrefix)
		}
	}
}

// ─── buildQueryFromFiles ────────────────────────────────────────────────────

func TestBuildQueryFromFiles(t *testing.T) {
	files := []string{
		"s3://bucket/events/bintrail_id=abc/event_date=2026-03-09/event_hour=11/events.parquet",
		"s3://bucket/events/bintrail_id=abc/event_date=2026-03-09/event_hour=12/events.parquet",
	}
	q, args := buildQueryFromFiles(files, query.Options{Limit: 50})
	assertContains(t, q, "FROM parquet_scan([")
	assertContains(t, q, "hive_partitioning=true, union_by_name=true)")
	assertContains(t, q, "event_hour=11/events.parquet")
	assertContains(t, q, "event_hour=12/events.parquet")
	assertContains(t, q, "ORDER BY event_timestamp, event_id")
	assertContains(t, q, "LIMIT ?")
	if len(args) != 1 || args[0] != 50 {
		t.Errorf("expected [50] args, got %v", args)
	}
}

func TestBuildQueryFromFilesEscaping(t *testing.T) {
	files := []string{"s3://bucket/it's/file.parquet"}
	q, _ := buildQueryFromFiles(files, query.Options{})
	assertContains(t, q, "it''s")
}

func TestBuildQueryFromFilesWithFilters(t *testing.T) {
	files := []string{"s3://bucket/f.parquet"}
	since := time.Date(2026, 3, 9, 11, 0, 0, 0, time.UTC)
	opts := query.Options{Schema: "mydb", Table: "orders", Since: &since, Limit: 10}
	q, args := buildQueryFromFiles(files, opts)
	assertContains(t, q, "schema_name = ?")
	assertContains(t, q, "table_name = ?")
	assertContains(t, q, "event_timestamp >= ?")
	// schema, table, since, limit
	if len(args) != 4 {
		t.Errorf("expected 4 args, got %d: %v", len(args), args)
	}
}

// ─── buildQuery (local glob path) ───────────────────────────────────────────

func assertContains(t *testing.T, s, want string) {
	t.Helper()
	if !strings.Contains(s, want) {
		t.Errorf("expected SQL to contain %q\ngot: %s", want, s)
	}
}

func TestBuildQueryNoFilters(t *testing.T) {
	q, args := buildQuery("/archives/*.parquet", query.Options{Limit: 50})
	assertContains(t, q, "FROM parquet_scan('/archives/*.parquet', hive_partitioning=true, union_by_name=true)")
	assertContains(t, q, "ORDER BY event_timestamp, event_id")
	assertContains(t, q, "LIMIT ?")
	if len(args) != 1 || args[0] != 50 {
		t.Errorf("expected [50] args, got %v", args)
	}
}

func TestBuildQueryViaGlob(t *testing.T) {
	glob := buildGlob("/archives/bintrail_id=abc-123")
	q, args := buildQuery(glob, query.Options{Limit: 50})
	assertContains(t, q, "/archives/bintrail_id=abc-123/**/*.parquet")
	assertContains(t, q, "LIMIT ?")
	if len(args) != 1 || args[0] != 50 {
		t.Errorf("expected [50] args, got %v", args)
	}
}

func TestBuildQuerySchemaTable(t *testing.T) {
	opts := query.Options{Schema: "mydb", Table: "orders", Limit: 10}
	q, args := buildQuery("/arc/*.parquet", opts)
	assertContains(t, q, "schema_name = ?")
	assertContains(t, q, "table_name = ?")
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d: %v", len(args), args)
	}
	if args[0] != "mydb" || args[1] != "orders" {
		t.Errorf("unexpected args: %v", args)
	}
}

func TestBuildQueryPK(t *testing.T) {
	opts := query.Options{PKValues: "12345", Limit: 100}
	q, args := buildQuery("/arc/*.parquet", opts)
	assertContains(t, q, "pk_values = ?")
	if strings.Contains(q, "SHA2") {
		t.Error("Parquet query must not use SHA2 (no index available)")
	}
	if args[0] != "12345" {
		t.Errorf("expected pk arg 12345, got %v", args[0])
	}
}

func TestBuildQueryEventType(t *testing.T) {
	et := parser.EventDelete
	opts := query.Options{EventType: &et, Limit: 100}
	q, args := buildQuery("/arc/*.parquet", opts)
	assertContains(t, q, "event_type = ?")
	if args[0] != int32(parser.EventDelete) {
		t.Errorf("expected event_type arg %d, got %v", parser.EventDelete, args[0])
	}
}

func TestBuildQuerySinceUntil(t *testing.T) {
	since := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 2, 28, 23, 59, 59, 0, time.UTC)
	opts := query.Options{Since: &since, Until: &until, Limit: 100}
	q, args := buildQuery("/arc/*.parquet", opts)
	assertContains(t, q, "event_timestamp >= ?")
	assertContains(t, q, "event_timestamp <= ?")
	if args[0] != since {
		t.Errorf("since arg mismatch: got %v", args[0])
	}
	if args[1] != until {
		t.Errorf("until arg mismatch: got %v", args[1])
	}
}

func TestBuildQueryGTID(t *testing.T) {
	opts := query.Options{GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:42", Limit: 100}
	q, args := buildQuery("/arc/*.parquet", opts)
	assertContains(t, q, "gtid = ?")
	if args[0] != opts.GTID {
		t.Errorf("gtid arg mismatch")
	}
}

func TestBuildQueryChangedColumn(t *testing.T) {
	opts := query.Options{Schema: "db", Table: "t", ChangedColumn: "status", Limit: 100}
	q, args := buildQuery("/arc/*.parquet", opts)
	assertContains(t, q, "json_contains(changed_columns, ?)")
	found := false
	for _, a := range args {
		if a == `"status"` {
			found = true
		}
	}
	if !found {
		t.Errorf("expected JSON-encoded needle %q in args, got %v", `"status"`, args)
	}
}

func TestBuildQueryNoLimit(t *testing.T) {
	q, args := buildQuery("/arc/*.parquet", query.Options{})
	if strings.Contains(q, "LIMIT") {
		t.Error("expected no LIMIT clause when Limit=0")
	}
	if len(args) != 0 {
		t.Errorf("expected no args for no-limit query, got %v", args)
	}
}

func TestBuildQueryGlobEscaping(t *testing.T) {
	q, _ := buildQuery("/it's/archives/*.parquet", query.Options{})
	assertContains(t, q, "parquet_scan('/it''s/archives/*.parquet', hive_partitioning=true, union_by_name=true)")
}

// ─── buildQueryForFile (single-file S3 with optional column handling) ──────

func TestBuildQueryForFileAllColumns(t *testing.T) {
	cols := map[string]bool{
		"event_id": true, "binlog_file": true, "start_pos": true,
		"end_pos": true, "event_timestamp": true, "gtid": true,
		"connection_id": true, "schema_name": true, "table_name": true,
		"event_type": true, "pk_values": true, "changed_columns": true,
		"row_before": true, "row_after": true, "schema_version": true,
	}
	q, args := buildQueryForFile("/tmp/events.parquet", query.Options{Limit: 10}, cols)
	assertContains(t, q, "connection_id,")
	if strings.Contains(q, "NULL::INT32") {
		t.Error("connection_id exists in file; should not use NULL fallback")
	}
	assertContains(t, q, "LIMIT ?")
	if len(args) != 1 || args[0] != 10 {
		t.Errorf("expected [10] args, got %v", args)
	}
}

func TestBuildQueryForFileMissingConnectionID(t *testing.T) {
	// Simulates pre-v0.4.4 parquet without connection_id.
	cols := map[string]bool{
		"event_id": true, "binlog_file": true, "start_pos": true,
		"end_pos": true, "event_timestamp": true, "gtid": true,
		"schema_name": true, "table_name": true, "event_type": true,
		"pk_values": true, "changed_columns": true, "row_before": true,
		"row_after": true, "schema_version": true,
	}
	q, _ := buildQueryForFile("/tmp/old.parquet", query.Options{Schema: "demo", Table: "customers"}, cols)
	assertContains(t, q, "NULL::INT32 AS connection_id")
	assertContains(t, q, "schema_name = ?")
	assertContains(t, q, "table_name = ?")
}

// ─── parseFileHour ──────────────────────────────────────────────────────────

func TestParseFileHour(t *testing.T) {
	tests := []struct {
		path   string
		wantOK bool
		want   time.Time
	}{
		{
			"s3://bucket/events/bintrail_id=abc/event_date=2026-03-09/event_hour=11/events.parquet",
			true,
			time.Date(2026, 3, 9, 11, 0, 0, 0, time.UTC),
		},
		{
			"/local/archives/event_date=2026-01-15/event_hour=00/events.parquet",
			true,
			time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{"s3://bucket/no-hive/events.parquet", false, time.Time{}},
		{"s3://bucket/event_date=bad/event_hour=11/e.parquet", false, time.Time{}},
	}
	for _, tc := range tests {
		got, ok := parseFileHour(tc.path)
		if ok != tc.wantOK {
			t.Errorf("parseFileHour(%q) ok = %v, want %v", tc.path, ok, tc.wantOK)
			continue
		}
		if ok && !got.Equal(tc.want) {
			t.Errorf("parseFileHour(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

// ─── filterFilesByTimeRange ─────────────────────────────────────────────────

func TestFilterFilesByTimeRange(t *testing.T) {
	files := []string{
		"s3://b/event_date=2026-03-09/event_hour=10/e.parquet",
		"s3://b/event_date=2026-03-09/event_hour=11/e.parquet",
		"s3://b/event_date=2026-03-09/event_hour=12/e.parquet",
		"s3://b/event_date=2026-03-09/event_hour=13/e.parquet",
	}

	since := time.Date(2026, 3, 9, 11, 0, 0, 0, time.UTC)
	until := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	got := filterFilesByTimeRange(files, &since, &until)
	// hour=10 ends at 11:00 which is not Before 11:00 → included
	// hour=11 overlaps → included
	// hour=12 starts at 12:00 which is not After 12:00 → included
	// hour=13 starts at 13:00 which is After 12:00 → excluded
	if len(got) != 3 {
		t.Fatalf("expected 3 files, got %d: %v", len(got), got)
	}

	// Since only
	got = filterFilesByTimeRange(files, &since, nil)
	if len(got) != 4 { // hour=10 ends at 11:00 (not before since), all included
		t.Errorf("since-only: expected 4, got %d", len(got))
	}

	// Until only — until=10:30 should include hour=10 only
	until1030 := time.Date(2026, 3, 9, 10, 30, 0, 0, time.UTC)
	got = filterFilesByTimeRange(files, nil, &until1030)
	if len(got) != 1 {
		t.Errorf("until-only 10:30: expected 1, got %d: %v", len(got), got)
	}

	// No filters
	got = filterFilesByTimeRange(files, nil, nil)
	if len(got) != 4 {
		t.Errorf("no filters: expected 4, got %d", len(got))
	}
}

func TestFilterFilesByTimeRangeUnparseable(t *testing.T) {
	files := []string{"s3://bucket/no-hive/events.parquet"}
	since := time.Date(2026, 3, 9, 11, 0, 0, 0, time.UTC)
	got := filterFilesByTimeRange(files, &since, nil)
	if len(got) != 1 {
		t.Errorf("unparseable files should be kept, got %d", len(got))
	}
}

// ─── generateDatePrefixes ──────────────────────────────────────────────────

func TestGenerateDatePrefixes(t *testing.T) {
	base := "events/bintrail_id=abc/"

	t.Run("both bounds same day", func(t *testing.T) {
		since := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
		until := time.Date(2026, 4, 12, 22, 0, 0, 0, time.UTC)
		got := generateDatePrefixes(base, &since, &until)
		if len(got) != 1 {
			t.Fatalf("expected 1 prefix, got %d: %v", len(got), got)
		}
		if got[0] != "events/bintrail_id=abc/event_date=2026-04-12/" {
			t.Errorf("unexpected prefix: %s", got[0])
		}
	})

	t.Run("two day span", func(t *testing.T) {
		since := time.Date(2026, 4, 12, 23, 0, 0, 0, time.UTC)
		until := time.Date(2026, 4, 13, 1, 0, 0, 0, time.UTC)
		got := generateDatePrefixes(base, &since, &until)
		if len(got) != 2 {
			t.Fatalf("expected 2 prefixes, got %d: %v", len(got), got)
		}
		if got[0] != "events/bintrail_id=abc/event_date=2026-04-12/" {
			t.Errorf("first prefix: %s", got[0])
		}
		if got[1] != "events/bintrail_id=abc/event_date=2026-04-13/" {
			t.Errorf("second prefix: %s", got[1])
		}
	})

	t.Run("cross month boundary", func(t *testing.T) {
		since := time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC)
		until := time.Date(2026, 4, 1, 23, 0, 0, 0, time.UTC)
		got := generateDatePrefixes(base, &since, &until)
		if len(got) != 2 {
			t.Fatalf("expected 2 prefixes, got %d: %v", len(got), got)
		}
		if got[0] != "events/bintrail_id=abc/event_date=2026-03-31/" {
			t.Errorf("first: %s", got[0])
		}
		if got[1] != "events/bintrail_id=abc/event_date=2026-04-01/" {
			t.Errorf("second: %s", got[1])
		}
	})

	t.Run("exactly 31 days returns prefixes", func(t *testing.T) {
		since := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
		until := time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC)
		got := generateDatePrefixes(base, &since, &until)
		if got == nil {
			t.Fatal("expected prefixes for exactly 31 days, got nil")
		}
		if len(got) != 31 {
			t.Errorf("expected 31 prefixes, got %d", len(got))
		}
	})

	t.Run("exceeds max days returns nil", func(t *testing.T) {
		since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		until := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
		got := generateDatePrefixes(base, &since, &until)
		if got != nil {
			t.Errorf("expected nil for wide range, got %d prefixes", len(got))
		}
	})

	t.Run("since only uses today as end", func(t *testing.T) {
		// Use yesterday as since — should produce 2 prefixes (yesterday + today).
		yesterday := time.Now().UTC().Truncate(24 * time.Hour).AddDate(0, 0, -1)
		since := yesterday.Add(10 * time.Hour) // yesterday 10:00
		got := generateDatePrefixes(base, &since, nil)
		if got == nil {
			t.Fatal("expected prefixes for since-only, got nil")
		}
		if len(got) != 2 {
			t.Errorf("expected 2 prefixes (yesterday + today), got %d", len(got))
		}
	})

	t.Run("until only defaults start to 31 days ago", func(t *testing.T) {
		// Use 5 days ago as until — should produce up to 31 prefixes
		// but since start is capped to 31 days before now, result
		// depends on the gap between now-31d and until.
		fiveDaysAgo := time.Now().UTC().Truncate(24 * time.Hour).AddDate(0, 0, -5)
		got := generateDatePrefixes(base, nil, &fiveDaysAgo)
		if got == nil {
			t.Fatal("expected prefixes for until-only, got nil")
		}
		// Start = now-31d, end = 5 days ago → ~26 days of prefixes.
		if len(got) < 20 || len(got) > 31 {
			t.Errorf("expected 20-31 prefixes for until-only, got %d", len(got))
		}
	})

	t.Run("no bounds returns nil", func(t *testing.T) {
		got := generateDatePrefixes(base, nil, nil)
		if got != nil {
			t.Errorf("expected nil for no bounds, got %d prefixes", len(got))
		}
	})
}

// ─── sortFilesByHour ───────────────────────────────────────────────────────

func TestSortFilesByHour(t *testing.T) {
	files := []string{
		"s3://b/event_date=2026-03-09/event_hour=13/e.parquet",
		"s3://b/event_date=2026-03-09/event_hour=10/e.parquet",
		"s3://b/no-hive/events.parquet",
		"s3://b/event_date=2026-03-09/event_hour=11/e.parquet",
	}
	got := sortFilesByHour(files)
	// Chronological order: 10, 11, 13, then unparseable at end
	want := []string{
		"s3://b/event_date=2026-03-09/event_hour=10/e.parquet",
		"s3://b/event_date=2026-03-09/event_hour=11/e.parquet",
		"s3://b/event_date=2026-03-09/event_hour=13/e.parquet",
		"s3://b/no-hive/events.parquet",
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("index %d: got %q, want %q", i, got[i], w)
		}
	}
}

func TestSortFilesByHourMultipleDates(t *testing.T) {
	files := []string{
		"s3://b/event_date=2026-03-10/event_hour=00/e.parquet",
		"s3://b/event_date=2026-03-09/event_hour=23/e.parquet",
		"s3://b/event_date=2026-03-09/event_hour=10/e.parquet",
	}
	got := sortFilesByHour(files)
	want := []string{
		"s3://b/event_date=2026-03-09/event_hour=10/e.parquet",
		"s3://b/event_date=2026-03-09/event_hour=23/e.parquet",
		"s3://b/event_date=2026-03-10/event_hour=00/e.parquet",
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("index %d: got %q, want %q", i, got[i], w)
		}
	}
}

func TestSortFilesByHourEmpty(t *testing.T) {
	got := sortFilesByHour(nil)
	if len(got) != 0 {
		t.Errorf("expected empty, got %d", len(got))
	}
}

func TestSortFilesByHourDoesNotMutateInput(t *testing.T) {
	files := []string{
		"s3://b/event_date=2026-03-09/event_hour=13/e.parquet",
		"s3://b/event_date=2026-03-09/event_hour=10/e.parquet",
	}
	orig := files[0]
	_ = sortFilesByHour(files)
	if files[0] != orig {
		t.Error("sortFilesByHour mutated the input slice")
	}
}

// ─── canTerminateEarly ─────────────────────────────────────────────────────

func TestCanTerminateEarly(t *testing.T) {
	mkRow := func(ts time.Time, id uint64) query.ResultRow {
		return query.ResultRow{EventTimestamp: ts, EventID: id}
	}

	t.Run("can terminate when next hour is after cutoff", func(t *testing.T) {
		results := []query.ResultRow{
			mkRow(time.Date(2026, 3, 9, 10, 15, 0, 0, time.UTC), 1),
			mkRow(time.Date(2026, 3, 9, 10, 30, 0, 0, time.UTC), 2),
			mkRow(time.Date(2026, 3, 9, 10, 45, 0, 0, time.UTC), 3),
		}
		remaining := []string{"s3://b/event_date=2026-03-09/event_hour=11/e.parquet"}
		if !canTerminateEarly(results, remaining, 3) {
			t.Error("expected early termination: next hour=11 is after all results in hour=10")
		}
	})

	t.Run("cannot terminate when next hour overlaps", func(t *testing.T) {
		results := []query.ResultRow{
			mkRow(time.Date(2026, 3, 9, 10, 15, 0, 0, time.UTC), 1),
			mkRow(time.Date(2026, 3, 9, 11, 30, 0, 0, time.UTC), 2),
			mkRow(time.Date(2026, 3, 9, 11, 45, 0, 0, time.UTC), 3),
		}
		remaining := []string{"s3://b/event_date=2026-03-09/event_hour=11/e.parquet"}
		if canTerminateEarly(results, remaining, 2) {
			t.Error("should not terminate: limit-th result is at 11:30, next hour starts at 11:00")
		}
	})

	t.Run("not enough results", func(t *testing.T) {
		results := []query.ResultRow{
			mkRow(time.Date(2026, 3, 9, 10, 15, 0, 0, time.UTC), 1),
		}
		remaining := []string{"s3://b/event_date=2026-03-09/event_hour=12/e.parquet"}
		if canTerminateEarly(results, remaining, 5) {
			t.Error("should not terminate: not enough results")
		}
	})

	t.Run("unparseable remaining file", func(t *testing.T) {
		results := []query.ResultRow{
			mkRow(time.Date(2026, 3, 9, 10, 15, 0, 0, time.UTC), 1),
		}
		remaining := []string{"s3://b/no-hive/events.parquet"}
		if canTerminateEarly(results, remaining, 1) {
			t.Error("should not terminate: can't parse remaining file's hour")
		}
	})

	t.Run("unsorted results still finds correct cutoff", func(t *testing.T) {
		// Results arrive out of order; limit=2 means the cutoff should be
		// the 2nd result after sorting: 10:30 (id=3).
		results := []query.ResultRow{
			mkRow(time.Date(2026, 3, 9, 10, 45, 0, 0, time.UTC), 5),
			mkRow(time.Date(2026, 3, 9, 10, 15, 0, 0, time.UTC), 1),
			mkRow(time.Date(2026, 3, 9, 10, 30, 0, 0, time.UTC), 3),
		}
		remaining := []string{"s3://b/event_date=2026-03-09/event_hour=11/e.parquet"}
		if !canTerminateEarly(results, remaining, 2) {
			t.Error("expected early termination: sorted 2nd result (10:30) is before hour=11")
		}
	})

	t.Run("cutoff exactly at next hour boundary does not terminate", func(t *testing.T) {
		// The limit-th result is exactly at 11:00, next file starts at hour=11.
		// nextHour.After(cutoff) → 11:00.After(11:00) → false → don't terminate.
		results := []query.ResultRow{
			mkRow(time.Date(2026, 3, 9, 11, 0, 0, 0, time.UTC), 1),
		}
		remaining := []string{"s3://b/event_date=2026-03-09/event_hour=11/e.parquet"}
		if canTerminateEarly(results, remaining, 1) {
			t.Error("should not terminate: cutoff is exactly at next hour start")
		}
	})

	t.Run("no remaining files", func(t *testing.T) {
		results := []query.ResultRow{
			mkRow(time.Date(2026, 3, 9, 10, 15, 0, 0, time.UTC), 1),
		}
		if canTerminateEarly(results, nil, 1) {
			t.Error("should not terminate: no remaining files")
		}
	})
}

// ─── drainSlots / removeTempFile (pipeline cleanup) ─────────────────────────

func TestDrainSlotsRemovesPrefetchedFiles(t *testing.T) {
	dir := t.TempDir()
	mkFile := func(name string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	// Two slots: one with a prefetched file, one closed without value
	// (simulates a download that was canceled before completing).
	a, b := mkFile("a.parquet"), mkFile("b.parquet")
	slots := []chan dlResult{
		make(chan dlResult, 1),
		make(chan dlResult, 1),
		make(chan dlResult, 1),
	}
	slots[0] <- dlResult{path: a}
	close(slots[0])
	slots[1] <- dlResult{path: b}
	close(slots[1])
	close(slots[2]) // closed empty — no path to remove

	drainSlots(slots)

	for _, p := range []string{a, b} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("expected %s removed, got err=%v", p, err)
		}
	}
}

func TestRemoveTempFileMissingIsNoOp(t *testing.T) {
	// Should not warn or panic on missing files.
	removeTempFile(filepath.Join(t.TempDir(), "does-not-exist.parquet"))
	removeTempFile("") // empty path is also a no-op
}

// ─── prefetchAll (pipeline concurrency invariants) ─────────────────────────

// fakeDownloader builds a downloadFn that creates real temp files in dir,
// optionally blocking on a per-call basis to model slow downloads. Callers
// can cancel mid-flight to exercise cleanup paths.
type fakeDownloader struct {
	dir     string
	created atomic.Int32
	gate    chan struct{} // closed → all calls return immediately; nil → no gating
	failOn  string        // src that should return an error instead of a path
}

func (f *fakeDownloader) fn() downloadFn {
	return func(ctx context.Context, src string) (string, error) {
		if f.gate != nil {
			select {
			case <-f.gate:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		if src == f.failOn {
			return "", errors.New("simulated download failure")
		}
		path := filepath.Join(f.dir, fmt.Sprintf("dl-%d.parquet", f.created.Add(1)))
		if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
			return "", err
		}
		return path, nil
	}
}

// remainingFiles returns the temp files in dir that haven't been deleted —
// used to assert pipeline cleanup did its job.
func remainingFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, e := range entries {
		paths = append(paths, e.Name())
	}
	return paths
}

func makeSlots(n int) []chan dlResult {
	slots := make([]chan dlResult, n)
	for i := range slots {
		slots[i] = make(chan dlResult, 1)
	}
	return slots
}

func TestPrefetchAllClosesEverySlotOnCancel(t *testing.T) {
	// Cancellation must close every slot so the consumer's <-slots[i] never
	// blocks forever. Mix of launched-but-pending downloads (held by gate)
	// and unlaunched slots (semaphore not yet acquired).
	dir := t.TempDir()
	gate := make(chan struct{}) // never closed; fake downloads will block
	fd := &fakeDownloader{dir: dir, gate: gate}

	files := []string{"a", "b", "c", "d", "e"}
	slots := makeSlots(len(files))
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		prefetchAll(ctx, files, slots, 2, fd.fn())
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("prefetchAll did not return after cancel")
	}

	for i, ch := range slots {
		select {
		case _, ok := <-ch:
			if ok {
				// Receiving a value is fine (ok=true) iff a download had
				// already completed before cancel; on the next read we'd
				// see ok=false. The point is the channel is not blocking.
			}
		default:
			t.Errorf("slot %d not closed (would block consumer)", i)
		}
	}
}

func TestPrefetchAllNoLeakWhenConsumerAbandonsMidStream(t *testing.T) {
	// Simulates the consumer breaking on early termination: it reads one
	// slot, then cancels and drains the rest. Every temp file the fake
	// downloader created must be removed.
	dir := t.TempDir()
	fd := &fakeDownloader{dir: dir} // no gate — downloads complete immediately

	files := []string{"a", "b", "c", "d", "e", "f"}
	slots := makeSlots(len(files))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		prefetchAll(ctx, files, slots, 2, fd.fn())
		close(done)
	}()

	// Consumer reads slot 0 and cleans up the temp file it received.
	dr := <-slots[0]
	if dr.err != nil {
		t.Fatalf("unexpected error: %v", dr.err)
	}
	removeTempFile(dr.path)

	// Mimic Fetch's early-termination path.
	cancel()
	drainSlots(slots[1:])

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("prefetchAll did not return after cancel")
	}

	if leftover := remainingFiles(t, dir); len(leftover) != 0 {
		t.Errorf("temp files leaked: %v", leftover)
	}
}

func TestPrefetchAllInFlightDownloadsCleanedUp(t *testing.T) {
	// The trickiest race: a download that's still in flight when cancel
	// fires. The goroutine's post-download ctx.Err() check must remove
	// the temp file rather than send it. Use a gate that releases AFTER
	// cancel to force this ordering.
	dir := t.TempDir()
	gate := make(chan struct{})
	fd := &fakeDownloader{dir: dir, gate: gate}

	files := []string{"a", "b", "c", "d"}
	slots := makeSlots(len(files))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		prefetchAll(ctx, files, slots, 2, fd.fn())
		close(done)
	}()

	// Cancel BEFORE releasing the gate so in-flight downloads observe
	// ctx.Done in their fake impl AND the post-download check.
	time.Sleep(20 * time.Millisecond) // let the first 2 enter the gate
	cancel()
	close(gate) // release whatever's waiting

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("prefetchAll did not return")
	}

	// Drain anything that managed to land in slots before cancel propagated.
	drainSlots(slots)

	if leftover := remainingFiles(t, dir); len(leftover) != 0 {
		t.Errorf("temp files leaked from in-flight downloads: %v", leftover)
	}
}
