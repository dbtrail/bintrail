package parquetquery

import (
	"strings"
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
