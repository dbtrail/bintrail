package parquetquery

import (
	"strings"
	"testing"
	"time"

	"github.com/bintrail/bintrail/internal/parser"
	"github.com/bintrail/bintrail/internal/query"
)

// ─── buildGlob ────────────────────────────────────────────────────────────────

func TestBuildGlob(t *testing.T) {
	tests := []struct {
		source string
		want   string
	}{
		{"/data/archives", "/data/archives/**/*.parquet"},
		{"/data/archives/", "/data/archives/**/*.parquet"},
		// Hive-partitioned path scoped to one server
		{"/data/archives/bintrail_id=abc-123", "/data/archives/bintrail_id=abc-123/**/*.parquet"},
		// Single file: returned as-is.
		{"/data/archives/events_14.parquet", "/data/archives/events_14.parquet"},
		{"s3://bucket/prefix", "s3://bucket/prefix/**/*.parquet"},
		{"s3://bucket/prefix/", "s3://bucket/prefix/**/*.parquet"},
		{"s3://bucket/prefix/bintrail_id=abc-123", "s3://bucket/prefix/bintrail_id=abc-123/**/*.parquet"},
		{"s3://bucket/prefix/*.parquet", "s3://bucket/prefix/*.parquet"},
	}
	for _, tc := range tests {
		got := buildGlob(tc.source)
		if got != tc.want {
			t.Errorf("buildGlob(%q) = %q, want %q", tc.source, got, tc.want)
		}
	}
}

// ─── buildQuery ───────────────────────────────────────────────────────────────

func assertContains(t *testing.T, s, want string) {
	t.Helper()
	if !strings.Contains(s, want) {
		t.Errorf("expected SQL to contain %q\ngot: %s", want, s)
	}
}

func TestBuildQueryNoFilters(t *testing.T) {
	q, args := buildQuery("/archives/*.parquet", query.Options{Limit: 50})
	assertContains(t, q, "FROM parquet_scan('/archives/*.parquet')")
	assertContains(t, q, "ORDER BY event_timestamp, event_id")
	assertContains(t, q, "LIMIT ?")
	// Only arg is the limit.
	if len(args) != 1 || args[0] != 50 {
		t.Errorf("expected [50] args, got %v", args)
	}
}

// TestBuildQueryViaGlob verifies the full buildGlob→buildQuery pipeline: a
// Hive-partitioned source path should produce SQL with the recursive
// /**/*.parquet glob, not the old flat /*.parquet pattern.
func TestBuildQueryViaGlob(t *testing.T) {
	glob := buildGlob("/archives/bintrail_id=abc-123")
	q, _ := buildQuery(glob, query.Options{Limit: 50})
	assertContains(t, q, "/archives/bintrail_id=abc-123/**/*.parquet")
}

func TestBuildQuerySchemaTable(t *testing.T) {
	opts := query.Options{Schema: "mydb", Table: "orders", Limit: 10}
	q, args := buildQuery("/arc/*.parquet", opts)
	assertContains(t, q, "schema_name = ?")
	assertContains(t, q, "table_name = ?")
	if len(args) != 3 { // schema, table, limit
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
	// No SHA2 — plain equality only.
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
	// The needle must be the JSON-encoded column name (with double quotes).
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
	// Limit=0 means "no LIMIT clause" so the merge layer can apply the real limit.
	q, args := buildQuery("/arc/*.parquet", query.Options{})
	if strings.Contains(q, "LIMIT") {
		t.Error("expected no LIMIT clause when Limit=0")
	}
	if len(args) != 0 {
		t.Errorf("expected no args for no-limit query, got %v", args)
	}
}

func TestBuildQueryGlobEscaping(t *testing.T) {
	// A single quote in the path must be escaped as '' to prevent SQL injection.
	q, _ := buildQuery("/it's/archives/*.parquet", query.Options{})
	assertContains(t, q, "/it''s/archives/*.parquet")
}
