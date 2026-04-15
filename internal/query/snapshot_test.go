package query

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
)

// TestShouldSkipSnapshot is the unit-level truth table for the four always-
// excluder filters. The DuckDB-backed FetchSnapshot path is integration-only,
// but these branches are pure logic — this test pins them without needing a
// real Parquet file.
func TestShouldSkipSnapshot(t *testing.T) {
	snapshotET := parser.EventSnapshot
	insertET := parser.EventInsert

	cases := []struct {
		name       string
		opts       Options
		wantSkip   bool
		wantReason string
	}{
		{"happy path — no excluders", Options{}, false, ""},
		{"event-type SNAPSHOT keeps source", Options{EventType: &snapshotET}, false, ""},
		{"event-type INSERT excludes", Options{EventType: &insertET}, true, "--event-type"},
		{"gtid set excludes", Options{GTID: "uuid:42"}, true, "--gtid"},
		{"changed-column excludes", Options{ChangedColumn: "status"}, true, "--changed-column"},
		{"flag excludes", Options{Flag: "pii"}, true, "--flag"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason, skip := shouldSkipSnapshot(tc.opts)
			if skip != tc.wantSkip {
				t.Fatalf("skip = %v, want %v (reason=%q)", skip, tc.wantSkip, reason)
			}
			if tc.wantSkip && !strings.Contains(reason, tc.wantReason) {
				t.Errorf("reason = %q, want substring %q", reason, tc.wantReason)
			}
			if !tc.wantSkip && reason != "" {
				t.Errorf("expected empty reason when not skipping; got %q", reason)
			}
		})
	}
}

// TestSnapshotFilters_columnEq pins the WHERE shape: typed Parquet columns are
// compared via CAST(... AS VARCHAR) = ? so the binlog index's
// JSON_UNQUOTE(JSON_EXTRACT(...)) string-coercion parity is preserved.
func TestSnapshotFilters_columnEq(t *testing.T) {
	opts := Options{
		ColumnEq: []ColumnEq{{Column: "status", Value: "active"}},
	}
	where, args, err := snapshotFilters(opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d: %v", len(where), where)
	}
	if !strings.Contains(where[0], `CAST("status" AS VARCHAR) = ?`) {
		t.Errorf("expected CAST-based predicate; got %q", where[0])
	}
	if len(args) != 1 || args[0] != "active" {
		t.Errorf("expected single bind arg \"active\"; got %v", args)
	}
}

// TestSnapshotFilters_isNull pins the NULL path: emits IS NULL with no bind arg.
func TestSnapshotFilters_isNull(t *testing.T) {
	opts := Options{
		ColumnEq: []ColumnEq{{Column: "deleted_at", IsNull: true}},
	}
	where, args, err := snapshotFilters(opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(where) != 1 || !strings.Contains(where[0], "IS NULL") {
		t.Errorf("expected IS NULL predicate; got %v", where)
	}
	if len(args) != 0 {
		t.Errorf("expected no bind args for IS NULL; got %v", args)
	}
}

// TestSnapshotFilters_unsafeColumnNameHardErrors pins the "hard error, not
// silent 1=0" contract: a column name that fails the allowlist must raise a
// visible error rather than silently returning zero rows. The CLI user runs
// `--format table` and would otherwise see "No results" with the rejection
// only in structured logs.
func TestSnapshotFilters_unsafeColumnNameHardErrors(t *testing.T) {
	opts := Options{
		ColumnEq: []ColumnEq{{Column: "x;DROP", Value: "v"}},
	}
	_, _, err := snapshotFilters(opts)
	if err == nil {
		t.Fatal("expected hard error for unsafe column name")
	}
	if !strings.Contains(err.Error(), "unsafe column name") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestSnapshotEventIDBase_aboveRealEventIDs documents the invariant that the
// synthetic event-id base sits far above any realistic binlog event_id. The
// indexer assigns event_ids via AUTO_INCREMENT BIGINT UNSIGNED, so in practice
// values never come near 2^62. A failure here means someone lowered the
// constant — reconsider MergeResults dedup safety before doing so.
func TestSnapshotEventIDBase_aboveRealEventIDs(t *testing.T) {
	if snapshotEventIDBase <= 1<<48 {
		t.Errorf("snapshotEventIDBase=%d must be > 1<<48 to safely avoid collision with AUTO_INCREMENT event_ids", snapshotEventIDBase)
	}
}

// TestLooksLikeJSONContainer guards the narrowed JSON promotion rule: only
// object/array payloads are parsed as JSON. Bare "null", bare strings, and
// bare numbers stay as strings so a TEXT column's literal "null" doesn't
// silently disappear as Go nil.
func TestLooksLikeJSONContainer(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{`{"a":1}`, true},
		{`[1,2,3]`, true},
		{`  {"a":1}`, true},
		{`null`, false},
		{`"foo"`, false},
		{`123`, false},
		{``, false},
	}
	for _, tc := range cases {
		if got := looksLikeJSONContainer([]byte(tc.in)); got != tc.want {
			t.Errorf("looksLikeJSONContainer(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestNormalizeSnapshotValue_bareJSONLiteralStaysString documents the
// regression fix: DuckDB may return a VARCHAR column containing "null" or "123"
// as []byte. Earlier revisions promoted these via json.Valid + Unmarshal,
// which silently dropped the literal string into Go nil or a number. The
// narrowed rule preserves the original string representation.
func TestNormalizeSnapshotValue_bareJSONLiteralStaysString(t *testing.T) {
	if got := normalizeSnapshotValue([]byte("null")); got != "null" {
		t.Errorf("normalizeSnapshotValue(\"null\") = %v (%T), want \"null\" (string)", got, got)
	}
	if got := normalizeSnapshotValue([]byte(`"hello"`)); got != `"hello"` {
		t.Errorf("normalizeSnapshotValue(quoted string) = %v, want verbatim passthrough", got)
	}
	if got := normalizeSnapshotValue([]byte("123")); got != "123" {
		t.Errorf("normalizeSnapshotValue(\"123\") = %v (%T), want \"123\" (string)", got, got)
	}
}

// TestNormalizeSnapshotValue_jsonContainerParses verifies that genuine JSON
// object/array payloads (as MySQL stores in JSON columns) are still decoded
// into map/slice so downstream JSON formatters don't double-escape them.
func TestNormalizeSnapshotValue_jsonContainerParses(t *testing.T) {
	got := normalizeSnapshotValue([]byte(`{"status":"open"}`))
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map from JSON object payload; got %T", got)
	}
	if m["status"] != "open" {
		t.Errorf("expected {status:open}; got %v", m)
	}
}

// TestNormalizeSnapshotValue_timeFormatsUTC pins the UTC formatting contract:
// TIMESTAMP/DATETIME columns scanned as time.Time must render as
// "2006-01-02 15:04:05" UTC. A silent regression to time.Local (see the
// earlier ParseTime UTC bug) would ship timezone-shifted strings in JSON
// output.
func TestNormalizeSnapshotValue_timeFormatsUTC(t *testing.T) {
	// Non-UTC input should still format as UTC.
	loc := time.FixedZone("UTC-5", -5*3600)
	in := time.Date(2026, 4, 15, 13, 30, 45, 0, loc)
	got := normalizeSnapshotValue(in)
	if got != "2026-04-15 18:30:45" {
		t.Errorf("normalizeSnapshotValue(non-UTC time) = %v, want \"2026-04-15 18:30:45\" (UTC-adjusted)", got)
	}
}

// TestEventSnapshot_notParsed sanity-checks that EventSnapshot is distinct
// from every real parser-emitted event type. A regression where the parser
// starts producing EventSnapshot would leak synthetic rows into the indexer
// pipeline.
func TestEventSnapshot_notParsed(t *testing.T) {
	for _, parsed := range []parser.EventType{parser.EventInsert, parser.EventUpdate, parser.EventDelete, parser.EventDDL, parser.EventGTID} {
		if parsed == parser.EventSnapshot {
			t.Errorf("EventSnapshot must not overlap with parser-emitted type %d", parsed)
		}
	}
}
