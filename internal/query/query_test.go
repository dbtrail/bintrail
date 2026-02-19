package query

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bintrail/bintrail/internal/parser"
)

// ─── buildQuery ───────────────────────────────────────────────────────────────

func TestBuildQuery_noFilters(t *testing.T) {
	opts := Options{Limit: 50}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "FROM binlog_events") {
		t.Errorf("expected FROM binlog_events in query, got: %s", q)
	}
	if strings.Contains(q, "WHERE") {
		t.Errorf("expected no WHERE clause with no filters, got: %s", q)
	}
	// Last arg must be the LIMIT value.
	if args[len(args)-1] != 50 {
		t.Errorf("expected LIMIT arg 50, got %v", args[len(args)-1])
	}
}

func TestBuildQuery_schemaTable(t *testing.T) {
	opts := Options{Schema: "mydb", Table: "orders", Limit: 10}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "schema_name = ?") {
		t.Errorf("expected schema_name = ? in query")
	}
	if !strings.Contains(q, "table_name = ?") {
		t.Errorf("expected table_name = ? in query")
	}
	if args[0] != "mydb" || args[1] != "orders" {
		t.Errorf("expected args [mydb orders ...], got %v", args)
	}
}

func TestBuildQuery_pkLookup(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", PKValues: "42", Limit: 1}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "pk_hash = SHA2(?, 256)") {
		t.Errorf("expected pk_hash = SHA2(?, 256) in query: %s", q)
	}
	if !strings.Contains(q, "pk_values = ?") {
		t.Errorf("expected pk_values = ? in query: %s", q)
	}
	// pk_values appears twice: once for hash, once for collision guard.
	count := strings.Count(q, "pk_")
	if count < 2 {
		t.Errorf("expected pk_hash and pk_values in query")
	}
	// Both args should be the same PK string.
	found := false
	for _, a := range args {
		if a == "42" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected pk value '42' in args, got %v", args)
	}
}

func TestBuildQuery_eventTypeFilter(t *testing.T) {
	et := parser.EventDelete
	opts := Options{EventType: &et, Limit: 10}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "event_type = ?") {
		t.Errorf("expected event_type = ? in query: %s", q)
	}
	// Find uint8(3) in args.
	found := false
	for _, a := range args {
		if a == uint8(3) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected event_type arg uint8(3) in %v", args)
	}
}

func TestBuildQuery_changedColumn(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", ChangedColumn: "status", Limit: 10}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "JSON_CONTAINS") {
		t.Errorf("expected JSON_CONTAINS in query: %s", q)
	}
	// The needle arg must be the JSON string `"status"` (with quotes).
	needle := `"status"`
	found := false
	for _, a := range args {
		if a == needle {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected JSON needle %q in args %v", needle, args)
	}
}

func TestBuildQuery_defaultLimit(t *testing.T) {
	opts := Options{} // Limit=0 → should default to 100
	_, args := buildQuery(opts)
	if args[len(args)-1] != 100 {
		t.Errorf("expected default limit 100, got %v", args[len(args)-1])
	}
}

func TestBuildQuery_sinceUntil(t *testing.T) {
	since := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	until := time.Date(2026, 2, 19, 15, 0, 0, 0, time.UTC)
	opts := Options{Since: &since, Until: &until, Limit: 10}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "event_timestamp >= ?") {
		t.Errorf("expected event_timestamp >= ? in query: %s", q)
	}
	if !strings.Contains(q, "event_timestamp <= ?") {
		t.Errorf("expected event_timestamp <= ? in query: %s", q)
	}
	// since and until should both appear in args.
	if args[0] != since || args[1] != until {
		t.Errorf("since/until args mismatch: %v", args)
	}
}

// ─── eventTypeName ───────────────────────────────────────────────────────────

func TestEventTypeName(t *testing.T) {
	cases := []struct {
		et   parser.EventType
		want string
	}{
		{parser.EventInsert, "INSERT"},
		{parser.EventUpdate, "UPDATE"},
		{parser.EventDelete, "DELETE"},
		{parser.EventType(99), "UNKNOWN"},
	}
	for _, tc := range cases {
		got := eventTypeName(tc.et)
		if got != tc.want {
			t.Errorf("eventTypeName(%d) = %q, want %q", tc.et, got, tc.want)
		}
	}
}

// ─── writeTable ───────────────────────────────────────────────────────────────

func TestWriteTable_empty(t *testing.T) {
	var buf bytes.Buffer
	n, err := writeTable(nil, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows, got %d", n)
	}
	if !strings.Contains(buf.String(), "No results") {
		t.Errorf("expected 'No results' in output, got: %s", buf.String())
	}
}

func TestWriteTable_singleRow(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 1, 0, time.UTC)
	gtid := "abc:42"
	rows := []ResultRow{{
		EventID:        1,
		BinlogFile:     "binlog.000042",
		StartPos:       100,
		EndPos:         200,
		EventTimestamp: ts,
		GTID:           &gtid,
		SchemaName:     "mydb",
		TableName:      "orders",
		EventType:      parser.EventUpdate,
		PKValues:       "12345",
		ChangedColumns: []string{"status", "amount"},
	}}
	var buf bytes.Buffer
	n, err := writeTable(rows, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
	out := buf.String()
	for _, want := range []string{"UPDATE", "mydb", "orders", "12345", "status,amount", "abc:42"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in table output:\n%s", want, out)
		}
	}
}

// ─── writeJSON ────────────────────────────────────────────────────────────────

func TestWriteJSON_structure(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 1, 0, time.UTC)
	rows := []ResultRow{{
		EventID:        7,
		EventTimestamp: ts,
		SchemaName:     "s",
		TableName:      "t",
		EventType:      parser.EventInsert,
		PKValues:       "1",
		RowAfter:       map[string]any{"id": float64(1), "name": "Alice"},
	}}
	var buf bytes.Buffer
	n, err := writeJSON(rows, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}

	var out []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 JSON element, got %d", len(out))
	}
	if out[0]["event_type"] != "INSERT" {
		t.Errorf("expected event_type=INSERT, got %v", out[0]["event_type"])
	}
	if out[0]["event_id"] != float64(7) {
		t.Errorf("expected event_id=7, got %v", out[0]["event_id"])
	}
}

// ─── writeCSV ─────────────────────────────────────────────────────────────────

func TestWriteCSV_headersAndRow(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 1, 0, time.UTC)
	rows := []ResultRow{{
		EventID:        3,
		BinlogFile:     "binlog.000001",
		EventTimestamp: ts,
		SchemaName:     "db",
		TableName:      "tbl",
		EventType:      parser.EventDelete,
		PKValues:       "99",
	}}
	var buf bytes.Buffer
	n, err := writeCSV(rows, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 { // header + 1 data row
		t.Errorf("expected 2 lines (header + row), got %d:\n%s", len(lines), buf.String())
	}
	if !strings.HasPrefix(lines[0], "event_id") {
		t.Errorf("expected CSV header to start with event_id, got: %s", lines[0])
	}
	if !strings.Contains(lines[1], "DELETE") {
		t.Errorf("expected DELETE in data row, got: %s", lines[1])
	}
}
