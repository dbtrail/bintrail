package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
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

func TestBuildQuery_noLimit(t *testing.T) {
	// Limit=0 means "no LIMIT clause" — callers (CLI, MCP) apply their own defaults.
	q, args := buildQuery(Options{})
	if strings.Contains(q, "LIMIT") {
		t.Error("expected no LIMIT clause when Limit=0")
	}
	if len(args) != 0 {
		t.Errorf("expected no args for no-limit query, got %v", args)
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
	// TO_SECONDS pruning hints must always be present, even for hour-aligned values,
	// because MySQL cannot infer partition pruning from parameterised datetime comparisons.
	outerSince := mysqlToSeconds(time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC))
	outerUntil := mysqlToSeconds(time.Date(2026, 2, 19, 16, 0, 0, 0, time.UTC))
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", outerSince)) {
		t.Errorf("expected lower-bound TO_SECONDS hint in query: %s", q)
	}
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", outerUntil)) {
		t.Errorf("expected upper-bound TO_SECONDS hint in query: %s", q)
	}
	// since and until should both appear in args.
	if args[0] != since || args[1] != until {
		t.Errorf("since/until args mismatch: %v", args)
	}
}

func TestBuildQuery_sinceUntil_nonHourAligned(t *testing.T) {
	since := time.Date(2026, 2, 19, 14, 45, 0, 0, time.UTC)
	until := time.Date(2026, 2, 19, 15, 13, 0, 0, time.UTC)
	opts := Options{Since: &since, Until: &until, Limit: 10}
	q, args := buildQuery(opts)

	// Outer partition-pruning hints must be present.
	outerSince := mysqlToSeconds(time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC))
	outerUntil := mysqlToSeconds(time.Date(2026, 2, 19, 16, 0, 0, 0, time.UTC))
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", outerSince)) {
		t.Errorf("expected outer lower-bound hint in query: %s", q)
	}
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", outerUntil)) {
		t.Errorf("expected outer upper-bound hint in query: %s", q)
	}
	// Exact parameterised bounds must still be present for correct filtering.
	if !strings.Contains(q, "event_timestamp >= ?") {
		t.Errorf("expected exact lower bound in query: %s", q)
	}
	if !strings.Contains(q, "event_timestamp <= ?") {
		t.Errorf("expected exact upper bound in query: %s", q)
	}
	// Args must be the exact since/until values (not the outer bounds).
	if args[0] != since {
		t.Errorf("expected args[0]=since (%v), got %v", since, args[0])
	}
	if args[1] != until {
		t.Errorf("expected args[1]=until (%v), got %v", until, args[1])
	}
}

func TestBuildQuery_sinceOnly_nonHourAligned(t *testing.T) {
	since := time.Date(2026, 2, 19, 14, 45, 0, 0, time.UTC)
	opts := Options{Since: &since}
	q, args := buildQuery(opts)

	outerSince := mysqlToSeconds(time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC))
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", outerSince)) {
		t.Errorf("expected lower-bound hint in query: %s", q)
	}
	// Must NOT emit an upper-bound hint when Until is nil.
	if strings.Contains(q, "TO_SECONDS(event_timestamp) <") {
		t.Errorf("unexpected upper-bound TO_SECONDS hint when Until is nil: %s", q)
	}
	if args[0] != since {
		t.Errorf("expected args[0]=since (%v), got %v", since, args[0])
	}
}

func TestBuildQuery_untilOnly_nonHourAligned(t *testing.T) {
	until := time.Date(2026, 2, 19, 15, 13, 0, 0, time.UTC)
	opts := Options{Until: &until}
	q, args := buildQuery(opts)

	outerUntil := mysqlToSeconds(time.Date(2026, 2, 19, 16, 0, 0, 0, time.UTC))
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", outerUntil)) {
		t.Errorf("expected upper-bound hint in query: %s", q)
	}
	// Must NOT emit a lower-bound hint when Since is nil.
	if strings.Contains(q, "TO_SECONDS(event_timestamp) >=") {
		t.Errorf("unexpected lower-bound TO_SECONDS hint when Since is nil: %s", q)
	}
	if args[0] != until {
		t.Errorf("expected args[0]=until (%v), got %v", until, args[0])
	}
}

func TestBuildQuery_sinceNonAligned_untilAligned(t *testing.T) {
	since := time.Date(2026, 2, 19, 14, 30, 0, 0, time.UTC)
	until := time.Date(2026, 2, 19, 15, 0, 0, 0, time.UTC) // exact hour boundary
	opts := Options{Since: &since, Until: &until}
	q, _ := buildQuery(opts)

	if !strings.Contains(q, "TO_SECONDS(event_timestamp) >=") {
		t.Errorf("expected lower-bound TO_SECONDS hint: %s", q)
	}
	// Even hour-aligned until must produce a TO_SECONDS hint.
	if !strings.Contains(q, "TO_SECONDS(event_timestamp) <") {
		t.Errorf("expected upper-bound TO_SECONDS hint: %s", q)
	}
}

func TestBuildQuery_sinceAligned_untilNonAligned(t *testing.T) {
	since := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC) // exact hour boundary
	until := time.Date(2026, 2, 19, 15, 13, 0, 0, time.UTC)
	opts := Options{Since: &since, Until: &until}
	q, _ := buildQuery(opts)

	// Even hour-aligned since must produce a TO_SECONDS hint.
	if !strings.Contains(q, "TO_SECONDS(event_timestamp) >=") {
		t.Errorf("expected lower-bound TO_SECONDS hint: %s", q)
	}
	if !strings.Contains(q, "TO_SECONDS(event_timestamp) <") {
		t.Errorf("expected upper-bound TO_SECONDS hint: %s", q)
	}
}

func TestMysqlToSeconds(t *testing.T) {
	// TO_SECONDS('1970-01-01 00:00:00') = 62167219200 per MySQL 8.0.
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := mysqlToSeconds(epoch); got != 62167219200 {
		t.Errorf("mysqlToSeconds(epoch) = %d, want 62167219200", got)
	}
	// Exact hour boundaries map to exact partition edge values.
	// Verify round-trip: mysqlToSeconds(partition_start) must equal the
	// TO_SECONDS integer stored as the partition upper boundary.
	h := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	hPlus1 := h.Add(time.Hour)
	if mysqlToSeconds(hPlus1) != mysqlToSeconds(h)+3600 {
		t.Errorf("expected hourly increment of 3600 seconds")
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

func TestBuildQuery_gtidFilter(t *testing.T) {
	opts := Options{GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:42", Limit: 10}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "gtid = ?") {
		t.Errorf("expected gtid = ? in query: %s", q)
	}
	found := false
	for _, a := range args {
		if a == "3e11fa47-71ca-11e1-9e33-c80aa9429562:42" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected GTID string in args, got %v", args)
	}
}

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

// ─── RBAC: buildQuery DenyTables ─────────────────────────────────────────────

func TestBuildQuery_denyTables(t *testing.T) {
	opts := Options{
		DenyTables: []SchemaTable{{Schema: "mydb", Table: "secrets"}},
		Limit:      10,
	}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "NOT (schema_name = ? AND table_name = ?)") {
		t.Errorf("expected NOT deny clause in query: %s", q)
	}
	// Schema and table must appear as consecutive args.
	found := false
	for i, a := range args {
		if a == "mydb" && i+1 < len(args) && args[i+1] == "secrets" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected mydb/secrets in args, got %v", args)
	}
}

func TestBuildQuery_multipleDenyTables(t *testing.T) {
	opts := Options{
		DenyTables: []SchemaTable{
			{Schema: "db1", Table: "t1"},
			{Schema: "db2", Table: "t2"},
		},
	}
	q, args := buildQuery(opts)

	count := strings.Count(q, "NOT (schema_name = ? AND table_name = ?)")
	if count != 2 {
		t.Errorf("expected 2 NOT deny clauses, got %d: %s", count, q)
	}
	if len(args) != 4 {
		t.Errorf("expected 4 args for 2 deny tables, got %d: %v", len(args), args)
	}
}

// ─── RBAC: applyRedaction ─────────────────────────────────────────────────────

func TestApplyRedaction(t *testing.T) {
	rows := []ResultRow{{
		SchemaName: "mydb",
		TableName:  "orders",
		RowBefore:  map[string]any{"amount": float64(100), "status": "paid"},
		RowAfter:   map[string]any{"amount": float64(200), "status": "refunded"},
	}}
	redact := []SchemaTableColumn{
		{Schema: "mydb", Table: "orders", Column: "amount"},
	}
	applyRedaction(rows, redact)

	if rows[0].RowBefore["amount"] != nil {
		t.Errorf("expected RowBefore[amount] to be nil, got %v", rows[0].RowBefore["amount"])
	}
	if rows[0].RowAfter["amount"] != nil {
		t.Errorf("expected RowAfter[amount] to be nil, got %v", rows[0].RowAfter["amount"])
	}
	// Non-redacted column must be preserved.
	if rows[0].RowBefore["status"] != "paid" {
		t.Errorf("expected RowBefore[status]=paid, got %v", rows[0].RowBefore["status"])
	}
	if rows[0].RowAfter["status"] != "refunded" {
		t.Errorf("expected RowAfter[status]=refunded, got %v", rows[0].RowAfter["status"])
	}
}

func TestBuildQuery_pkValuesIn(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", PKValuesIn: []string{"1", "2", "3"}, Limit: 10}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "pk_values IN (?,?,?)") {
		t.Errorf("expected pk_values IN (?,?,?) in query: %s", q)
	}
	if strings.Contains(q, "SHA2") {
		t.Errorf("PKValuesIn must not use the SHA2 single-PK path: %s", q)
	}
	// Args order: schema, table, pk1, pk2, pk3, limit
	wantArgs := []any{"db", "t", "1", "2", "3", 10}
	if fmt.Sprintf("%v", args) != fmt.Sprintf("%v", wantArgs) {
		t.Errorf("args mismatch: got %v want %v", args, wantArgs)
	}
}

func TestBuildQuery_pkValues_winsOver_pkValuesIn(t *testing.T) {
	// PKValues takes precedence so callers that set both (defensive callers)
	// keep the SHA2-indexed fast path. PKValuesIn is silently ignored.
	opts := Options{Schema: "db", Table: "t", PKValues: "42", PKValuesIn: []string{"1", "2"}, Limit: 5}
	q, _ := buildQuery(opts)
	if !strings.Contains(q, "pk_hash = SHA2(?, 256)") {
		t.Errorf("PKValues must take precedence over PKValuesIn: %s", q)
	}
	if strings.Contains(q, "pk_values IN") {
		t.Errorf("PKValuesIn must be ignored when PKValues is set: %s", q)
	}
}

func TestBuildQuery_limitPerPK(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", PKValuesIn: []string{"1", "2"}, LimitPerPK: 1, Limit: 100}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "ROW_NUMBER() OVER (PARTITION BY pk_values") {
		t.Errorf("expected ROW_NUMBER window in query: %s", q)
	}
	if !strings.Contains(q, "ORDER BY event_timestamp DESC, event_id DESC") {
		t.Errorf("expected DESC ordering inside window so latest events are kept: %s", q)
	}
	if !strings.Contains(q, "WHERE bt_rn <= ?") {
		t.Errorf("expected outer WHERE bt_rn <= ?: %s", q)
	}
	// Outer ORDER BY remains ASC for stable global output.
	if !strings.HasSuffix(strings.TrimSpace(strings.Split(q, "ORDER BY event_timestamp,")[1]), "event_id LIMIT ?") {
		t.Errorf("expected outer ORDER BY event_timestamp, event_id ASC: %s", q)
	}
	// Args: schema, table, pk1, pk2, limitPerPK, limit
	wantArgs := []any{"db", "t", "1", "2", 1, 100}
	if fmt.Sprintf("%v", args) != fmt.Sprintf("%v", wantArgs) {
		t.Errorf("args mismatch: got %v want %v", args, wantArgs)
	}
}

func TestLimitPerPK_helper(t *testing.T) {
	ts := func(s string) time.Time {
		t, _ := time.Parse("2006-01-02 15:04:05", s)
		return t
	}
	rows := []ResultRow{
		{EventID: 1, EventTimestamp: ts("2026-04-15 10:00:00"), PKValues: "a"},
		{EventID: 2, EventTimestamp: ts("2026-04-15 10:01:00"), PKValues: "b"},
		{EventID: 3, EventTimestamp: ts("2026-04-15 10:02:00"), PKValues: "a"},
		{EventID: 4, EventTimestamp: ts("2026-04-15 10:03:00"), PKValues: "a"},
		{EventID: 5, EventTimestamp: ts("2026-04-15 10:04:00"), PKValues: "b"},
	}
	got := LimitPerPK(rows, 1)
	if len(got) != 2 {
		t.Fatalf("expected 2 rows after limit-per-pk=1, got %d", len(got))
	}
	// Latest per PK: a→4, b→5; ordering preserved (ASC).
	if got[0].EventID != 4 || got[1].EventID != 5 {
		t.Errorf("expected events [4,5] (latest per PK in ASC order), got %v", []uint64{got[0].EventID, got[1].EventID})
	}
}

func TestLimitPerPK_zero_passthrough(t *testing.T) {
	rows := []ResultRow{{EventID: 1, PKValues: "a"}, {EventID: 2, PKValues: "a"}}
	got := LimitPerPK(rows, 0)
	if len(got) != 2 {
		t.Errorf("LimitPerPK(_, 0) should be a passthrough, got %d rows", len(got))
	}
}

func TestApplyRedaction_wrongTable(t *testing.T) {
	rows := []ResultRow{{
		SchemaName: "mydb",
		TableName:  "products", // different table
		RowBefore:  map[string]any{"amount": float64(50)},
		RowAfter:   map[string]any{"amount": float64(60)},
	}}
	redact := []SchemaTableColumn{
		{Schema: "mydb", Table: "orders", Column: "amount"}, // different table
	}
	applyRedaction(rows, redact)

	// Values must be unchanged — redaction only applies to the matching table.
	if rows[0].RowBefore["amount"] != float64(50) {
		t.Errorf("expected RowBefore[amount]=50, got %v", rows[0].RowBefore["amount"])
	}
	if rows[0].RowAfter["amount"] != float64(60) {
		t.Errorf("expected RowAfter[amount]=60, got %v", rows[0].RowAfter["amount"])
	}
}
