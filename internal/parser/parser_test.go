package parser

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/metadata"
)

// ─── EventType wire-contract pin ──────────────────────────────────────────────

// TestEventType_wireContract pins the integer values of every EventType
// constant. SaaS-side consumers key off these integers across process
// boundaries (dbtrail #1309/#1310), so a silent renumber would corrupt every
// downstream consumer. Failure here means a contributor changed a constant —
// re-check the SaaS ingestion before doing so.
func TestEventType_wireContract(t *testing.T) {
	cases := []struct {
		name string
		got  EventType
		want uint8
	}{
		{"EventInsert", EventInsert, 1},
		{"EventUpdate", EventUpdate, 2},
		{"EventDelete", EventDelete, 3},
		{"EventDDL", EventDDL, 4},
		{"EventGTID", EventGTID, 5},
		{"EventSnapshot", EventSnapshot, 6},
	}
	for _, tc := range cases {
		if uint8(tc.got) != tc.want {
			t.Errorf("%s = %d, want %d (wire contract: SaaS consumers key off this integer)", tc.name, tc.got, tc.want)
		}
	}
}

// ─── ChangedColumns ───────────────────────────────────────────────────────────

func TestChangedColumns_noChange(t *testing.T) {
	before := map[string]any{"id": int64(1), "status": "open", "amount": 9.99}
	after := map[string]any{"id": int64(1), "status": "open", "amount": 9.99}
	got := ChangedColumns(before, after)
	if len(got) != 0 {
		t.Errorf("expected no changed columns, got %v", got)
	}
}

func TestChangedColumns_singleChange(t *testing.T) {
	before := map[string]any{"id": int64(1), "status": "open"}
	after := map[string]any{"id": int64(1), "status": "closed"}
	got := ChangedColumns(before, after)
	if len(got) != 1 || got[0] != "status" {
		t.Errorf("expected [status], got %v", got)
	}
}

func TestChangedColumns_multipleChanges_sorted(t *testing.T) {
	before := map[string]any{"z": 1, "a": 2, "m": 3}
	after := map[string]any{"z": 9, "a": 2, "m": 9}
	got := ChangedColumns(before, after)
	// Must be sorted: [m, z]
	if len(got) != 2 || got[0] != "m" || got[1] != "z" {
		t.Errorf("expected [m z], got %v", got)
	}
}

func TestChangedColumns_nilBefore(t *testing.T) {
	got := ChangedColumns(nil, map[string]any{"id": 1})
	if got != nil {
		t.Errorf("expected nil for nil before image, got %v", got)
	}
}

func TestChangedColumns_nilAfter(t *testing.T) {
	got := ChangedColumns(map[string]any{"id": 1}, nil)
	if got != nil {
		t.Errorf("expected nil for nil after image, got %v", got)
	}
}

// ─── BuildPKValues ────────────────────────────────────────────────────────────

func TestBuildPKValues_singleIntPK(t *testing.T) {
	cols := []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true}}
	row := map[string]any{"id": int64(12345)}
	got := BuildPKValues(cols, row)
	if got != "12345" {
		t.Errorf("expected '12345', got %q", got)
	}
}

func TestBuildPKValues_compositePK(t *testing.T) {
	cols := []metadata.ColumnMeta{
		{Name: "id", OrdinalPosition: 1, IsPK: true},
		{Name: "seq", OrdinalPosition: 2, IsPK: true},
	}
	row := map[string]any{"id": int64(12345), "seq": int64(2)}
	got := BuildPKValues(cols, row)
	if got != "12345|2" {
		t.Errorf("expected '12345|2', got %q", got)
	}
}

func TestBuildPKValues_escapesPipe(t *testing.T) {
	// A PK value that contains a pipe must be escaped.
	cols := []metadata.ColumnMeta{{Name: "code", OrdinalPosition: 1, IsPK: true}}
	row := map[string]any{"code": "a|b"}
	got := BuildPKValues(cols, row)
	if got != `a\|b` {
		t.Errorf(`expected 'a\|b', got %q`, got)
	}
}

func TestBuildPKValues_escapesBackslash(t *testing.T) {
	cols := []metadata.ColumnMeta{{Name: "path", OrdinalPosition: 1, IsPK: true}}
	row := map[string]any{"path": `C:\dir`}
	got := BuildPKValues(cols, row)
	if got != `C:\\dir` {
		t.Errorf(`expected 'C:\\dir', got %q`, got)
	}
}

func TestBuildPKValues_emptyPKColumns(t *testing.T) {
	// Tables without a PK produce an empty string — unusual but must not panic.
	got := BuildPKValues(nil, map[string]any{"id": 1})
	if got != "" {
		t.Errorf("expected empty string for no PK columns, got %q", got)
	}
}

// ─── formatGTID ──────────────────────────────────────────────────────────────

func TestFormatGTID_valid(t *testing.T) {
	// UUID "3e11fa47-71ca-11e1-9e33-c80aa9429562", GNO=42
	sid := []byte{0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42, 0x95, 0x62}
	got := formatGTID(sid, 42)
	want := "3e11fa47-71ca-11e1-9e33-c80aa9429562:42"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestFormatGTID_shortSID(t *testing.T) {
	// Fewer than 16 bytes → GTID not enabled → empty string.
	got := formatGTID([]byte{0x01, 0x02}, 1)
	if got != "" {
		t.Errorf("expected empty string for short SID, got %q", got)
	}
}

// ─── Filters.Matches ─────────────────────────────────────────────────────────

func TestFilters_Matches_noFilters(t *testing.T) {
	f := Filters{} // both nil → accept all
	if !f.Matches("any_schema", "any_table") {
		t.Error("expected match with no filters")
	}
}

func TestFilters_Matches_schemaAccept(t *testing.T) {
	f := Filters{Schemas: map[string]bool{"mydb": true}}
	if !f.Matches("mydb", "orders") {
		t.Error("expected match for schema mydb")
	}
}

func TestFilters_Matches_schemaReject(t *testing.T) {
	f := Filters{Schemas: map[string]bool{"mydb": true}}
	if f.Matches("other", "orders") {
		t.Error("expected reject for schema other")
	}
}

func TestFilters_Matches_tableAccept(t *testing.T) {
	f := Filters{Tables: map[string]bool{"mydb.orders": true}}
	if !f.Matches("mydb", "orders") {
		t.Error("expected match for table mydb.orders")
	}
}

func TestFilters_Matches_tableReject(t *testing.T) {
	f := Filters{Tables: map[string]bool{"mydb.orders": true}}
	if f.Matches("mydb", "items") {
		t.Error("expected reject for table mydb.items")
	}
}

func TestFilters_Matches_bothFilters(t *testing.T) {
	f := Filters{
		Schemas: map[string]bool{"mydb": true},
		Tables:  map[string]bool{"mydb.orders": true},
	}
	// Both match
	if !f.Matches("mydb", "orders") {
		t.Error("expected match for mydb.orders with both filters")
	}
	// Schema matches but table doesn't
	if f.Matches("mydb", "items") {
		t.Error("expected reject: schema matches but table doesn't")
	}
	// Neither matches
	if f.Matches("other", "orders") {
		t.Error("expected reject: schema doesn't match")
	}
}

// ─── parseDDL ─────────────────────────────────────────────────────────────────

// newTestLogger returns a slog.Logger that writes text output to buf.
func newTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestParseDDL_ddlStatements(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		query   string
		ddlType DDLKind
		schema  string
		table   string
	}{
		{"ALTER TABLE orders ADD COLUMN foo INT", DDLAlterTable, "", "orders"},
		{"CREATE TABLE new_tbl (id INT)", DDLCreateTable, "", "new_tbl"},
		{"DROP TABLE old_tbl", DDLDropTable, "", "old_tbl"},
		{"RENAME TABLE a TO b", DDLRenameTable, "", "a"},
		{"ALTER TABLE mydb.orders ADD COLUMN foo INT", DDLAlterTable, "mydb", "orders"},
		{"ALTER TABLE `mydb`.`orders` ADD COLUMN foo INT", DDLAlterTable, "mydb", "orders"},
		{"DROP TABLE IF EXISTS old_tbl", DDLDropTable, "", "old_tbl"},
		{"CREATE TABLE IF NOT EXISTS `mydb`.`new_tbl` (id INT)", DDLCreateTable, "mydb", "new_tbl"},
		{"TRUNCATE orders", DDLTruncateTable, "", "orders"},
		{"TRUNCATE TABLE orders", DDLTruncateTable, "", "orders"},
		{"TRUNCATE TABLE mydb.orders", DDLTruncateTable, "mydb", "orders"},
		{"TRUNCATE `mydb`.`orders`", DDLTruncateTable, "mydb", "orders"},
	}

	for _, tt := range tests {
		buf.Reset()
		ev, ok := parseDDL(logger, "binlog.000001", 100, ts, "uuid:1", tt.query, 0)
		if !ok {
			t.Errorf("parseDDL(%q) returned false, want true", tt.query)
			continue
		}
		if ev.DDLType != tt.ddlType {
			t.Errorf("parseDDL(%q).DDLType = %q, want %q", tt.query, ev.DDLType, tt.ddlType)
		}
		if ev.Schema != tt.schema {
			t.Errorf("parseDDL(%q).Schema = %q, want %q", tt.query, ev.Schema, tt.schema)
		}
		if ev.Table != tt.table {
			t.Errorf("parseDDL(%q).Table = %q, want %q", tt.query, ev.Table, tt.table)
		}
		if ev.EventType != EventDDL {
			t.Errorf("parseDDL(%q).EventType = %d, want %d", tt.query, ev.EventType, EventDDL)
		}
		if ev.DDLQuery != tt.query {
			t.Errorf("parseDDL(%q).DDLQuery = %q, want same", tt.query, ev.DDLQuery)
		}
		if ev.GTID != "uuid:1" {
			t.Errorf("parseDDL(%q).GTID = %q, want %q", tt.query, ev.GTID, "uuid:1")
		}
	}
}

func TestParseDDL_nonDDL(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	nonDDL := []string{
		"BEGIN",
		"COMMIT",
		"INSERT INTO orders VALUES (1)",
		"UPDATE orders SET status = 'done'",
		"SELECT 1",
	}
	for _, stmt := range nonDDL {
		buf.Reset()
		_, ok := parseDDL(logger, "binlog.000001", 100, ts, "", stmt, 0)
		if ok {
			t.Errorf("parseDDL(%q) returned true, want false", stmt)
		}
	}
}

func TestParseDDL_caseInsensitive(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	ev, ok := parseDDL(logger, "binlog.000001", 100, ts, "", "alter table orders add column x int", 0)
	if !ok {
		t.Errorf("parseDDL(lowercase ALTER TABLE) returned false, want true")
	}
	if !strings.Contains(buf.String(), "DDL detected") {
		t.Errorf("expected DDL warning for lowercase DDL, got: %q", buf.String())
	}
	if ev.Table != "orders" {
		t.Errorf("parseDDL(lowercase).Table = %q, want %q", ev.Table, "orders")
	}
}

// ─── SwapResolver + schemaVersion ──────────────────────────────────────────────

func TestParser_SwapResolver_updatesSchemaVersion(t *testing.T) {
	r1 := metadata.NewResolverFromTables(5, nil)
	p := New("/tmp", r1, Filters{}, nil)

	if got := p.schemaVersion.Load(); got != 5 {
		t.Fatalf("initial schemaVersion = %d, want 5", got)
	}

	r2 := metadata.NewResolverFromTables(12, nil)
	p.SwapResolver(r2)

	if got := p.schemaVersion.Load(); got != 12 {
		t.Errorf("after SwapResolver schemaVersion = %d, want 12", got)
	}
}

func TestStreamParser_SwapResolver_updatesSchemaVersion(t *testing.T) {
	r1 := metadata.NewResolverFromTables(3, nil)
	sp := NewStreamParser(r1, Filters{}, nil)

	if got := sp.schemaVersion.Load(); got != 3 {
		t.Fatalf("initial schemaVersion = %d, want 3", got)
	}

	r2 := metadata.NewResolverFromTables(7, nil)
	sp.SwapResolver(r2)

	if got := sp.schemaVersion.Load(); got != 7 {
		t.Errorf("after SwapResolver schemaVersion = %d, want 7", got)
	}
}

func TestParser_nilResolver_schemaVersionZero(t *testing.T) {
	p := New("/tmp", nil, Filters{}, nil)
	if got := p.schemaVersion.Load(); got != 0 {
		t.Errorf("nil resolver schemaVersion = %d, want 0", got)
	}
}

func TestStreamParser_nilResolver_schemaVersionZero(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	if got := sp.schemaVersion.Load(); got != 0 {
		t.Errorf("nil resolver schemaVersion = %d, want 0", got)
	}
}

// ─── Timestamp UTC ────────────────────────────────────────────────────────────

func TestTimestampUTC(t *testing.T) {
	epoch := int64(1_700_000_000)
	ts := time.Unix(epoch, 0).UTC()
	if ts.Location() != time.UTC {
		t.Errorf("expected UTC location, got %v", ts.Location())
	}
	if ts.Unix() != epoch {
		t.Errorf("expected epoch %d, got %d", epoch, ts.Unix())
	}
}
