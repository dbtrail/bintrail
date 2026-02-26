package parser

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/bintrail/bintrail/internal/metadata"
)

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

// ─── warnOnDDL ────────────────────────────────────────────────────────────────

// newTestLogger returns a slog.Logger that writes text output to buf.
func newTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestWarnOnDDL_ddlStatements(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)

	ddlStmts := []string{
		"ALTER TABLE orders ADD COLUMN foo INT",
		"CREATE TABLE new_tbl (id INT)",
		"DROP TABLE old_tbl",
		"TRUNCATE orders",
		"RENAME TABLE a TO b",
	}
	for _, stmt := range ddlStmts {
		buf.Reset()
		warnOnDDL(logger, "binlog.000001", 100, stmt)
		if buf.Len() == 0 {
			t.Errorf("expected warning for DDL %q, got none", stmt)
		}
	}
}

func TestWarnOnDDL_nonDDL(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)

	nonDDL := []string{
		"BEGIN",
		"COMMIT",
		"INSERT INTO orders VALUES (1)",
		"UPDATE orders SET status = 'done'",
		"SELECT 1",
	}
	for _, stmt := range nonDDL {
		buf.Reset()
		warnOnDDL(logger, "binlog.000001", 100, stmt)
		if buf.Len() != 0 {
			t.Errorf("expected no warning for non-DDL %q, got: %s", stmt, buf.String())
		}
	}
}

func TestWarnOnDDL_caseInsensitive(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)

	warnOnDDL(logger, "binlog.000001", 100, "alter table orders add column x int")
	if !strings.Contains(buf.String(), "DDL detected") {
		t.Errorf("expected DDL warning for lowercase DDL, got: %q", buf.String())
	}
}
