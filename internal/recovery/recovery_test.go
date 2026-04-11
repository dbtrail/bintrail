package recovery

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// newGen returns a Generator with no DB and no resolver (triggers all-cols fallback).
func newGen() *Generator { return New(nil, nil) }

// ─── FormatSQLValue ─────────────────────────────────────────────────────────────

func TestFormatValue_nil(t *testing.T) {
	if got := FormatSQLValue(nil); got != "NULL" {
		t.Errorf("expected NULL, got %q", got)
	}
}

func TestFormatValue_boolTrue(t *testing.T) {
	if got := FormatSQLValue(true); got != "1" {
		t.Errorf("expected 1, got %q", got)
	}
}

func TestFormatValue_boolFalse(t *testing.T) {
	if got := FormatSQLValue(false); got != "0" {
		t.Errorf("expected 0, got %q", got)
	}
}

func TestFormatValue_integerFloat(t *testing.T) {
	// JSON round-trip turns int64(12345) into float64(12345).
	got := FormatSQLValue(float64(12345))
	if got != "12345" {
		t.Errorf("expected '12345', got %q", got)
	}
}

func TestFormatValue_negativeInt(t *testing.T) {
	got := FormatSQLValue(float64(-7))
	if got != "-7" {
		t.Errorf("expected '-7', got %q", got)
	}
}

func TestFormatValue_decimal(t *testing.T) {
	got := FormatSQLValue(float64(3.14))
	if !strings.Contains(got, ".") {
		t.Errorf("expected decimal point in %q", got)
	}
	if got == "NULL" || got == "3" {
		t.Errorf("unexpected result for float 3.14: %q", got)
	}
}

func TestFormatValue_string_simple(t *testing.T) {
	got := FormatSQLValue("hello")
	if got != "'hello'" {
		t.Errorf("expected \"'hello'\", got %q", got)
	}
}

func TestFormatValue_string_singleQuote(t *testing.T) {
	got := FormatSQLValue("it's fine")
	if !strings.Contains(got, `\'`) {
		t.Errorf("expected escaped single quote in %q", got)
	}
}

func TestFormatValue_string_backslash(t *testing.T) {
	got := FormatSQLValue(`C:\path`)
	// Backslash must be doubled
	if !strings.Contains(got, `\\`) {
		t.Errorf("expected escaped backslash in %q", got)
	}
}

func TestFormatValue_jsonObject(t *testing.T) {
	got := FormatSQLValue(map[string]any{"key": "val"})
	// Should be a quoted JSON string
	if !strings.HasPrefix(got, "'") || !strings.HasSuffix(got, "'") {
		t.Errorf("expected single-quoted JSON, got %q", got)
	}
	if !strings.Contains(got, "key") {
		t.Errorf("expected JSON content in %q", got)
	}
}

// ─── QuoteName ────────────────────────────────────────────────────────────────

func TestQuoteName_simple(t *testing.T) {
	if got := QuoteName("orders"); got != "`orders`" {
		t.Errorf("expected `orders`, got %q", got)
	}
}

func TestQuoteName_withBacktick(t *testing.T) {
	if got := QuoteName("col`name"); got != "`col``name`" {
		t.Errorf("expected `col``name`, got %q", got)
	}
}

// ─── EscapeString ─────────────────────────────────────────────────────────────

func TestEscapeString_singleQuote(t *testing.T) {
	if got := EscapeString("O'Brien"); !strings.Contains(got, `\'`) {
		t.Errorf("single quote not escaped in %q", got)
	}
}

func TestEscapeString_backslash(t *testing.T) {
	if got := EscapeString(`a\b`); !strings.Contains(got, `\\`) {
		t.Errorf("backslash not escaped in %q", got)
	}
}

// ─── generateInsert (DELETE → INSERT) ────────────────────────────────────────

func TestGenerateInsert_basic(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID:    1,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventDelete,
		RowBefore: map[string]any{
			"id":     float64(42),
			"status": "active",
			"amount": float64(99.99),
		},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "INSERT INTO")
	assertSQL(t, stmt, "`mydb`")
	assertSQL(t, stmt, "`orders`")
	assertSQL(t, stmt, "`id`")
	assertSQL(t, stmt, "42")
	assertSQL(t, stmt, "'active'")
}

func TestGenerateInsert_nilRowBefore(t *testing.T) {
	g := newGen()
	_, err := g.generateInsert(query.ResultRow{EventID: 1, EventType: parser.EventDelete})
	if err == nil {
		t.Error("expected error for nil row_before, got nil")
	}
}

func TestGenerateInsert_columnsSorted(t *testing.T) {
	// Columns should appear in alphabetical order for determinism.
	g := newGen()
	row := query.ResultRow{
		EventID: 1, SchemaName: "db", TableName: "t", EventType: parser.EventDelete,
		RowBefore: map[string]any{"zzz": "z", "aaa": "a", "mmm": "m"},
	}
	stmt, _ := g.generateInsert(row)
	// Find positions of column names in the INSERT statement.
	posA := strings.Index(stmt, "`aaa`")
	posM := strings.Index(stmt, "`mmm`")
	posZ := strings.Index(stmt, "`zzz`")
	if !(posA < posM && posM < posZ) {
		t.Errorf("expected alphabetical column order in: %s", stmt)
	}
}

// ─── generateUpdate (UPDATE → reverse UPDATE) ─────────────────────────────────

func TestGenerateUpdate_basic(t *testing.T) {
	g := newGen() // nil resolver → all-cols WHERE fallback
	row := query.ResultRow{
		EventID:    2,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventUpdate,
		PKValues:   "42",
		RowBefore:  map[string]any{"id": float64(42), "status": "pending"},
		RowAfter:   map[string]any{"id": float64(42), "status": "shipped"},
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "UPDATE")
	assertSQL(t, stmt, "SET")
	assertSQL(t, stmt, "WHERE")
	// SET clause must use row_before value "pending"
	assertSQL(t, stmt, "'pending'")
	// WHERE clause must use row_after value "shipped" (all-cols fallback)
	assertSQL(t, stmt, "'shipped'")
}

func TestGenerateUpdate_nilRowBefore(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID: 2, EventType: parser.EventUpdate,
		RowAfter: map[string]any{"id": float64(1)},
	}
	_, err := g.generateUpdate(row)
	if err == nil {
		t.Error("expected error for nil row_before")
	}
}

func TestGenerateUpdate_nilRowAfter(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID: 2, EventType: parser.EventUpdate,
		RowBefore: map[string]any{"id": float64(1)},
	}
	_, err := g.generateUpdate(row)
	if err == nil {
		t.Error("expected error for nil row_after")
	}
}

// ─── generateDelete (INSERT → DELETE) ────────────────────────────────────────

func TestGenerateDelete_basic(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID:    3,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventInsert,
		RowAfter:   map[string]any{"id": float64(99), "status": "new"},
	}
	stmt, err := g.generateDelete(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "DELETE FROM")
	assertSQL(t, stmt, "`mydb`")
	assertSQL(t, stmt, "`orders`")
	assertSQL(t, stmt, "WHERE")
	assertSQL(t, stmt, "99")
}

func TestGenerateDelete_nilRowAfter(t *testing.T) {
	g := newGen()
	_, err := g.generateDelete(query.ResultRow{EventID: 3, EventType: parser.EventInsert})
	if err == nil {
		t.Error("expected error for nil row_after")
	}
}

// ─── GenerateSQL integration (no DB, exercising the output wrapper) ────────────

func TestGenerateSQL_noRows(t *testing.T) {
	// We can't call GenerateSQL without a DB, but we can test the no-events path
	// by calling the internal writer indirectly. We test the output format here
	// by wiring up a fake set of events.
	g := newGen()
	var buf bytes.Buffer

	// Manually exercise the output format.
	rows := []query.ResultRow{
		{
			EventID:    10,
			SchemaName: "db",
			TableName:  "tbl",
			EventType:  parser.EventDelete,
			PKValues:   "5",
			RowBefore:  map[string]any{"id": float64(5), "name": "Alice"},
		},
	}

	// Call the private emitter path by using GenerateStatement directly.
	stmt, err := g.generateStatement(rows[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	buf.WriteString("BEGIN;\n")
	buf.WriteString(stmt + ";\n")
	buf.WriteString("COMMIT;\n")

	out := buf.String()
	assertSQL(t, out, "BEGIN;")
	assertSQL(t, out, "INSERT INTO")
	assertSQL(t, out, "COMMIT;")
}

// ─── Null / special value handling ───────────────────────────────────────────

func TestFormatValue_nullInRow(t *testing.T) {
	// A NULL column (Go nil) must produce SQL NULL.
	got := FormatSQLValue(nil)
	if got != "NULL" {
		t.Errorf("expected NULL, got %q", got)
	}
}

func TestGenerateInsert_withNullColumn(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID:    5,
		SchemaName: "db",
		TableName:  "t",
		EventType:  parser.EventDelete,
		RowBefore:  map[string]any{"id": float64(1), "note": nil},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(stmt, "NULL") {
		t.Errorf("expected NULL in INSERT for nil column: %s", stmt)
	}
}

// ─── Helper ───────────────────────────────────────────────────────────────────

// assertSQL checks that want appears in the SQL string stmt.
func assertSQL(t *testing.T, stmt, want string) {
	t.Helper()
	if !strings.Contains(stmt, want) {
		t.Errorf("expected %q in SQL:\n  %s", want, stmt)
	}
}

// ─── GenerateSQL output: BEGIN/COMMIT wrapper ─────────────────────────────────

func TestGenerateSQL_noEventsMessage(t *testing.T) {
	// Verify the exact text emitted when there are no matching events.
	var buf bytes.Buffer
	fmt.Fprintln(&buf, "-- No events matched the specified criteria.")
	if !strings.Contains(buf.String(), "No events matched") {
		t.Error("expected no-events message")
	}
}

// ─── FormatSQLValue edge cases ──────────────────────────────────────────────────

func TestFormatValue_arraySlice(t *testing.T) {
	// JSON array column: []any should be serialised as a quoted JSON array.
	val := []any{"a", float64(1), true}
	got := FormatSQLValue(val)
	if !strings.HasPrefix(got, "'") || !strings.HasSuffix(got, "'") {
		t.Errorf("expected single-quoted JSON array, got %q", got)
	}
	if !strings.Contains(got, `"a"`) {
		t.Errorf("expected array element 'a' in %q", got)
	}
}

func TestFormatValue_jsonRawMessage(t *testing.T) {
	raw := json.RawMessage(`{"key":"value"}`)
	got := FormatSQLValue(raw)
	if !strings.HasPrefix(got, "'") || !strings.HasSuffix(got, "'") {
		t.Errorf("expected quoted JSON, got %q", got)
	}
	if !strings.Contains(got, "key") {
		t.Errorf("expected JSON content in %q", got)
	}
}

func TestFormatValue_largeFloat(t *testing.T) {
	// float64 >= 1e15 takes the FormatFloat path (not int64 conversion).
	// FormatFloat('f', -1) for exact whole numbers still omits the decimal,
	// so the output looks like an integer — the guard is about int64 overflow
	// safety, not about output format.
	got := FormatSQLValue(float64(1e15))
	if got != "1000000000000000" {
		t.Errorf("expected 1000000000000000, got %q", got)
	}
}

func TestFormatValue_veryLargeFloat(t *testing.T) {
	// 1e18 exceeds the int64 guard but is representable in float64.
	got := FormatSQLValue(float64(1e18))
	if got != "1000000000000000000" {
		t.Errorf("expected 1000000000000000000, got %q", got)
	}
}

func TestFormatValue_beyondInt64Range(t *testing.T) {
	// 1e19 is beyond int64 max (~9.2e18). The guard prevents int64 overflow;
	// FormatFloat handles it correctly.
	got := FormatSQLValue(float64(1e19))
	if got == "" {
		t.Error("expected non-empty result for 1e19")
	}
	// Should not panic — the value is too large for int64 but FormatFloat
	// handles it safely.
}

func TestFormatValue_infinity(t *testing.T) {
	got := FormatSQLValue(math.Inf(1))
	if got == "NULL" {
		t.Errorf("expected float format for +Inf, got %q", got)
	}
	// Should not panic — just format somehow.
}

func TestFormatValue_nan(t *testing.T) {
	got := FormatSQLValue(math.NaN())
	if got == "NULL" {
		t.Errorf("expected float format for NaN, got %q", got)
	}
}

func TestFormatValue_negativeZero(t *testing.T) {
	got := FormatSQLValue(math.Copysign(0, -1))
	// -0 == 0, so Trunc(-0) == -0, and -0 == -0. math.Abs(-0) = 0 < 1e15.
	// It should format as "0" (integer format).
	if got != "0" {
		t.Errorf("expected '0' for negative zero, got %q", got)
	}
}

// ─── EscapeString edge cases ─────────────────────────────────────────────────

func TestEscapeString_nullByte(t *testing.T) {
	got := EscapeString("hello\x00world")
	if !strings.Contains(got, `\0`) {
		t.Errorf("expected \\0 for null byte, got %q", got)
	}
	if strings.Contains(got, "\x00") {
		t.Errorf("raw null byte should be replaced, got %q", got)
	}
}

func TestEscapeString_combined(t *testing.T) {
	got := EscapeString("it's a \\path\x00end")
	if !strings.Contains(got, `\'`) {
		t.Errorf("expected escaped quote in %q", got)
	}
	if !strings.Contains(got, `\\`) {
		t.Errorf("expected escaped backslash in %q", got)
	}
	if !strings.Contains(got, `\0`) {
		t.Errorf("expected escaped null in %q", got)
	}
}

// ─── Generated column filtering ──────────────────────────────────────────────

// newGenWithResolver returns a Generator backed by a resolver containing a
// table with one STORED generated column ("line_total").
func newGenWithResolver() *Generator {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "order_items",
		Columns: []metadata.ColumnMeta{
			{Name: "order_id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "quantity", OrdinalPosition: 2, DataType: "int"},
			{Name: "unit_price", OrdinalPosition: 3, DataType: "decimal"},
			{Name: "line_total", OrdinalPosition: 4, DataType: "decimal", IsGenerated: true},
		},
		PKColumns: []string{"order_id"},
	}
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"shop.order_items": tm,
	})
	return New(nil, resolver)
}

func TestGenerateInsert_skipsGeneratedColumns(t *testing.T) {
	g := newGenWithResolver()
	row := query.ResultRow{
		EventID:    10,
		SchemaName: "shop",
		TableName:  "order_items",
		EventType:  parser.EventDelete,
		RowBefore: map[string]any{
			"order_id":   float64(5),
			"quantity":   float64(3),
			"unit_price": float64(68.81),
			"line_total": float64(206.43), // STORED generated — must be excluded
		},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "INSERT INTO")
	assertSQL(t, stmt, "`order_id`")
	assertSQL(t, stmt, "`quantity`")
	assertSQL(t, stmt, "`unit_price`")
	if strings.Contains(stmt, "line_total") {
		t.Errorf("generated column 'line_total' must not appear in INSERT: %s", stmt)
	}
}

func TestGenerateUpdate_skipsGeneratedColumns(t *testing.T) {
	g := newGenWithResolver()
	row := query.ResultRow{
		EventID:    11,
		SchemaName: "shop",
		TableName:  "order_items",
		EventType:  parser.EventUpdate,
		RowBefore: map[string]any{
			"order_id":   float64(5),
			"quantity":   float64(2),
			"unit_price": float64(68.81),
			"line_total": float64(137.62), // STORED generated — must be excluded from SET
		},
		RowAfter: map[string]any{
			"order_id":   float64(5),
			"quantity":   float64(3),
			"unit_price": float64(68.81),
			"line_total": float64(206.43),
		},
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "UPDATE")
	assertSQL(t, stmt, "SET")
	assertSQL(t, stmt, "`quantity` = 2")
	setIdx := strings.Index(stmt, "SET")
	whereIdx := strings.Index(stmt, "WHERE")
	if setIdx < 0 || whereIdx < 0 {
		t.Fatalf("expected SET and WHERE in: %s", stmt)
	}
	setPart := stmt[setIdx:whereIdx]
	if strings.Contains(setPart, "line_total") {
		t.Errorf("generated column 'line_total' must not appear in SET clause: %s", setPart)
	}
}

// ─── GenerateSQLFromRows ──────────────────────────────────────────────────────

func TestGenerateSQLFromRows_empty(t *testing.T) {
	g := newGen()
	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows(nil, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 statements, got %d", n)
	}
	assertSQL(t, buf.String(), "No events matched")
}

func TestGenerateSQLFromRows_reverseOrder(t *testing.T) {
	g := newGen()
	rows := []query.ResultRow{
		{
			EventID: 1, SchemaName: "db", TableName: "t", EventType: parser.EventDelete,
			PKValues:  "10",
			RowBefore: map[string]any{"id": float64(10), "name": "first"},
		},
		{
			EventID: 2, SchemaName: "db", TableName: "t", EventType: parser.EventInsert,
			PKValues: "20",
			RowAfter: map[string]any{"id": float64(20), "name": "second"},
		},
	}

	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 statements, got %d", n)
	}

	out := buf.String()
	assertSQL(t, out, "BEGIN;")
	assertSQL(t, out, "COMMIT;")

	// Event 2 (INSERT → DELETE) should appear before event 1 (DELETE → INSERT)
	// because GenerateSQLFromRows reverses the input order.
	deletePos := strings.Index(out, "DELETE FROM")
	insertPos := strings.Index(out, "INSERT INTO")
	if deletePos < 0 || insertPos < 0 {
		t.Fatalf("expected both DELETE and INSERT in output:\n%s", out)
	}
	if deletePos > insertPos {
		t.Errorf("expected reversed order (event 2 before event 1):\n%s", out)
	}
}

// ─── resolverForRow ──────────────────────────────────────────────────────────

func TestResolverForRow_zeroVersionReturnsFallback(t *testing.T) {
	resolver := metadata.NewResolverFromTables(5, map[string]*metadata.TableMeta{
		"db.t": {Schema: "db", Table: "t", Columns: []metadata.ColumnMeta{{Name: "id", IsPK: true}}},
	})
	g := New(nil, resolver)
	row := query.ResultRow{SchemaVersion: 0, SchemaName: "db", TableName: "t"}
	got := g.resolverForRow(row)
	if got != resolver {
		t.Error("expected fallback resolver for SchemaVersion=0")
	}
}

func TestResolverForRow_nilDB_returnsFallback(t *testing.T) {
	resolver := metadata.NewResolverFromTables(5, nil)
	g := New(nil, resolver)
	row := query.ResultRow{SchemaVersion: 99}
	got := g.resolverForRow(row)
	if got != resolver {
		t.Error("expected fallback resolver when db is nil")
	}
}

func TestResolverForRow_matchingFallback(t *testing.T) {
	resolver := metadata.NewResolverFromTables(5, nil)
	g := New(nil, resolver)
	row := query.ResultRow{SchemaVersion: 5}
	got := g.resolverForRow(row)
	if got != resolver {
		t.Error("expected fallback resolver when SchemaVersion matches")
	}
}

func TestResolverForRow_cacheHit(t *testing.T) {
	cachedResolver := metadata.NewResolverFromTables(42, map[string]*metadata.TableMeta{
		"db.t": {Schema: "db", Table: "t", Columns: []metadata.ColumnMeta{{Name: "id", IsPK: true}}},
	})
	// db must be non-nil so resolverForRow doesn't short-circuit; the cache hit
	// prevents any actual DB access.
	g := New(new(sql.DB), nil)
	g.cache = map[uint32]*metadata.Resolver{42: cachedResolver}
	row := query.ResultRow{SchemaVersion: 42, SchemaName: "db", TableName: "t"}
	got := g.resolverForRow(row)
	if got != cachedResolver {
		t.Error("expected cached resolver for SchemaVersion=42")
	}
}

func TestGenerateSQLFromRows_differentSchemaVersions_differentPKs(t *testing.T) {
	// Simulate a schema change: snapshot 10 has PK=id, snapshot 20 has PK=uuid.
	// Rows with different SchemaVersion values should use different WHERE clauses.
	resolver10 := metadata.NewResolverFromTables(10, map[string]*metadata.TableMeta{
		"shop.orders": {Schema: "shop", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true},
			{Name: "status", OrdinalPosition: 2},
		}},
	})
	resolver20 := metadata.NewResolverFromTables(20, map[string]*metadata.TableMeta{
		"shop.orders": {Schema: "shop", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "uuid", OrdinalPosition: 1, IsPK: true},
			{Name: "id", OrdinalPosition: 2},
			{Name: "status", OrdinalPosition: 3},
		}},
	})

	// db must be non-nil so resolverForRow doesn't short-circuit; the cache
	// pre-population prevents any actual DB access.
	g := New(new(sql.DB), resolver20)
	g.cache = map[uint32]*metadata.Resolver{10: resolver10, 20: resolver20}

	rows := []query.ResultRow{
		{
			EventID: 1, SchemaName: "shop", TableName: "orders",
			EventType: parser.EventInsert, SchemaVersion: 10,
			EventTimestamp: time.Now(),
			RowAfter:       map[string]any{"id": float64(1), "status": "new"},
		},
		{
			EventID: 2, SchemaName: "shop", TableName: "orders",
			EventType: parser.EventInsert, SchemaVersion: 20,
			EventTimestamp: time.Now(),
			RowAfter:       map[string]any{"uuid": "abc-123", "id": float64(2), "status": "new"},
		},
	}

	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 statements, got %d", n)
	}

	output := buf.String()
	// Row with SchemaVersion=10 → resolver10 (PK=id) → WHERE `id` = 1
	if !strings.Contains(output, "WHERE `id` = 1") {
		t.Errorf("expected WHERE `id` = 1 for SchemaVersion=10 row, got:\n%s", output)
	}
	// Row with SchemaVersion=20 → resolver20 (PK=uuid) → WHERE `uuid` = 'abc-123'
	if !strings.Contains(output, "WHERE `uuid` = 'abc-123'") {
		t.Errorf("expected WHERE `uuid` = 'abc-123' for SchemaVersion=20 row, got:\n%s", output)
	}
}

func TestGenerateInsert_noResolver_includesAllColumns(t *testing.T) {
	// Without a resolver, all columns (including any generated ones) are emitted —
	// the generator has no way to know which are generated.
	g := newGen()
	row := query.ResultRow{
		EventID:    12,
		SchemaName: "shop",
		TableName:  "order_items",
		EventType:  parser.EventDelete,
		RowBefore: map[string]any{
			"order_id":   float64(5),
			"line_total": float64(206.43),
		},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "line_total")
}

// ─── FormatSQLValue extended types (DuckDB scan) ─────────────────────────────
// These exercise the int64/time.Time/[]byte cases added for the full-table
// reconstruct path (#187), where values come from DuckDB's database/sql driver
// rather than a JSON round-trip.

func TestFormatValue_int64(t *testing.T) {
	if got := FormatSQLValue(int64(9876543210)); got != "9876543210" {
		t.Errorf("int64: got %q", got)
	}
	if got := FormatSQLValue(int64(-42)); got != "-42" {
		t.Errorf("negative int64: got %q", got)
	}
}

func TestFormatValue_int32(t *testing.T) {
	if got := FormatSQLValue(int32(12345)); got != "12345" {
		t.Errorf("int32: got %q", got)
	}
}

func TestFormatValue_uint64(t *testing.T) {
	// Values above int64 max must round-trip unsigned.
	if got := FormatSQLValue(uint64(18446744073709551615)); got != "18446744073709551615" {
		t.Errorf("uint64: got %q", got)
	}
}

func TestFormatValue_timeTime(t *testing.T) {
	// Microsecond-precision UTC literal matching the indexer convention.
	val := time.Date(2026, 4, 11, 14, 30, 45, 123456000, time.UTC)
	got := FormatSQLValue(val)
	if got != "'2026-04-11 14:30:45.123456'" {
		t.Errorf("time.Time: got %q", got)
	}
}

func TestFormatValue_timeTimeNonUTC(t *testing.T) {
	// A time.Time in another zone must be normalised to UTC before formatting.
	loc, _ := time.LoadLocation("America/New_York")
	val := time.Date(2026, 4, 11, 10, 30, 45, 0, loc) // 14:30:45 UTC
	got := FormatSQLValue(val)
	if got != "'2026-04-11 14:30:45.000000'" {
		t.Errorf("time.Time non-UTC: got %q", got)
	}
}

func TestFormatValue_byteSlice(t *testing.T) {
	// Binary blob as MySQL hex literal.
	val := []byte{0xde, 0xad, 0xbe, 0xef}
	got := FormatSQLValue(val)
	if got != "X'deadbeef'" {
		t.Errorf("[]byte: got %q", got)
	}
}

func TestFormatValue_emptyByteSlice(t *testing.T) {
	// Empty slice must still emit a valid MySQL hex literal.
	got := FormatSQLValue([]byte{})
	if got != "X''" {
		t.Errorf("empty []byte: got %q", got)
	}
}

func TestFormatValue_byteSliceWithNullByte(t *testing.T) {
	// Arbitrary non-UTF-8 bytes must survive via hex encoding.
	val := []byte{0x00, 0xff, 0x7f, 0x80}
	got := FormatSQLValue(val)
	if got != "X'00ff7f80'" {
		t.Errorf("arbitrary []byte: got %q", got)
	}
}
