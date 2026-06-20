package baseline

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver — exercises the real read path
	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/recovery"
)

// TestParseSchemaUnsigned pins the colRe attribute-tail match: the UNSIGNED flag
// must be detected only when it follows the type token (optionally after a
// display width), and never from a column name or comment that merely contains
// the word "unsigned" (issue #506).
func TestParseSchemaUnsigned(t *testing.T) {
	const schema = "CREATE TABLE `t` (\n" +
		"  `a` int unsigned NOT NULL,\n" +
		"  `b` int(10) unsigned NOT NULL,\n" +
		"  `c` bigint unsigned NOT NULL,\n" +
		"  `d` bigint unsigned zerofill NOT NULL,\n" +
		"  `e` int NOT NULL,\n" +
		"  `f` bigint NOT NULL,\n" +
		"  `is_unsigned` tinyint NOT NULL,\n" +
		"  `note` varchar(64) DEFAULT NULL COMMENT 'unsigned counter',\n" +
		"  PRIMARY KEY (`a`)\n" +
		") ENGINE=InnoDB;\n"

	dir := t.TempDir()
	path := filepath.Join(dir, "shop.t-schema.sql")
	if err := os.WriteFile(path, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := ParseSchema(path)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}

	want := []struct {
		name      string
		mysqlType string
		unsigned  bool
	}{
		{"a", "int", true},
		{"b", "int", true},
		{"c", "bigint", true},
		{"d", "bigint", true},
		{"e", "int", false},
		{"f", "bigint", false},
		{"is_unsigned", "tinyint", false}, // name contains "unsigned" — must NOT match
		{"note", "varchar", false},        // COMMENT contains "unsigned" — must NOT match
	}
	if len(cols) != len(want) {
		t.Fatalf("got %d columns, want %d: %+v", len(cols), len(want), cols)
	}
	for i, w := range want {
		if cols[i].Name != w.name {
			t.Errorf("col[%d].Name = %q, want %q", i, cols[i].Name, w.name)
		}
		if cols[i].MySQLType != w.mysqlType {
			t.Errorf("col[%d].MySQLType = %q, want %q", i, cols[i].MySQLType, w.mysqlType)
		}
		if cols[i].Unsigned != w.unsigned {
			t.Errorf("col[%d] (%s) Unsigned = %v, want %v", i, w.name, cols[i].Unsigned, w.unsigned)
		}
	}
}

// TestConvertValueUnsigned verifies that UNSIGNED integers at the top of their
// range convert without overflowing into the convertValue error path (which,
// post-fix, aborts the write instead of silently NULLing — issue #506/#503).
func TestConvertValueUnsigned(t *testing.T) {
	cases := []struct {
		name      string
		mysqlType string
		raw       string
		check     func(*testing.T, parquet.Value)
	}{
		{"int_unsigned_max", "int", "4294967295", func(t *testing.T, v parquet.Value) {
			if v.Int64() != 4294967295 {
				t.Errorf("got %d, want 4294967295", v.Int64())
			}
		}},
		{"int_unsigned_mid", "int", "3000000000", func(t *testing.T, v parquet.Value) {
			if v.Int64() != 3000000000 {
				t.Errorf("got %d, want 3000000000", v.Int64())
			}
		}},
		{"bigint_unsigned_max", "bigint", "18446744073709551615", func(t *testing.T, v parquet.Value) {
			// Stored as the int64 bit pattern; read back via uint64 reinterpret.
			if got := uint64(v.Int64()); got != 18446744073709551615 {
				t.Errorf("got %d, want 18446744073709551615", got)
			}
		}},
		{"bigint_unsigned_mid", "bigint", "10000000000000000000", func(t *testing.T, v parquet.Value) {
			if got := uint64(v.Int64()); got != 10000000000000000000 {
				t.Errorf("got %d, want 10000000000000000000", got)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := Column{Name: "c", MySQLType: tc.mysqlType, Unsigned: true}
			v, err := convertValue(col, tc.raw)
			if err != nil {
				t.Fatalf("convertValue(%q unsigned, %q): %v", tc.mysqlType, tc.raw, err)
			}
			tc.check(t, v)
		})
	}
}

// TestWriteRowUnsignedRoundTrip is the end-to-end arbiter for the #506 fix: an
// INT UNSIGNED at 4294967295 and a BIGINT UNSIGNED at 18446744073709551615
// must survive a full WriteRow → Parquet → read-back without becoming NULL.
//
// Column names are alphabetical so MySQL write order matches Parquet storage
// order (parquet.Group sorts alphabetically).
func TestWriteRowUnsignedRoundTrip(t *testing.T) {
	cols := []Column{
		{Name: "c_big_u", MySQLType: "bigint", Unsigned: true, ParquetType: mysqlToParquetNode("bigint", true)},
		{Name: "c_int_u", MySQLType: "int", Unsigned: true, ParquetType: mysqlToParquetNode("int", true)},
	}

	dir := t.TempDir()
	outPath := filepath.Join(dir, "unsigned.parquet")
	w, err := NewWriter(outPath, cols, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Values in MySQL (== alphabetical) order: c_big_u, c_int_u.
	values := []string{"18446744073709551615", "4294967295"}
	nulls := []bool{false, false}
	if err := w.WriteRow(values, nulls); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rf.Close()
	info, err := rf.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	reader := parquet.NewReader(pf)
	defer reader.Close()
	rows := make([]parquet.Row, 1)
	n, _ := reader.ReadRows(rows)
	if n != 1 {
		t.Fatalf("ReadRows = %d, want 1", n)
	}
	row := rows[0]
	if len(row) != 2 {
		t.Fatalf("row has %d values, want 2", len(row))
	}

	// Column order is alphabetical: [0]=c_big_u, [1]=c_int_u.
	if row[0].IsNull() {
		t.Fatal("c_big_u (BIGINT UNSIGNED): got NULL, want 18446744073709551615")
	}
	if got := uint64(row[0].Int64()); got != 18446744073709551615 {
		t.Errorf("c_big_u: got %d, want 18446744073709551615", got)
	}
	if row[1].IsNull() {
		t.Fatal("c_int_u (INT UNSIGNED): got NULL, want 4294967295")
	}
	if got := row[1].Int64(); got != 4294967295 {
		t.Errorf("c_int_u: got %d, want 4294967295", got)
	}
}

// TestWriteRowUnsignedDuckDBScan is the round-trip the TOOL actually performs:
// every production consumer (reconstruct.ReadBaselineRow, fulltable.go, shim
// _snapshot) reads baselines through DuckDB `parquet_scan`, not parquet-go's
// manual reader. This pins that the unsigned maxima survive the real read path
// AND format to the right SQL literal via recovery.FormatSQLValue — the chain
// that turns a scanned value into recovery output (issue #506 review).
//
// Type expectations differ by column because schema.go widens INT UNSIGNED into
// a SIGNED Int(64) but BIGINT UNSIGNED into Uint(64):
//   - BIGINT UNSIGNED → DuckDB UBIGINT → Go uint64 (the load-bearing assertion:
//     proves the real consumer never sees a negative for the unsigned maximum)
//   - INT UNSIGNED    → DuckDB BIGINT  → Go int64 (positive, exact value)
func TestWriteRowUnsignedDuckDBScan(t *testing.T) {
	cols := []Column{
		{Name: "c_big_u", MySQLType: "bigint", Unsigned: true, ParquetType: mysqlToParquetNode("bigint", true)},
		{Name: "c_int_u", MySQLType: "int", Unsigned: true, ParquetType: mysqlToParquetNode("int", true)},
	}

	dir := t.TempDir()
	outPath := filepath.Join(dir, "unsigned_duck.parquet")
	w, err := NewWriter(outPath, cols, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Values in MySQL (== alphabetical) order: c_big_u, c_int_u.
	if err := w.WriteRow([]string{"18446744073709551615", "4294967295"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	safePath := strings.ReplaceAll(outPath, "'", "''")
	var big, intv any
	row := db.QueryRowContext(context.Background(),
		"SELECT c_big_u, c_int_u FROM parquet_scan('"+safePath+"')")
	if err := row.Scan(&big, &intv); err != nil {
		t.Fatalf("scan parquet_scan row: %v", err)
	}

	// BIGINT UNSIGNED max must come back as a concrete uint64 — a negative int64
	// here would be the #506 corruption surfacing through the real consumer.
	bigU, ok := big.(uint64)
	if !ok {
		t.Fatalf("c_big_u scanned as %T, want uint64", big)
	}
	if bigU != 18446744073709551615 {
		t.Errorf("c_big_u = %d, want 18446744073709551615", bigU)
	}
	if got := recovery.FormatSQLValue(bigU); got != "18446744073709551615" {
		t.Errorf("FormatSQLValue(c_big_u) = %q, want %q", got, "18446744073709551615")
	}

	// INT UNSIGNED widened into a signed Int(64) → scans as int64 (positive).
	intI, ok := intv.(int64)
	if !ok {
		t.Fatalf("c_int_u scanned as %T, want int64", intv)
	}
	if intI != 4294967295 {
		t.Errorf("c_int_u = %d, want 4294967295", intI)
	}
	if got := recovery.FormatSQLValue(intI); got != "4294967295" {
		t.Errorf("FormatSQLValue(c_int_u) = %q, want %q", got, "4294967295")
	}
}

// TestParseSchemaWiringUnsignedDuckDBScan exercises the FULL production chain —
// ParseSchema (NOT a hand-built ParquetType) → NewWriter → WriteRow → DuckDB
// parquet_scan — for UNSIGNED maxima. This is the one test that fails if the
// schema.go call site regresses (e.g. ParseSchema calling
// mysqlToParquetNode(typeToken, false)): every other unsigned test hand-builds
// the ParquetType with explicit unsigned=true and would stay green under that
// bug (MUT-D). Here the widening must come from ParseSchema itself.
func TestParseSchemaWiringUnsignedDuckDBScan(t *testing.T) {
	const schema = "CREATE TABLE `t` (\n" +
		"  `c_big_u` bigint unsigned NOT NULL,\n" +
		"  `c_int_u` int unsigned NOT NULL,\n" +
		"  PRIMARY KEY (`c_int_u`)\n" +
		") ENGINE=InnoDB;\n"

	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "shop.t-schema.sql")
	if err := os.WriteFile(schemaPath, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := ParseSchema(schemaPath)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}

	outPath := filepath.Join(dir, "wiring.parquet")
	w, err := NewWriter(outPath, cols, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Values in MySQL (schema) order: c_big_u, c_int_u.
	if err := w.WriteRow([]string{"18446744073709551615", "4294967295"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	safePath := strings.ReplaceAll(outPath, "'", "''")
	var big, intv any
	row := db.QueryRowContext(context.Background(),
		"SELECT c_big_u, c_int_u FROM parquet_scan('"+safePath+"')")
	if err := row.Scan(&big, &intv); err != nil {
		t.Fatalf("scan parquet_scan row: %v", err)
	}

	// BIGINT UNSIGNED → DuckDB UBIGINT → Go uint64. Under MUT-D this column would
	// be a signed Int64 (DuckDB BIGINT → int64) and the max would read negative.
	bigU, ok := big.(uint64)
	if !ok {
		t.Fatalf("c_big_u scanned as %T, want uint64 (ParseSchema must widen BIGINT UNSIGNED to Uint64)", big)
	}
	if bigU != 18446744073709551615 {
		t.Errorf("c_big_u = %d, want 18446744073709551615", bigU)
	}

	// INT UNSIGNED → widened to signed Int64 → DuckDB BIGINT → Go int64. Under
	// MUT-D this would be an Int32 column and 4294967295 would fail to convert.
	intI, ok := intv.(int64)
	if !ok {
		t.Fatalf("c_int_u scanned as %T, want int64 (ParseSchema must widen INT UNSIGNED to Int64)", intv)
	}
	if intI != 4294967295 {
		t.Errorf("c_int_u = %d, want 4294967295", intI)
	}
}

// TestWriteRowFailsLoudOnUnconvertible pins the #503 item-3 silencer fix: a
// genuinely-unconvertible value must abort WriteRow with an error that names the
// column and value — NOT silently become NULL. This exercises WriteRow (where
// the old silent swallow lived), not convertValue alone.
func TestWriteRowFailsLoudOnUnconvertible(t *testing.T) {
	cols := []Column{
		{Name: "n", MySQLType: "int", ParquetType: MysqlToParquetNode("int")},
	}
	dir := t.TempDir()
	w, err := NewWriter(filepath.Join(dir, "loud.parquet"), cols, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	err = w.WriteRow([]string{"not-a-number"}, []bool{false})
	if err == nil {
		t.Fatal("WriteRow with unconvertible int value: got nil error, want loud failure (silent-NULL regression)")
	}
	// The error must locate the row for the operator.
	for _, want := range []string{"n", "not-a-number"} {
		if !contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
