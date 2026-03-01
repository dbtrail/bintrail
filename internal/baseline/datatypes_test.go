package baseline

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// TestAllMySQLDataTypes_ConversionTable verifies convertValue for every
// documented MySQL column type. For each type we check that the value is
// converted without truncation, corruption, or silent data loss.
func TestAllMySQLDataTypes_ConversionTable(t *testing.T) {
	knownDT := time.Date(2026, 2, 20, 14, 30, 0, 0, time.UTC)
	knownDTFrac := time.Date(2026, 2, 20, 14, 30, 0, 123456000, time.UTC) // 123456 µs
	knownDate := time.Date(2026, 2, 20, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		mysql string
		raw   string
		check func(*testing.T, parquet.Value)
	}{
		// ── INT32 family ─────────────────────────────────────────────────────
		{"tinyint", "-128", func(t *testing.T, v parquet.Value) {
			if v.Int32() != -128 {
				t.Errorf("got %d, want -128", v.Int32())
			}
		}},
		{"tinyint", "127", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 127 {
				t.Errorf("got %d, want 127", v.Int32())
			}
		}},
		{"smallint", "-32768", func(t *testing.T, v parquet.Value) {
			if v.Int32() != -32768 {
				t.Errorf("got %d, want -32768", v.Int32())
			}
		}},
		{"smallint", "32767", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 32767 {
				t.Errorf("got %d, want 32767", v.Int32())
			}
		}},
		{"mediumint", "-8388608", func(t *testing.T, v parquet.Value) {
			if v.Int32() != -8388608 {
				t.Errorf("got %d, want -8388608", v.Int32())
			}
		}},
		{"mediumint", "8388607", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 8388607 {
				t.Errorf("got %d, want 8388607", v.Int32())
			}
		}},
		{"int", "-2147483648", func(t *testing.T, v parquet.Value) {
			if v.Int32() != -2147483648 {
				t.Errorf("got %d, want -2147483648", v.Int32())
			}
		}},
		{"int", "2147483647", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 2147483647 {
				t.Errorf("got %d, want 2147483647", v.Int32())
			}
		}},
		{"integer", "42", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 42 {
				t.Errorf("got %d, want 42", v.Int32())
			}
		}},
		{"year", "2026", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 2026 {
				t.Errorf("got %d, want 2026", v.Int32())
			}
		}},

		// ── INT64 family ─────────────────────────────────────────────────────
		{"bigint", "9876543210", func(t *testing.T, v parquet.Value) {
			if v.Int64() != 9876543210 {
				t.Errorf("got %d, want 9876543210", v.Int64())
			}
		}},
		{"bigint", "-9223372036854775808", func(t *testing.T, v parquet.Value) {
			if v.Int64() != -9223372036854775808 {
				t.Errorf("got %d, want min int64", v.Int64())
			}
		}},

		// ── Floating point ────────────────────────────────────────────────────
		{"float", "3.140625", func(t *testing.T, v parquet.Value) {
			// 3.140625 is exactly representable as float32 (= 2 + 1 + 1/8 + 1/64).
			if v.Float() != float32(3.140625) {
				t.Errorf("got %v, want 3.140625", v.Float())
			}
		}},
		{"float", "-1.5", func(t *testing.T, v parquet.Value) {
			if v.Float() != float32(-1.5) {
				t.Errorf("got %v, want -1.5", v.Float())
			}
		}},
		{"double", "2.718281828459045", func(t *testing.T, v parquet.Value) {
			if v.Double() != 2.718281828459045 {
				t.Errorf("got %v, want 2.718281828459045", v.Double())
			}
		}},
		{"real", "1.5", func(t *testing.T, v parquet.Value) {
			if v.Double() != 1.5 {
				t.Errorf("got %v, want 1.5", v.Double())
			}
		}},

		// ── Exact numeric (preserved as string to avoid precision loss) ────────
		{"decimal", "99999.99", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "99999.99" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "99999.99")
			}
		}},
		{"decimal", "-0.000001", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "-0.000001" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "-0.000001")
			}
		}},
		{"numeric", "12345.6789", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "12345.6789" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "12345.6789")
			}
		}},

		// ── Date/time types ───────────────────────────────────────────────────
		{"datetime", "2026-02-20 14:30:00", func(t *testing.T, v parquet.Value) {
			if v.Int64() != knownDT.UnixMicro() {
				t.Errorf("got %d, want %d (%s)", v.Int64(), knownDT.UnixMicro(), knownDT)
			}
		}},
		{"datetime", "2026-02-20 14:30:00.123456", func(t *testing.T, v parquet.Value) {
			// 123456 µs = 123456000 ns sub-second precision must survive the round-trip.
			if v.Int64() != knownDTFrac.UnixMicro() {
				t.Errorf("got %d, want %d (%s)", v.Int64(), knownDTFrac.UnixMicro(), knownDTFrac)
			}
		}},
		{"timestamp", "2026-02-20 14:30:00", func(t *testing.T, v parquet.Value) {
			if v.Int64() != knownDT.UnixMicro() {
				t.Errorf("got %d, want %d", v.Int64(), knownDT.UnixMicro())
			}
		}},
		{"date", "2026-02-20", func(t *testing.T, v parquet.Value) {
			wantDays := int32(knownDate.Unix() / 86400)
			if v.Int32() != wantDays {
				t.Errorf("got %d days, want %d", v.Int32(), wantDays)
			}
		}},
		{"time", "14:30:00", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "14:30:00" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "14:30:00")
			}
		}},

		// ── String types (all → BYTE_ARRAY) ──────────────────────────────────
		{"char", "HELLO", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "HELLO" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "HELLO")
			}
		}},
		{"varchar", "hello world", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "hello world" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "hello world")
			}
		}},
		{"tinytext", "small text", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "small text" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "small text")
			}
		}},
		{"text", "longer text value", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "longer text value" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "longer text value")
			}
		}},
		{"mediumtext", "medium text", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "medium text" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "medium text")
			}
		}},
		{"longtext", "long text value", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "long text value" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "long text value")
			}
		}},
		{"enum", "active", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "active" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "active")
			}
		}},
		{"set", "a,b,c", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "a,b,c" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "a,b,c")
			}
		}},
		{"json", `{"key":"value","num":42}`, func(t *testing.T, v parquet.Value) {
			want := `{"key":"value","num":42}`
			if string(v.ByteArray()) != want {
				t.Errorf("got %q, want %q", string(v.ByteArray()), want)
			}
		}},

		// ── Binary types (all → BYTE_ARRAY) ──────────────────────────────────
		{"binary", "BINDATA", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "BINDATA" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "BINDATA")
			}
		}},
		{"varbinary", "binary data", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "binary data" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "binary data")
			}
		}},
		{"tinyblob", "tblob", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "tblob" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "tblob")
			}
		}},
		{"blob", "blobdata", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "blobdata" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "blobdata")
			}
		}},
		{"mediumblob", "mblob", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "mblob" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "mblob")
			}
		}},
		{"longblob", "lblob", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "lblob" {
				t.Errorf("got %q, want %q", string(v.ByteArray()), "lblob")
			}
		}},
		{"bit", "\x01\x02", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "\x01\x02" {
				t.Errorf("got %v, want [0x01 0x02]", v.ByteArray())
			}
		}},
	}

	for _, tc := range cases {
		name := tc.mysql + "/" + tc.raw
		if len(tc.raw) > 30 {
			name = tc.mysql
		}
		t.Run(name, func(t *testing.T) {
			col := Column{Name: "col", MySQLType: tc.mysql}
			v, err := convertValue(col, tc.raw)
			if err != nil {
				t.Fatalf("convertValue(%q, %q): %v", tc.mysql, tc.raw, err)
			}
			tc.check(t, v)
		})
	}
}

// TestAllMySQLDataTypes_RoundTrip writes a single row containing every MySQL
// type to a Parquet file, reads it back, and asserts that each value is
// preserved exactly — no truncation, corruption, type coercion, or data loss.
//
// Column names are chosen in alphabetical order so that the MySQL write order
// matches the Parquet storage order (parquet.Group sorts columns
// alphabetically), which lets us compare rows[0][i] directly against cases[i].
func TestAllMySQLDataTypes_RoundTrip(t *testing.T) {
	knownDT := time.Date(2026, 2, 20, 14, 30, 0, 0, time.UTC)
	knownDate := time.Date(2026, 2, 20, 0, 0, 0, 0, time.UTC)

	wantBA := func(want string) func(*testing.T, parquet.Value) {
		return func(t *testing.T, v parquet.Value) {
			t.Helper()
			if string(v.ByteArray()) != want {
				t.Errorf("got %q, want %q", string(v.ByteArray()), want)
			}
		}
	}

	type rtCase struct {
		name  string // column name (alphabetically sorted — must match Parquet order)
		mysql string
		value string
		check func(*testing.T, parquet.Value)
	}

	// These 32 entries are sorted alphabetically by Name.
	cases := []rtCase{
		{"c_bigint", "bigint", "9876543210", func(t *testing.T, v parquet.Value) {
			if v.Int64() != 9876543210 {
				t.Errorf("bigint: got %d, want 9876543210", v.Int64())
			}
		}},
		{"c_binary", "binary", "BINDATA", wantBA("BINDATA")},
		{"c_bit", "bit", "\x01", wantBA("\x01")},
		{"c_blob", "blob", "blobcontent", wantBA("blobcontent")},
		{"c_char", "char", "FIXED", wantBA("FIXED")},
		{"c_date", "date", "2026-02-20", func(t *testing.T, v parquet.Value) {
			want := int32(knownDate.Unix() / 86400)
			if v.Int32() != want {
				t.Errorf("date: got %d days, want %d", v.Int32(), want)
			}
		}},
		{"c_datetime", "datetime", "2026-02-20 14:30:00", func(t *testing.T, v parquet.Value) {
			if v.Int64() != knownDT.UnixMicro() {
				t.Errorf("datetime: got %d, want %d", v.Int64(), knownDT.UnixMicro())
			}
		}},
		{"c_decimal", "decimal", "99999.99", wantBA("99999.99")},
		{"c_double", "double", "2.718281828459045", func(t *testing.T, v parquet.Value) {
			if v.Double() != 2.718281828459045 {
				t.Errorf("double: got %v, want 2.718281828459045", v.Double())
			}
		}},
		{"c_enum", "enum", "active", wantBA("active")},
		{"c_float", "float", "3.140625", func(t *testing.T, v parquet.Value) {
			// 3.140625 is exactly representable as float32.
			if v.Float() != float32(3.140625) {
				t.Errorf("float: got %v, want 3.140625", v.Float())
			}
		}},
		{"c_int", "int", "2147483647", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 2147483647 {
				t.Errorf("int: got %d, want 2147483647", v.Int32())
			}
		}},
		{"c_integer", "integer", "1000", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 1000 {
				t.Errorf("integer: got %d, want 1000", v.Int32())
			}
		}},
		{"c_json", "json", `{"key":"value"}`, wantBA(`{"key":"value"}`)},
		{"c_longblob", "longblob", "longblobcontent", wantBA("longblobcontent")},
		{"c_longtext", "longtext", "long text value", wantBA("long text value")},
		{"c_mediumblob", "mediumblob", "medblobcontent", wantBA("medblobcontent")},
		{"c_mediumint", "mediumint", "8388607", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 8388607 {
				t.Errorf("mediumint: got %d, want 8388607", v.Int32())
			}
		}},
		{"c_mediumtext", "mediumtext", "medium text value", wantBA("medium text value")},
		{"c_numeric", "numeric", "12345.6789", wantBA("12345.6789")},
		{"c_real", "real", "1.5", func(t *testing.T, v parquet.Value) {
			if v.Double() != 1.5 {
				t.Errorf("real: got %v, want 1.5", v.Double())
			}
		}},
		{"c_set", "set", "a,b,c", wantBA("a,b,c")},
		{"c_smallint", "smallint", "32767", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 32767 {
				t.Errorf("smallint: got %d, want 32767", v.Int32())
			}
		}},
		{"c_text", "text", "longer text value", wantBA("longer text value")},
		{"c_time", "time", "14:30:00", wantBA("14:30:00")},
		{"c_timestamp", "timestamp", "2026-02-20 14:30:00", func(t *testing.T, v parquet.Value) {
			if v.Int64() != knownDT.UnixMicro() {
				t.Errorf("timestamp: got %d, want %d", v.Int64(), knownDT.UnixMicro())
			}
		}},
		{"c_tinyblob", "tinyblob", "tinyblob", wantBA("tinyblob")},
		{"c_tinyint", "tinyint", "127", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 127 {
				t.Errorf("tinyint: got %d, want 127", v.Int32())
			}
		}},
		{"c_tinytext", "tinytext", "tiny text", wantBA("tiny text")},
		{"c_varbinary", "varbinary", "varbin data", wantBA("varbin data")},
		{"c_varchar", "varchar", "hello world", wantBA("hello world")},
		{"c_year", "year", "2026", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 2026 {
				t.Errorf("year: got %d, want 2026", v.Int32())
			}
		}},
	}

	// Build columns in MySQL order (== alphabetical == Parquet order).
	cols := make([]Column, len(cases))
	for i, tc := range cases {
		cols[i] = Column{
			Name:        tc.name,
			MySQLType:   tc.mysql,
			ParquetType: MysqlToParquetNode(tc.mysql),
		}
	}

	// ── Write ─────────────────────────────────────────────────────────────────
	dir := t.TempDir()
	outPath := filepath.Join(dir, "all_types.parquet")
	w, err := NewWriter(outPath, cols, WriterConfig{
		Compression:  "none",
		RowGroupSize: 1000,
		Metadata:     map[string]string{"bintrail.source_table": "all_types_demo"},
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	values := make([]string, len(cases))
	nulls := make([]bool, len(cases))
	for i, tc := range cases {
		values[i] = tc.value
	}
	if err := w.WriteRow(values, nulls); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// ── Read back ─────────────────────────────────────────────────────────────
	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open for read: %v", err)
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
	if pf.NumRows() != 1 {
		t.Fatalf("NumRows = %d, want 1", pf.NumRows())
	}

	reader := parquet.NewReader(pf)
	defer reader.Close()
	rows := make([]parquet.Row, 1)
	n, _ := reader.ReadRows(rows)
	if n != 1 {
		t.Fatalf("ReadRows returned %d rows, want 1", n)
	}
	row := rows[0]
	if len(row) != len(cases) {
		t.Fatalf("row has %d values, want %d (one per column)", len(row), len(cases))
	}

	// ── Verify every column ───────────────────────────────────────────────────
	for i, tc := range cases {
		t.Run(tc.mysql+"/"+tc.name, func(t *testing.T) {
			v := row[i]
			if v.IsNull() {
				t.Fatalf("column %q (%s): got NULL, want non-null", tc.name, tc.mysql)
			}
			tc.check(t, v)
		})
	}
}

// TestAllMySQLDataTypes_NullValues writes a row of all NULLs and reads it back,
// verifying that every MySQL type correctly stores and retrieves NULL.
func TestAllMySQLDataTypes_NullValues(t *testing.T) {
	// Column names in alphabetical order (Parquet storage order).
	types := []struct{ name, mysql string }{
		{"c_bigint", "bigint"},
		{"c_blob", "blob"},
		{"c_date", "date"},
		{"c_datetime", "datetime"},
		{"c_decimal", "decimal"},
		{"c_double", "double"},
		{"c_float", "float"},
		{"c_int", "int"},
		{"c_json", "json"},
		{"c_text", "text"},
		{"c_timestamp", "timestamp"},
		{"c_varchar", "varchar"},
	}

	cols := make([]Column, len(types))
	for i, tc := range types {
		cols[i] = Column{
			Name:        tc.name,
			MySQLType:   tc.mysql,
			ParquetType: MysqlToParquetNode(tc.mysql),
		}
	}

	dir := t.TempDir()
	outPath := filepath.Join(dir, "null_test.parquet")
	w, err := NewWriter(outPath, cols, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	values := make([]string, len(types))
	nulls := make([]bool, len(types))
	for i := range nulls {
		nulls[i] = true
	}
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
		t.Fatalf("expected 1 row, got %d", n)
	}
	row := rows[0]

	for i, tc := range types {
		t.Run(tc.mysql, func(t *testing.T) {
			if i >= len(row) {
				t.Fatalf("row too short (len %d, index %d)", len(row), i)
			}
			if !row[i].IsNull() {
				t.Errorf("column %q (%s): expected NULL, got %v", tc.name, tc.mysql, row[i])
			}
		})
	}
}
