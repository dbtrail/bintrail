package archive

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver — exercises the real read path
	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func TestBinlogEventColumns_count(t *testing.T) {
	if len(BinlogEventColumns) != 15 {
		t.Errorf("expected 15 columns, got %d", len(BinlogEventColumns))
	}
}

func TestBinlogEventColumns_names(t *testing.T) {
	wantNames := []string{
		"event_id", "binlog_file", "start_pos", "end_pos",
		"event_timestamp", "gtid", "connection_id", "schema_name", "table_name",
		"event_type", "pk_values", "changed_columns", "row_before", "row_after",
		"schema_version",
	}
	for i, want := range wantNames {
		if i >= len(BinlogEventColumns) {
			t.Fatalf("missing column at index %d, want %q", i, want)
		}
		if BinlogEventColumns[i].Name != want {
			t.Errorf("column[%d].Name = %q, want %q", i, BinlogEventColumns[i].Name, want)
		}
	}
}

func TestBinlogEventColumns_parquetTypes(t *testing.T) {
	for _, col := range BinlogEventColumns {
		if col.ParquetType == nil {
			t.Errorf("column %q has nil ParquetType", col.Name)
		}
	}
}

// TestWriteReadRoundTrip verifies that BinlogEventColumns can be used to write
// and read a Parquet file without a DB — it tests column definitions, null
// handling, and metadata embedding end-to-end.
func TestWriteReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "p_2026021900.parquet")

	cfg := baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			"bintrail.archive.partition": "p_2026021900",
			"bintrail.archive.version":   "1.0.0",
		},
	}

	w, err := baseline.NewWriter(outPath, BinlogEventColumns, cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Row 1: all non-null fields populated.
	row1 := []string{
		"1",                      // event_id
		"binlog.000001",          // binlog_file
		"100",                    // start_pos
		"200",                    // end_pos
		"2026-02-19 10:00:00",    // event_timestamp
		"abc123:1",               // gtid
		"12345",                  // connection_id
		"mydb",                   // schema_name
		"orders",                 // table_name
		"1",                      // event_type (INSERT)
		"42",                     // pk_values
		`["col1","col2"]`,        // changed_columns
		`{"id":42,"old":"val"}`,  // row_before
		`{"id":42,"new":"val2"}`, // row_after
		"0",                      // schema_version
	}
	nulls1 := make([]bool, 15) // all false
	if err := w.WriteRow(row1, nulls1); err != nil {
		t.Fatalf("WriteRow 1: %v", err)
	}

	// Row 2: nullable fields (gtid, connection_id, changed_columns, row_before, row_after) are null.
	row2 := []string{
		"2", "binlog.000001", "200", "300", "2026-02-19 10:00:01",
		"", "", "mydb", "orders", "3", "43",
		"", "", "", "1",
	}
	nulls2 := []bool{
		false, false, false, false, false,
		true, // gtid null
		true, // connection_id null
		false, false, false, false,
		true, true, true, // changed_columns, row_before, row_after null
		false,            // schema_version not null
	}
	if err := w.WriteRow(row2, nulls2); err != nil {
		t.Fatalf("WriteRow 2: %v", err)
	}

	// Row 3: binlog_file is null (the dbtrail/bintrail#318 case — customer
	// indexes that predate the NOT NULL constraint or rows from external
	// pipelines). Confirms the Parquet writer accepts NULL at column index 1.
	row3 := []string{
		"3", "", "300", "400", "2026-02-19 10:00:02",
		"def456:1", "67890", "mydb", "orders", "1", "44",
		`["col1"]`, `{"id":44}`, `{"id":44,"v":1}`, "1",
	}
	nulls3 := []bool{
		false,
		true, // binlog_file null
		false, false, false, false, false, false, false, false, false,
		false, false, false, false,
	}
	if err := w.WriteRow(row3, nulls3); err != nil {
		t.Fatalf("WriteRow 3: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read back and verify row count + metadata.
	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open parquet file: %v", err)
	}
	defer rf.Close()
	info, _ := rf.Stat()
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	if pf.NumRows() != 3 {
		t.Errorf("NumRows = %d, want 3", pf.NumRows())
	}

	// Read back the rows and verify binlog_file NULL semantics: row 1 (the
	// first non-NULL row written) carries "binlog.000001"; row 3 is the
	// dbtrail/bintrail#318 case — must be a real Parquet NULL, not an
	// empty string smuggled into the column. Without nulls[1]=!binlogFile.Valid
	// in archive.go, the writer would emit "" and IsNull() would be false.
	//
	// parquet-go orders row values alphabetically by column name, not by
	// schema declaration order — binlog_file sorts to index 0.
	binlogFileIdx := parquetColumnIndex(t, pf, "binlog_file")
	reader := parquet.NewReader(pf)
	defer reader.Close()
	parquetRows := make([]parquet.Row, 3)
	if n, err := reader.ReadRows(parquetRows); err != nil || n != 3 {
		t.Fatalf("ReadRows returned (%d, %v), want (3, nil)", n, err)
	}
	if parquetRows[0][binlogFileIdx].IsNull() {
		t.Errorf("row 0 binlog_file: got NULL, want \"binlog.000001\"")
	} else if got := parquetRows[0][binlogFileIdx].String(); got != "binlog.000001" {
		t.Errorf("row 0 binlog_file: got %q, want \"binlog.000001\"", got)
	}
	if !parquetRows[2][binlogFileIdx].IsNull() {
		t.Errorf("row 2 binlog_file: got %q, want NULL", parquetRows[2][binlogFileIdx].String())
	}

	// Verify key-value metadata was embedded.
	got, ok := pf.Lookup("bintrail.archive.partition")
	if !ok {
		t.Error("expected bintrail.archive.partition metadata key")
	} else if got != "p_2026021900" {
		t.Errorf("archive.partition = %q, want p_2026021900", got)
	}

	if _, ok := pf.Lookup("bintrail.archive.version"); !ok {
		t.Error("expected bintrail.archive.version metadata key")
	}
}

// TestWriteRowConnectionIDUnsignedDuckDBScan pins the fail-loud regression fix:
// connection_id is INT UNSIGNED (a CONNECTION_ID() reaches 4294967295, past
// int32). Before BinlogEventColumns widened it via MysqlToParquetNode2("int",
// true), a value over 2147483647 hit the new fail-loud WriteRow against a signed
// Int(32) column and ABORTED the whole partition archive. This drives the
// production BinlogEventColumns through WriteRow with the unsigned maximum and a
// mid-range value, then reads back via DuckDB parquet_scan (the real consumer):
// both must round-trip, NOT abort and NOT become NULL.
func TestWriteRowConnectionIDUnsignedDuckDBScan(t *testing.T) {
	const connIDIdx = 6 // MySQL order: event_id, binlog_file, start_pos, end_pos, event_timestamp, gtid, connection_id

	for _, connID := range []string{"4294967295", "3000000000"} {
		t.Run(connID, func(t *testing.T) {
			dir := t.TempDir()
			outPath := filepath.Join(dir, "conn.parquet")
			w, err := baseline.NewWriter(outPath, BinlogEventColumns,
				baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}

			// Only connection_id and the truly-NOT-NULL columns carry a value;
			// everything else is NULL so the row stays minimal.
			values := make([]string, 15)
			nulls := make([]bool, 15)
			for i := range nulls {
				nulls[i] = true
			}
			values[0], nulls[0] = "1", false // event_id
			values[4], nulls[4] = "2026-02-19 10:00:00", false
			values[connIDIdx], nulls[connIDIdx] = connID, false

			if err := w.WriteRow(values, nulls); err != nil {
				t.Fatalf("WriteRow(connection_id=%s): got error, want round-trip (fail-loud regression): %v", connID, err)
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
			var conn any
			if err := db.QueryRowContext(context.Background(),
				"SELECT connection_id FROM parquet_scan('"+safePath+"')").Scan(&conn); err != nil {
				t.Fatalf("scan connection_id: %v", err)
			}
			// INT UNSIGNED widened to signed Int64 → DuckDB BIGINT → Go int64.
			got, ok := conn.(int64)
			if !ok {
				t.Fatalf("connection_id scanned as %T, want int64", conn)
			}
			want := int64(0)
			switch connID {
			case "4294967295":
				want = 4294967295
			case "3000000000":
				want = 3000000000
			}
			if got != want {
				t.Errorf("connection_id = %d, want %d", got, want)
			}
		})
	}
}

// parquetColumnIndex looks up the position of a column in the file's leaf
// schema. parquet-go's NewReader returns rows whose values are ordered by
// the schema's leaf walk (alphabetical for our flat schemas), not by the
// order columns were passed to NewWriter — so callers that want to assert
// on a specific column must look up its index dynamically.
func parquetColumnIndex(t *testing.T, pf *parquet.File, name string) int {
	t.Helper()
	for i, col := range pf.Schema().Columns() {
		if len(col) == 1 && col[0] == name {
			return i
		}
	}
	t.Fatalf("column %q not found in parquet schema", name)
	return -1
}
