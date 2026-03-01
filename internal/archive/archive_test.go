package archive

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/bintrail/bintrail/internal/baseline"
)

func TestBinlogEventColumns_count(t *testing.T) {
	if len(binlogEventColumns) != 14 {
		t.Errorf("expected 14 columns, got %d", len(binlogEventColumns))
	}
}

func TestBinlogEventColumns_names(t *testing.T) {
	wantNames := []string{
		"event_id", "binlog_file", "start_pos", "end_pos",
		"event_timestamp", "gtid", "schema_name", "table_name",
		"event_type", "pk_values", "changed_columns", "row_before", "row_after",
		"schema_version",
	}
	for i, want := range wantNames {
		if i >= len(binlogEventColumns) {
			t.Fatalf("missing column at index %d, want %q", i, want)
		}
		if binlogEventColumns[i].Name != want {
			t.Errorf("column[%d].Name = %q, want %q", i, binlogEventColumns[i].Name, want)
		}
	}
}

func TestBinlogEventColumns_parquetTypes(t *testing.T) {
	for _, col := range binlogEventColumns {
		if col.ParquetType == nil {
			t.Errorf("column %q has nil ParquetType", col.Name)
		}
	}
}

// TestWriteReadRoundTrip verifies that binlogEventColumns can be used to write
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

	w, err := baseline.NewWriter(outPath, binlogEventColumns, cfg)
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
		"mydb",                   // schema_name
		"orders",                 // table_name
		"1",                      // event_type (INSERT)
		"42",                     // pk_values
		`["col1","col2"]`,        // changed_columns
		`{"id":42,"old":"val"}`,  // row_before
		`{"id":42,"new":"val2"}`, // row_after
		"0",                      // schema_version
	}
	nulls1 := make([]bool, 14) // all false
	if err := w.WriteRow(row1, nulls1); err != nil {
		t.Fatalf("WriteRow 1: %v", err)
	}

	// Row 2: nullable fields (gtid, changed_columns, row_before, row_after) are null.
	row2 := []string{
		"2", "binlog.000001", "200", "300", "2026-02-19 10:00:01",
		"", "mydb", "orders", "3", "43",
		"", "", "", "1",
	}
	nulls2 := []bool{
		false, false, false, false, false,
		true, // gtid null
		false, false, false, false,
		true, true, true, // changed_columns, row_before, row_after null
		false,            // schema_version not null
	}
	if err := w.WriteRow(row2, nulls2); err != nil {
		t.Fatalf("WriteRow 2: %v", err)
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

	if pf.NumRows() != 2 {
		t.Errorf("NumRows = %d, want 2", pf.NumRows())
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
