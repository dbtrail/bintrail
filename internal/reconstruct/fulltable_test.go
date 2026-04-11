package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/bintrail/internal/baseline"
	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// writeTestBaseline creates a small Parquet baseline on disk with two
// columns (id INT, status VARCHAR) and the caller-provided rows. Returns
// the local path.
func writeTestBaseline(t *testing.T, rows [][]string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "baseline.parquet")
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 10,
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	return path
}

// pkColsIntID returns a minimal PK column descriptor for the test table
// (primary key is the single `id` INT column at ordinal position 1).
func pkColsIntID() []metadata.ColumnMeta {
	return []metadata.ColumnMeta{
		{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
	}
}

// pkStrForInt returns the canonical pk_values encoding for an integer id,
// matching what the binlog parser would produce at index time.
func pkStrForInt(id int) string {
	return strings.TrimSpace(
		// parser.BuildPKValues does fmt.Sprintf("%v", ...) so int → "42".
		// Construct the same way for test determinism.
		parser.BuildPKValues(pkColsIntID(), map[string]any{"id": id}),
	)
}

// TestMergeBaseline_passthroughOnly verifies the "no events" case: every
// baseline row flows through unchanged.
func TestMergeBaseline_passthroughOnly(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
		{"3", "shipped"},
	})
	outDir := t.TempDir()

	rep := &TableReport{Schema: "mydb", Table: "orders"}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           map[string]*query.ResultRow{},
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter: %v", err)
	}

	if rep.BaselineRows != 3 {
		t.Errorf("BaselineRows = %d, want 3", rep.BaselineRows)
	}
	if rep.UpdatesApplied != 0 || rep.InsertsEmitted != 0 || rep.DeletesSkipped != 0 {
		t.Errorf("unexpected event counters: %+v", rep)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	for _, want := range []string{"(1, 'new')", "(2, 'paid')", "(3, 'shipped')"} {
		if !strings.Contains(chunk, want) {
			t.Errorf("chunk missing %q:\n%s", want, chunk)
		}
	}
}

// TestMergeBaseline_updateSubstitution verifies that an UPDATE event on a
// baseline row produces the row_after values in the output and bumps the
// UpdatesApplied counter, not BaselineRows.
func TestMergeBaseline_updateSubstitution(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
	})
	outDir := t.TempDir()

	changes := map[string]*query.ResultRow{
		pkStrForInt(2): {
			EventType: parser.EventUpdate,
			PKValues:  pkStrForInt(2),
			RowBefore: map[string]any{"id": float64(2), "status": "paid"},
			RowAfter:  map[string]any{"id": float64(2), "status": "shipped"},
		},
	}

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter: %v", err)
	}

	if rep.BaselineRows != 1 {
		t.Errorf("BaselineRows = %d, want 1 (id=1 passes through)", rep.BaselineRows)
	}
	if rep.UpdatesApplied != 1 {
		t.Errorf("UpdatesApplied = %d, want 1", rep.UpdatesApplied)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	if !strings.Contains(chunk, "(1, 'new')") {
		t.Errorf("chunk missing passthrough row:\n%s", chunk)
	}
	if !strings.Contains(chunk, "(2, 'shipped')") {
		t.Errorf("chunk missing updated row:\n%s", chunk)
	}
	if strings.Contains(chunk, "(2, 'paid')") {
		t.Errorf("chunk still contains the pre-update baseline value:\n%s", chunk)
	}
}

// TestMergeBaseline_deleteSkipsRow verifies that a DELETE event removes the
// matching baseline row from the output (it is skipped, not substituted)
// and bumps DeletesSkipped.
func TestMergeBaseline_deleteSkipsRow(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
		{"3", "shipped"},
	})
	outDir := t.TempDir()

	changes := map[string]*query.ResultRow{
		pkStrForInt(2): {
			EventType: parser.EventDelete,
			PKValues:  pkStrForInt(2),
			RowBefore: map[string]any{"id": float64(2), "status": "paid"},
		},
	}

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter: %v", err)
	}

	if rep.DeletesSkipped != 1 {
		t.Errorf("DeletesSkipped = %d, want 1", rep.DeletesSkipped)
	}
	if rep.BaselineRows != 2 {
		t.Errorf("BaselineRows = %d, want 2", rep.BaselineRows)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	if strings.Contains(chunk, "(2,") {
		t.Errorf("chunk still contains deleted row (id=2):\n%s", chunk)
	}
	if !strings.Contains(chunk, "(1, 'new')") || !strings.Contains(chunk, "(3, 'shipped')") {
		t.Errorf("chunk missing surviving rows:\n%s", chunk)
	}
}

// TestMergeBaseline_insertAppendsAfterBaseline verifies that an INSERT event
// for a PK that is NOT in the baseline appends a new row after the baseline
// pass, and bumps InsertsEmitted.
func TestMergeBaseline_insertAppendsAfterBaseline(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
	})
	outDir := t.TempDir()

	changes := map[string]*query.ResultRow{
		pkStrForInt(42): {
			EventType: parser.EventInsert,
			PKValues:  pkStrForInt(42),
			RowAfter:  map[string]any{"id": float64(42), "status": "just-inserted"},
		},
		pkStrForInt(43): {
			EventType: parser.EventInsert,
			PKValues:  pkStrForInt(43),
			RowAfter:  map[string]any{"id": float64(43), "status": "also-inserted"},
		},
	}

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter: %v", err)
	}

	if rep.BaselineRows != 1 {
		t.Errorf("BaselineRows = %d, want 1", rep.BaselineRows)
	}
	if rep.InsertsEmitted != 2 {
		t.Errorf("InsertsEmitted = %d, want 2", rep.InsertsEmitted)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	for _, want := range []string{"(1, 'new')", "(42, 'just-inserted')", "(43, 'also-inserted')"} {
		if !strings.Contains(chunk, want) {
			t.Errorf("chunk missing %q:\n%s", want, chunk)
		}
	}

	// New inserts must be appended AFTER the baseline rows, not before.
	baselineIdx := strings.Index(chunk, "(1, 'new')")
	insert1Idx := strings.Index(chunk, "(42, ")
	insert2Idx := strings.Index(chunk, "(43, ")
	if baselineIdx < 0 || insert1Idx < 0 || insert2Idx < 0 {
		t.Fatalf("missing expected rows in chunk:\n%s", chunk)
	}
	if baselineIdx > insert1Idx || baselineIdx > insert2Idx {
		t.Errorf("baseline row should appear BEFORE inserted rows; got positions %d / %d / %d",
			baselineIdx, insert1Idx, insert2Idx)
	}
	// Deterministic ordering between new inserts (sorted by PK).
	if insert1Idx > insert2Idx {
		t.Errorf("new inserts should be sorted by PK; got id=42 at %d, id=43 at %d", insert1Idx, insert2Idx)
	}
}

// TestMergeBaseline_mixedAllEventTypes combines INSERT, UPDATE and DELETE
// in one run to verify the counters and output are correct together.
func TestMergeBaseline_mixedAllEventTypes(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "baseline-1"},
		{"2", "baseline-2"},
		{"3", "baseline-3"},
	})
	outDir := t.TempDir()

	changes := map[string]*query.ResultRow{
		pkStrForInt(2): {
			EventType: parser.EventUpdate,
			PKValues:  pkStrForInt(2),
			RowAfter:  map[string]any{"id": float64(2), "status": "updated-2"},
		},
		pkStrForInt(3): {
			EventType: parser.EventDelete,
			PKValues:  pkStrForInt(3),
		},
		pkStrForInt(10): {
			EventType: parser.EventInsert,
			PKValues:  pkStrForInt(10),
			RowAfter:  map[string]any{"id": float64(10), "status": "inserted-10"},
		},
	}

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter: %v", err)
	}

	if rep.BaselineRows != 1 {
		t.Errorf("BaselineRows = %d, want 1 (only id=1 is a passthrough)", rep.BaselineRows)
	}
	if rep.UpdatesApplied != 1 {
		t.Errorf("UpdatesApplied = %d, want 1", rep.UpdatesApplied)
	}
	if rep.DeletesSkipped != 1 {
		t.Errorf("DeletesSkipped = %d, want 1", rep.DeletesSkipped)
	}
	if rep.InsertsEmitted != 1 {
		t.Errorf("InsertsEmitted = %d, want 1", rep.InsertsEmitted)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	// Expected final rows: 1 (passthrough), 2 (updated), 10 (new insert).
	// Row 3 was deleted and must not appear.
	if !strings.Contains(chunk, "(1, 'baseline-1')") {
		t.Errorf("missing passthrough row:\n%s", chunk)
	}
	if !strings.Contains(chunk, "(2, 'updated-2')") {
		t.Errorf("missing updated row:\n%s", chunk)
	}
	if strings.Contains(chunk, "(3, ") {
		t.Errorf("deleted row still present:\n%s", chunk)
	}
	if !strings.Contains(chunk, "(10, 'inserted-10')") {
		t.Errorf("missing new insert:\n%s", chunk)
	}
}

// TestSplitSchemaTable covers the pure helper for parsing --tables entries.
func TestSplitSchemaTable(t *testing.T) {
	cases := []struct {
		in      string
		ok      bool
		schema  string
		table   string
	}{
		{"mydb.orders", true, "mydb", "orders"},
		{"schema_with_underscore.table_name", true, "schema_with_underscore", "table_name"},
		{"", false, "", ""},
		{"nodot", false, "", ""},
		{".notable", false, "", ""},
		{"noschema.", false, "", ""},
		{"too.many.dots", false, "", ""},
	}
	for _, c := range cases {
		s, tbl, ok := splitSchemaTable(c.in)
		if ok != c.ok {
			t.Errorf("splitSchemaTable(%q) ok = %v, want %v", c.in, ok, c.ok)
			continue
		}
		if ok && (s != c.schema || tbl != c.table) {
			t.Errorf("splitSchemaTable(%q) = (%q, %q), want (%q, %q)",
				c.in, s, tbl, c.schema, c.table)
		}
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// mustReadOnlyChunk reads the single .sql chunk file in dir (other than
// schema files) and returns its contents, failing the test if zero or
// multiple chunk files are present.
func mustReadOnlyChunk(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read output dir: %v", err)
	}
	var chunks []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".sql") && !strings.Contains(name, "-schema.sql") {
			chunks = append(chunks, name)
		}
	}
	if len(chunks) != 1 {
		t.Fatalf("expected exactly 1 chunk file in %s, got %d: %v", dir, len(chunks), chunks)
	}
	b, err := os.ReadFile(filepath.Join(dir, chunks[0]))
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	return string(b)
}
