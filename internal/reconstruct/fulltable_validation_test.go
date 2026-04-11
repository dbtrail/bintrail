package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/bintrail/internal/baseline"
	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/query"
)

// TestReconstructTables_validationErrors covers the pre-DB-connect guards in
// ReconstructTables. None of these reach the MySQL connection step, so the
// test runs without Docker/MySQL.
func TestReconstructTables_validationErrors(t *testing.T) {
	cases := []struct {
		name     string
		cfg      FullTableConfig
		wantSub  string
	}{
		{
			name: "missing IndexDSN",
			cfg: FullTableConfig{
				BaselineSrc: "/tmp/baselines",
				Tables:      []string{"db.t"},
				OutputDir:   "/tmp/out",
			},
			wantSub: "IndexDSN",
		},
		{
			name: "missing BaselineSrc",
			cfg: FullTableConfig{
				IndexDSN:  "root@tcp(localhost:3306)/idx",
				Tables:    []string{"db.t"},
				OutputDir: "/tmp/out",
			},
			wantSub: "BaselineSrc",
		},
		{
			name: "no tables",
			cfg: FullTableConfig{
				IndexDSN:    "root@tcp(localhost:3306)/idx",
				BaselineSrc: "/tmp/baselines",
				OutputDir:   "/tmp/out",
			},
			wantSub: "table",
		},
		{
			name: "missing OutputDir",
			cfg: FullTableConfig{
				IndexDSN:    "root@tcp(localhost:3306)/idx",
				BaselineSrc: "/tmp/baselines",
				Tables:      []string{"db.t"},
			},
			wantSub: "OutputDir",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReconstructTables(context.Background(), tc.cfg)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("expected error to mention %q, got: %v", tc.wantSub, err)
			}
		})
	}
}

// TestMergeBaselineIntoWriter_missingCreateTableSQL is a proxy test for the
// "baseline lacks bintrail.create_table_sql" hard-fail — the actual check
// lives in ReconstructTable (which requires a DB) so we cover the equivalent
// behavior by verifying the empty-CreateTableSQL path in mergeBaselineIntoWriter
// and the constant definition that both rely on.
//
// ReconstructTable's hard-fail path at fulltable.go checks bmeta.CreateTableSQL
// after baseline.ReadParquetMetadataAny and aborts with a re-baseline message.
// An integration-style test for that full path is in reconstruct_fulltable_integration_test.go.
func TestMergeBaselineIntoWriter_emptyCreateTableSQL(t *testing.T) {
	// Write a real baseline Parquet so DuckDB can scan it; pass an empty
	// CreateTableSQL and verify the schema file is still produced (it's
	// empty — not the hard-fail itself, but proves the writer accepts it
	// without corruption). The hard-fail is enforced one layer up in
	// ReconstructTable and requires a DB.
	dir := t.TempDir()
	baselinePath := writeTestBaseline(t, [][]string{{"1", "a"}})

	rep := &TableReport{Schema: "d", Table: "t"}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "", // empty
		Schema:            "d",
		Table:             "t",
		PKCols:            pkColsIntID(),
		Changes:           map[string]*query.ResultRow{},
		OutputDir:         dir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter with empty schema: %v", err)
	}
	schemaPath := filepath.Join(dir, "d.t-schema.sql")
	// With empty CreateTableSQL the file is created but empty. The
	// hard-fail lives upstream in ReconstructTable where the user-facing
	// error message instructs re-running bintrail baseline.
	info, statErr := readFileInfo(schemaPath)
	if statErr != nil {
		t.Fatalf("stat schema file: %v", statErr)
	}
	if info.Size() != 0 {
		t.Errorf("expected empty schema file, got size %d", info.Size())
	}
}

// TestMetaKeyCreateTableSQL_constant pins the key name so a rename doesn't
// silently break baselines written before the rename.
func TestMetaKeyCreateTableSQL_constant(t *testing.T) {
	if baseline.MetaKeyCreateTableSQL != "bintrail.create_table_sql" {
		t.Errorf("MetaKeyCreateTableSQL changed: got %q, want bintrail.create_table_sql", baseline.MetaKeyCreateTableSQL)
	}
}

// TestSplitSchemaTable is already covered in fulltable_test.go; do not
// duplicate here.

// TestZipMap_pairsAcrossColumns verifies the pure helper used to build a
// row map for parser.BuildPKValues lookups.
func TestZipMap_pairsAcrossColumns(t *testing.T) {
	m := zipMap([]string{"a", "b", "c"}, []any{int64(1), "two", nil})
	if m["a"] != int64(1) || m["b"] != "two" || m["c"] != nil {
		t.Errorf("unexpected zipMap result: %+v", m)
	}
	if len(m) != 3 {
		t.Errorf("expected 3 entries, got %d", len(m))
	}
}

// TestRowAfterOrdered_missingColumnBecomesNull verifies the schema-drift
// fallback: a column present in the baseline but absent from the event's
// row_after image is filled with nil and warned about.
func TestRowAfterOrdered_missingColumnBecomesNull(t *testing.T) {
	cols := []string{"id", "status", "added_later"}
	rowAfter := map[string]any{"id": float64(1), "status": "ok"} // added_later missing
	out := rowAfterOrdered(rowAfter, cols, "d", "t")
	if len(out) != 3 {
		t.Fatalf("expected 3 values, got %d", len(out))
	}
	if out[0] != float64(1) || out[1] != "ok" || out[2] != nil {
		t.Errorf("unexpected ordered result: %+v", out)
	}
}

// readFileInfo wraps os.Stat; kept tiny for test readability.
func readFileInfo(path string) (fileInfoLike, error) {
	return os.Stat(path)
}

type fileInfoLike interface {
	Size() int64
}

// ensure metadata types used in compile-time checks are referenced; prevents
// the `metadata` import from being unused in the validation test file.
var _ = metadata.ColumnMeta{}

// TestMissingPKColumnError_survivesErrorsJoin pins the contract that
// ReconstructTables' errors.Join aggregation preserves the typed
// *MissingPKColumnError for callers that want to recover the column name.
// A future refactor back to errs[0] or to a single string-concatenated
// error would fail errors.As / errors.Is through the join and this test
// would catch it.
func TestMissingPKColumnError_survivesErrorsJoin(t *testing.T) {
	missing := &MissingPKColumnError{Column: "created_at"}
	other := errors.New("other table: baseline read failed")
	joined := errors.Join(missing, other)

	// The sentinel still matches through the join.
	if !errors.Is(joined, ErrPKColumnMissing) {
		t.Errorf("errors.Is(joined, ErrPKColumnMissing) = false; want true")
	}

	// The typed error and its Column field are still recoverable.
	var gotMissing *MissingPKColumnError
	if !errors.As(joined, &gotMissing) {
		t.Fatalf("errors.As(joined, *MissingPKColumnError) = false; want true")
	}
	if gotMissing.Column != "created_at" {
		t.Errorf("recovered Column = %q, want %q", gotMissing.Column, "created_at")
	}

	// A third error that's neither the sentinel nor the typed error must
	// NOT trip either assertion by accident.
	unrelated := errors.New("unrelated")
	if errors.Is(unrelated, ErrPKColumnMissing) {
		t.Error("unrelated error incorrectly matched ErrPKColumnMissing")
	}
}
