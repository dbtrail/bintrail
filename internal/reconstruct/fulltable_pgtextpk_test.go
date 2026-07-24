package reconstruct

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// writePGTextBaseline writes a baseline whose columns are stored as text, the
// way pgbaseline's COPY-text output lands them — DuckDB scans them back as Go
// strings, matching the pgoutput-text pk_values the delta side stored. This is
// the fixture behind the PGTextPK string-identity PK match.
func writePGTextBaseline(t *testing.T, rows [][]string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "baseline.parquet")
	cols := []baseline.Column{
		{Name: "id", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return path
}

// pgTextPKCols is a PostgreSQL-style PK descriptor: DataType is EMPTY (a PG
// column carries no MySQL DATA_TYPE token). That empty token is exactly what
// makes the MySQL canonicalizer error and what PGTextPK bypasses.
func pgTextPKCols() []metadata.ColumnMeta {
	return []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: ""}}
}

// TestSnapshotFullTableImages_pgTextPK_match proves the #1022 core: a PostgreSQL
// full-table merge (baseline + deltas) succeeds with an empty-DATA_TYPE PK and
// matches the delta events to baseline rows by string identity — no live PG,
// pure Parquet + change map. This is the first full-table PG merge; the MySQL
// canonicalizer is bypassed via PGTextPK.
func TestSnapshotFullTableImages_pgTextPK_match(t *testing.T) {
	baselinePath := writePGTextBaseline(t, [][]string{{"1", "new"}, {"2", "paid"}, {"3", "shipped"}})
	pkCols := pgTextPKCols()
	changes := map[string]*query.ResultRow{
		event.BuildPKValues(pkCols, map[string]any{"id": "2"}): {
			EventType: event.EventUpdate, SchemaName: "app", TableName: "orders",
			PKValues: "2", RowAfter: map[string]any{"id": "2", "status": "DONE"},
		},
		event.BuildPKValues(pkCols, map[string]any{"id": "4"}): {
			EventType: event.EventInsert, SchemaName: "app", TableName: "orders",
			PKValues: "4", RowAfter: map[string]any{"id": "4", "status": "NEW"},
		},
	}

	got := map[string]string{}
	err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
		BaselinePath: baselinePath, Schema: "app", Table: "orders",
		PKCols: pkCols, Changes: changes, PGTextPK: true,
	}, func(row map[string]any) error {
		got[fmt.Sprint(row["id"])] = fmt.Sprint(row["status"])
		return nil
	})
	if err != nil {
		t.Fatalf("PGTextPK merge must succeed for an empty-DATA_TYPE PK: %v", err)
	}
	want := map[string]string{"1": "new", "2": "DONE", "3": "shipped", "4": "NEW"}
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d (%v)", len(got), len(want), got)
	}
	for id, st := range want {
		if got[id] != st {
			t.Errorf("id=%s status=%q, want %q (full: %v)", id, got[id], st, got)
		}
	}
}

// TestSnapshotFullTableImages_pgTextPK_falseStillCanonicalizes proves the flag —
// not some unrelated change — is what bypasses the canonicalizer, and that the
// MySQL path stays intact: with PGTextPK=false the same empty-DATA_TYPE PK hits
// the canonicalizer and errors loudly.
func TestSnapshotFullTableImages_pgTextPK_falseStillCanonicalizes(t *testing.T) {
	baselinePath := writePGTextBaseline(t, [][]string{{"1", "new"}})
	err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
		BaselinePath: baselinePath, Schema: "app", Table: "orders",
		PKCols: pgTextPKCols(), Changes: map[string]*query.ResultRow{}, PGTextPK: false,
	}, func(map[string]any) error { return nil })
	if err == nil {
		t.Fatal("expected a canonicalize error for an empty-DATA_TYPE PK when PGTextPK=false")
	}
}
