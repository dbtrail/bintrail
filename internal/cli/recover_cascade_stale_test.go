package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// writeChildBaselineParquet writes a minimal real Parquet baseline snapshot
// for <schema>.child (columns id, pid — id is the PK, pid is the FK the
// cascade engine filters on) under <dir>/<snapshotDir>/<schema>/child.parquet.
// Mirrors internal/reconstruct's writeTestBaseline fixture pattern.
func writeChildBaselineParquet(t *testing.T, dir, snapshotDir, schema string, rows [][]string) string {
	t.Helper()
	path := filepath.Join(dir, snapshotDir, schema, "child.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "pid", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
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

// childResolver builds an in-memory schema resolver (no DB) for <schema>.child
// with PK "id" and FK column "pid" — the shape cascadeBaselineProvider expects.
func childResolver(schema string) *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: schema,
		Table:  "child",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "pid", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{schema + ".child": tm})
}

// TestCascadeBaselineProvider_staleFallbackPropagates pins #618 at the CLI
// provider boundary — the sibling of the identical console test. When
// reconstruct.FindBaseline falls back to an older snapshot because the child
// table is absent from the newest one, the returned
// cascade.BaselineLookup.StaleMessage must carry that signal instead of
// discarding it (the pre-#618 behavior: `path, snap, _, err := FindBaseline(...)`).
// A lookup where the chosen snapshot IS the newest eligible one must leave
// StaleMessage empty.
func TestCascadeBaselineProvider_staleFallbackPropagates(t *testing.T) {
	dir := t.TempDir()
	schema := "shop"
	writeChildBaselineParquet(t, dir, "2026-01-01T00-00-00Z", schema, [][]string{{"10", "1"}})
	// Newer, complete snapshot that does NOT have shop.child — this is what
	// makes the 2026-01-01 pick a stale fallback.
	if err := os.MkdirAll(filepath.Join(dir, "2026-02-01T00-00-00Z"), 0o755); err != nil {
		t.Fatal(err)
	}

	provider := &cascadeBaselineProvider{source: dir, resolver: childResolver(schema)}

	// `at` is after both snapshots, so the newer (childless) one is the
	// "newest eligible" snapshot and the older one is a stale fallback.
	at := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	lookup, ok, err := provider.BaselineChildren(context.Background(), schema, "child", "pid", "1", at, 100)
	if err != nil {
		t.Fatalf("BaselineChildren: %v", err)
	}
	if !ok {
		t.Fatalf("BaselineChildren ok = false, want true (the older snapshot covers the table)")
	}
	if len(lookup.Rows) != 1 || lookup.Rows[0].PKValues != "10" {
		t.Fatalf("Rows = %+v, want one row with PKValues=10", lookup.Rows)
	}
	if lookup.StaleMessage == "" {
		t.Fatal("StaleMessage is empty, want the #466 stale-fallback signal to be carried through")
	}
	if !strings.Contains(lookup.StaleMessage, schema+".child") || !strings.Contains(lookup.StaleMessage, "absent from the newest snapshot") {
		t.Fatalf("StaleMessage = %q, want it to name %s.child and the fallback reason (matches reconstruct.staleFallback's format)", lookup.StaleMessage, schema)
	}

	// When the chosen snapshot IS the newest eligible one, StaleMessage must
	// be empty — not a spurious caveat every time Phase-2 runs.
	at2 := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	lookup2, ok2, err := provider.BaselineChildren(context.Background(), schema, "child", "pid", "1", at2, 100)
	if err != nil {
		t.Fatalf("BaselineChildren (non-stale): %v", err)
	}
	if !ok2 {
		t.Fatalf("BaselineChildren (non-stale) ok = false, want true")
	}
	if lookup2.StaleMessage != "" {
		t.Fatalf("StaleMessage = %q, want empty when the chosen snapshot is the newest eligible one", lookup2.StaleMessage)
	}
}
