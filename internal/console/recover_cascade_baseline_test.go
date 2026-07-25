package console

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// writeChildBaselineParquet writes a minimal real Parquet baseline snapshot
// for <schema>.child (columns id, pid — id is the PK, pid is the FK the
// cascade engine filters on) under <dir>/<snapshotDir>/<schema>/child.parquet.
// meta, when non-nil, lands in the Parquet footer's key/value metadata — that
// is where the #797 binlog coordinates live.
func writeChildBaselineParquet(t *testing.T, dir, snapshotDir, schema string, rows [][]string, meta map[string]string) string {
	t.Helper()
	path := filepath.Join(dir, snapshotDir, schema, "child.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "pid", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: meta})
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
// with PK "id" and FK column "pid" — the shape the cascade baseline provider
// expects.
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

// TestCascadeProviderFor_setsSincePos pins #1101: the console's cascade Phase-2
// provider must anchor the candidate-victim fetch on the baseline's exact
// recorded binlog position when the footer has one (#797), not on the coarse
// SnapshotTime DATETIME alone. Before the two providers were unified, the
// console copy never read the footer and always left SincePos nil, so a child
// whose statement executed just before SnapshotTime but got logged just after
// it could be silently missed on the console path while the CLI path caught it.
func TestCascadeProviderFor_setsSincePos(t *testing.T) {
	dir := t.TempDir()
	schema := "shop"
	writeChildBaselineParquet(t, dir, "2026-01-01T00-00-00Z", schema, [][]string{{"10", "1"}}, map[string]string{
		baseline.MetaKeyBinlogFile: "binlog.000042",
		baseline.MetaKeyBinlogPos:  "12345",
	})

	b := &bundle{baselineSrc: dir, resolver: childResolver(schema), baselineConfigured: true}
	provider := cascadeProviderFor(b)
	if provider == nil {
		t.Fatal("cascadeProviderFor returned nil; a typed-nil provider would report BaselineActive with no baseline behind it")
	}

	at := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	lookup, ok, err := provider.BaselineChildren(context.Background(), schema, "child", "pid", "1", at, 100)
	if err != nil {
		t.Fatalf("BaselineChildren: %v", err)
	}
	if !ok || len(lookup.Rows) != 1 || lookup.Rows[0].PKValues != "10" {
		t.Fatalf("lookup = (ok=%v, rows=%+v), want one row with PKValues=10", ok, lookup.Rows)
	}
	if lookup.SincePos == nil {
		t.Fatal("SincePos is nil on the console path; want the baseline's recorded binlog position (#1101)")
	}
	if lookup.SincePos.File != "binlog.000042" || lookup.SincePos.Pos != 12345 {
		t.Fatalf("SincePos = %+v, want {binlog.000042 12345}", *lookup.SincePos)
	}
}

// TestCascadeProviderFor_usesBundleFallback pins #1102: the console's cascade
// Phase-2 provider goes through bundle.findBaseline, so it composes with the
// #766 local→S3 fallback the rest of the console already gets. Before the
// unification it called reconstruct.FindBaseline against b.baselineSrc
// directly, so a table only present in the durable fallback copy silently
// degraded to "no baseline" (Phase-1 only, flagged incomplete).
//
// The fallback source here is a local directory rather than a real s3:// prefix
// — bundle.findBaseline treats both identically (it just re-runs the lookup
// against baselineFallbackSrc on ErrNoBaseline), and this keeps the test free of
// AWS.
func TestCascadeProviderFor_usesBundleFallback(t *testing.T) {
	schema := "shop"
	primary := t.TempDir() // stands in for a pruned/stale local baseline dir
	fallback := t.TempDir()
	writeChildBaselineParquet(t, fallback, "2026-01-01T00-00-00Z", schema, [][]string{{"10", "1"}}, nil)

	b := &bundle{
		baselineSrc:         primary,
		baselineFallbackSrc: fallback,
		resolver:            childResolver(schema),
		baselineConfigured:  true,
	}

	at := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	lookup, ok, err := cascadeProviderFor(b).BaselineChildren(context.Background(), schema, "child", "pid", "1", at, 100)
	if err != nil {
		t.Fatalf("BaselineChildren: %v", err)
	}
	if !ok {
		t.Fatal("ok = false: the cascade provider did not fall back to the durable copy (#766/#1102)")
	}
	if len(lookup.Rows) != 1 || lookup.Rows[0].PKValues != "10" {
		t.Fatalf("Rows = %+v, want the single row the fallback source holds", lookup.Rows)
	}

	// Without a fallback configured, the same primary-only bundle still degrades
	// to Phase-1 (ok=false, no error) — the fallback must not mask a genuine
	// "no baseline covers this table".
	noFallback := &bundle{baselineSrc: primary, resolver: childResolver(schema), baselineConfigured: true}
	_, ok2, err := cascadeProviderFor(noFallback).BaselineChildren(context.Background(), schema, "child", "pid", "1", at, 100)
	if err != nil {
		t.Fatalf("BaselineChildren (no fallback): %v, want nil (Phase-1 only)", err)
	}
	if ok2 {
		t.Fatal("ok = true with no baseline anywhere, want false")
	}
}
