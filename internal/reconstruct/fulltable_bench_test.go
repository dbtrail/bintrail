package reconstruct

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/dbtrail/bintrail/internal/baseline"
	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// BenchmarkMergeBaselineIntoWriter measures end-to-end wall-clock time for
// the full-table reconstruct merge loop against a synthetic Parquet
// baseline of N rows with M in-memory change events. This is the
// preparatory measurement harness for #207 (row-group level PK-range
// pruning): the issue's own guidance says to "benchmark against a 10GB+
// baseline with ~1% change rate to quantify the speedup before deciding
// it's worth the complexity." This benchmark is the "before" picture —
// any future pruning optimization must beat these numbers or it's not
// worth the silent-wrong-output risk of touching the merge loop.
//
// The benchmark does NOT need MySQL — mergeBaselineIntoWriter takes a
// pre-built mergeInput with an in-process change map, so the only real
// I/O is (1) reading the Parquet baseline via DuckDB parquet_scan and
// (2) writing the mydumper SQL chunk files to a tempdir. That isolates
// the measurement to the parts #207 would actually optimize: the
// canonicalize + lookup + write-or-passthrough inner loop.
//
// Sub-benchmarks parameterise on (baseline rows × change rate):
//
//   - 100k × 1% (1k changes)   — smallest tier, always runs
//   - 1M × 1% (10k changes)    — medium tier, always runs
//   - 10M × 1% (100k changes)  — large tier, skipped under -short
//     because the synthetic 10M-row parquet takes ~30s to build
//
// Usage:
//
//	go test -bench=BenchmarkMergeBaselineIntoWriter \
//	    -benchmem -run=^$ -count=3 -timeout 30m \
//	    ./internal/reconstruct/
//
// Decision framework (per the plan file for #207):
//
//   - 1M/10k subtest < 5s wall & < 50MB alloc: close #207 WONTFIX.
//   - 5-30s: open a focused follow-up to implement the Q1/Q2 split.
//   - > 30s or > 500MB: prioritize the split; land the two-path
//     agreement test first to rule out silent wrong output.
func BenchmarkMergeBaselineIntoWriter(b *testing.B) {
	cases := []struct {
		name      string
		rows      int
		changes   int
		skipShort bool
	}{
		// 100k rows × 1% change rate — fastest tier, always runs.
		{name: "rows=100k/changes=1k", rows: 100_000, changes: 1_000},
		// 1M rows × 1% change rate — medium tier, always runs. This is
		// the one the plan file pins the WONTFIX / proceed thresholds
		// against; read this line carefully when interpreting numbers.
		{name: "rows=1M/changes=10k", rows: 1_000_000, changes: 10_000},
		// 10M rows × 1% change rate — large tier. Skipped under -short
		// so `go test -short ./...` stays fast; unskipping takes ~30s
		// for the fixture build alone.
		{name: "rows=10M/changes=100k", rows: 10_000_000, changes: 100_000, skipShort: true},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			if tc.skipShort && testing.Short() {
				b.Skipf("skipping %s under -short (fixture build is ~30s)", tc.name)
			}

			// Build the fixture ONCE outside the timed loop. The
			// Parquet baseline is a large artifact (100MB+ for the 10M
			// tier) and b.N would otherwise rebuild it every iteration,
			// burying the merge-loop signal under file-generation cost.
			baselinePath := writeBenchBaseline(b, tc.rows)
			changes := buildBenchChanges(tc.rows, tc.changes)
			createSQL := "CREATE TABLE `t` (\n" +
				"  `id` INT NOT NULL,\n" +
				"  `payload` VARCHAR(64) NOT NULL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB;\n"

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Re-clone the change map per iteration: the merge
				// loop DRAINS it (entries are deleted as they are
				// matched against baseline rows) to decide which
				// changes are still-missing inserts to append at the
				// end. Running twice on the same map would report
				// zero UpdatesApplied on iteration 2.
				b.StopTimer()
				changesCopy := make(map[string]*query.ResultRow, len(changes))
				for k, v := range changes {
					changesCopy[k] = v
				}
				outDir := b.TempDir()
				b.StartTimer()

				rep := &TableReport{Schema: "bench", Table: "t"}
				if err := mergeBaselineIntoWriter(context.Background(), mergeInput{
					LocalBaselinePath: baselinePath,
					CreateTableSQL:    createSQL,
					Schema:            "bench",
					Table:             "t",
					PKCols:            pkColsIntID(),
					Changes:           changesCopy,
					OutputDir:         outDir,
					ChunkSize:         256 << 20,
				}, rep); err != nil {
					b.Fatalf("mergeBaselineIntoWriter: %v", err)
				}

				// Report derived metrics on the last iteration so the
				// output line contains the ratios readers care about.
				// b.N iterations all share the same fixture and should
				// produce identical counters; asserting on the first
				// iteration catches a regression in the merge loop
				// that a pure timing benchmark would silently miss.
				if i == 0 {
					wantBaseline := int64(tc.rows - tc.changes)
					if rep.BaselineRows != wantBaseline {
						b.Fatalf("BaselineRows = %d, want %d", rep.BaselineRows, wantBaseline)
					}
					if rep.UpdatesApplied != int64(tc.changes) {
						b.Fatalf("UpdatesApplied = %d, want %d", rep.UpdatesApplied, tc.changes)
					}
					// Rows/sec across the full merged row set (baseline
					// + applied updates). Higher is better.
					rowsPerSec := float64(tc.rows) / b.Elapsed().Seconds() * float64(b.N)
					b.ReportMetric(rowsPerSec, "rows/sec")
					// Hit ratio (matched changes / total changes) —
					// should be exactly 1.0 for this fixture. Reported
					// so a future divergence surfaces as a non-1.0
					// number instead of a silent test failure.
					b.ReportMetric(float64(rep.UpdatesApplied)/float64(tc.changes), "hit-ratio")
				}
			}
		})
	}
}

// writeBenchBaseline creates a Parquet baseline with n rows of schema
// (id INT, payload VARCHAR). id ranges from 1 to n (sequential) so the
// change-map keys in buildBenchChanges can be computed deterministically
// without a lookup table. Row group size is fixed at 500_000 — the same
// ballpark the issue description uses for its 50GB / 500-row-group
// reference workload, so that relative numbers across tiers stay
// comparable if the default changes.
func writeBenchBaseline(tb testing.TB, n int) string {
	tb.Helper()
	dir := tb.TempDir()
	path := filepath.Join(dir, "baseline.parquet")
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "payload", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "zstd",
		RowGroupSize: 500_000,
	})
	if err != nil {
		tb.Fatalf("baseline.NewWriter: %v", err)
	}
	nulls := []bool{false, false}
	row := make([]string, 2)
	for i := 1; i <= n; i++ {
		row[0] = strconv.Itoa(i)
		// Fixed-length payload so total file size scales predictably
		// with n. Length ~16 bytes keeps the 10M tier under ~200MB.
		row[1] = fmt.Sprintf("payload-%08d", i)
		if err := w.WriteRow(row, nulls); err != nil {
			tb.Fatalf("WriteRow at i=%d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatalf("writer close: %v", err)
	}
	return path
}

// buildBenchChanges builds a change map with m UPDATE events evenly
// spaced across [1, n]. Even spacing matters for #207 because a
// row-group pruner's speedup depends on how many row groups contain at
// least one changed PK: clustered changes leave most groups untouched,
// while evenly-spaced changes touch every group. Evenly-spaced is the
// adversarial case — if the pruner still wins here, it wins everywhere.
func buildBenchChanges(n, m int) map[string]*query.ResultRow {
	changes := make(map[string]*query.ResultRow, m)
	if m == 0 {
		return changes
	}
	// Stride ≈ n/m: the i-th change targets row id = 1 + i*stride,
	// clamped to n-1 so we never emit an event for a PK the baseline
	// doesn't contain (that would turn into an append instead of an
	// update, skewing the counters).
	stride := n / m
	if stride < 1 {
		stride = 1
	}
	for i := 0; i < m; i++ {
		id := 1 + i*stride
		if id >= n {
			id = n - 1
		}
		pk := pkStrForInt(id)
		changes[pk] = &query.ResultRow{
			EventType: parser.EventUpdate,
			PKValues:  pk,
			RowBefore: map[string]any{"id": float64(id), "payload": fmt.Sprintf("payload-%08d", id)},
			RowAfter:  map[string]any{"id": float64(id), "payload": fmt.Sprintf("updated-%08d", id)},
		}
	}
	return changes
}
