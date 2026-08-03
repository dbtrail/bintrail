package console

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// TestAppendRenderGUCsWarning pins the console-side predicate (#921): only a
// PostgreSQL baseline (LSN anchor) with an absent or mismatched rendering-GUC
// stamp adds a "render_gucs_mismatch:" entry; MySQL baselines (LSN 0 — which
// also covers a failed metadata read) and matching stamps are no-ops, and
// existing warnings are preserved.
func TestAppendRenderGUCsWarning(t *testing.T) {
	if got := appendRenderGUCsWarning(nil, baseline.DumpMetadata{}); got != nil {
		t.Errorf("MySQL baseline (LSN 0) must not add a warning, got %v", got)
	}
	if got := appendRenderGUCsWarning(nil, baseline.DumpMetadata{LSN: 42, RenderGUCs: baseline.RenderGUCsPinned}); got != nil {
		t.Errorf("matching stamp must not add a warning, got %v", got)
	}
	base := []string{"gap warning"}
	got := appendRenderGUCsWarning(base, baseline.DumpMetadata{LSN: 42, RenderGUCs: "TimeZone=America/New_York"})
	if len(got) != 2 || got[0] != "gap warning" || !strings.HasPrefix(got[1], "render_gucs_mismatch: ") {
		t.Fatalf("mismatched stamp not appended correctly: %v", got)
	}
	if !strings.Contains(got[1], "TimeZone=America/New_York") || !strings.Contains(got[1], "bintrail-pg baseline") {
		t.Errorf("warning lacks the stamp or the remediation: %q", got[1])
	}
	// Pre-pin PG baseline: LSN anchor present, stamp absent.
	if got := appendRenderGUCsWarning(nil, baseline.DumpMetadata{LSN: 42}); len(got) != 1 || !strings.HasPrefix(got[0], "render_gucs_mismatch: ") {
		t.Errorf("pre-pin PG baseline must warn, got %v", got)
	}
}

// TestRenderGUCsMismatchReachesWarningsDTO pins the read+predicate+append
// contract on a REAL mismatched stamp (#921). It does NOT drive
// handleReconstruct — the handler wiring (bmeta reaching the response
// Warnings) is pinned by TestIntegrationReconstructRenderGUCsWarning in
// reconstruct_integration_test.go. Original framing, kept for the helper half: #466 stale_baseline
// test: a baseline Parquet whose metadata carries an LSN anchor and an old
// rendering-GUC stamp is read with the same ReadParquetMetadataAny call
// handleReconstruct uses, and appendRenderGUCsWarning lands the entry in the
// reconstructResponse the Time-travel UI renders.
func TestRenderGUCsMismatchReachesWarningsDTO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "2026-01-01T00-00-00Z", "shop", "orders.parquet")
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 10,
		Metadata: map[string]string{
			baseline.MetaKeyLSN:        "42",
			baseline.MetaKeyRenderGUCs: "TimeZone=America/New_York;DateStyle=SQL;extra_float_digits=0;bytea_output=escape;IntervalStyle=sql_standard",
		},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	// Same read handleReconstruct performs after findBaseline.
	bmeta, err := baseline.ReadParquetMetadataAny(context.Background(), path)
	if err != nil {
		t.Fatalf("ReadParquetMetadataAny: %v", err)
	}

	at := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	resp := reconstructResponse{
		Schema: "shop", Table: "orders", PK: "1",
		At:       at.Format(consoleTSFormat),
		Warnings: appendRenderGUCsWarning(nil, bmeta),
	}
	if len(resp.Warnings) != 1 || !strings.HasPrefix(resp.Warnings[0], "render_gucs_mismatch: ") {
		t.Fatalf("Warnings DTO missing the render_gucs_mismatch entry: %v", resp.Warnings)
	}

	// And it survives JSON encoding (what the UI receives).
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "render_gucs_mismatch") {
		t.Fatalf("encoded response lacks render_gucs_mismatch: %s", b)
	}
}
