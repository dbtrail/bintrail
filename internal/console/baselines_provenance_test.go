package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeProvenanceTable writes one real baseline Parquet whose footer carries the
// producer and the snapshot instant a run of that kind would leave.
func writeProvenanceTable(t *testing.T, root, snapshot, schema, table, producer, stampedAt string) {
	t.Helper()
	const createSQL = "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	dir := filepath.Join(root, snapshot, schema)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(dir, table+".parquet"), cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL:    createSQL,
			baseline.MetaKeySnapshotProducer:  producer,
			baseline.MetaKeySnapshotTimestamp: stampedAt,
		},
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestBaselineFilesAPI_reportsHowEachTableWasMade covers the per-table half of
// #1545 through the real endpoint.
//
// Per TABLE and not per snapshot, because carry-forward makes one snapshot a
// mix: this fixture is one snapshot holding a folded table and a carried one,
// which is the shape a snapshot-level answer cannot describe at all.
func TestBaselineFilesAPI_reportsHowEachTableWasMade(t *testing.T) {
	root := t.TempDir()
	const snap = "2026-06-10T12-00-00Z"
	const snapAt = "2026-06-10T12:00:00Z"
	const olderAt = "2026-06-03T12:00:00Z"

	writeProvenanceTable(t, root, snap, "shop", "orders", baseline.ProducerReconstruct, snapAt)
	// Carried: the previous file, hard linked, so its footer keeps the OLDER
	// instant. Written directly here rather than linked, because what is under
	// test is the READING rule; the link itself is pinned in reconstruct.
	writeProvenanceTable(t, root, snap, "shop", "cold", baseline.ProducerReconstruct, olderAt)
	writeProvenanceTable(t, root, snap, "shop", "fresh", baseline.ProducerDump, snapAt)
	if err := os.WriteFile(filepath.Join(root, snap, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	srv := newBaselineServer(t, root, true)
	at, err := time.Parse(time.RFC3339, snapAt)
	if err != nil {
		t.Fatal(err)
	}
	rec, body := doServersReq(t, srv, "GET",
		"/api/baselines/files?at="+at.Format(time.RFC3339), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselineFilesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{
		"cold":   baseline.ProducedByCarriedForward,
		"fresh":  baseline.ProducedByDump,
		"orders": baseline.ProducedByFold,
	}
	if len(got.Tables) != len(want) {
		t.Fatalf("tables = %+v, want %d", got.Tables, len(want))
	}
	for _, row := range got.Tables {
		if row.ProducedBy != want[row.Table] {
			t.Errorf("%s.%s made by %q, want %q", row.Schema, row.Table, row.ProducedBy, want[row.Table])
		}
	}
	for _, row := range got.Tables {
		if row.Table == "cold" && row.From == "" {
			t.Error("the carried table does not name the snapshot its bytes came from, " +
				"which is what answers how far back the last real read of the source is")
		}
		if row.Table == "fresh" && row.From != "" {
			t.Errorf("a table read from the source reports from = %q; it is derived from nothing", row.From)
		}
	}
}
