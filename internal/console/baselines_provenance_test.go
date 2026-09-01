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

// TestBaselineFilesAPI_unreadableFooterIsNotAVerdict keeps "we could not find
// out" distinct from "the file records nothing" (#1545).
//
// Collapsing them hands the operator a verdict nobody checked, which is the
// ee#115 class this endpoint's comment cites by name. An empty produced_by
// renders as a dash; `unknown` renders as a claim about the file.
func TestBaselineFilesAPI_unreadableFooterIsNotAVerdict(t *testing.T) {
	root := t.TempDir()
	const snap = "2026-06-10T12-00-00Z"
	writeProvenanceTable(t, root, snap, "shop", "orders", baseline.ProducerDump, "2026-06-10T12:00:00Z")
	// Not Parquet at all: the reader will refuse it.
	if err := os.WriteFile(filepath.Join(root, snap, "shop", "broken.parquet"),
		[]byte("this is not a parquet file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, snap, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	srv := newBaselineServer(t, root, true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/files?at=2026-06-10T12:00:00Z", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s — one unreadable file must not take the listing down", rec.Code, body)
	}
	var got baselineFilesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	var seen int
	for _, row := range got.Tables {
		switch row.Table {
		case "broken":
			seen++
			if row.ProducedBy != "" {
				t.Errorf("an unreadable footer reported %q; nothing was found out about that file, "+
					"and a verdict here is a claim nobody checked", row.ProducedBy)
			}
		case "orders":
			seen++
			// The readable one beside it still answers, so "empty" above is not
			// the whole endpoint having given up.
			if row.ProducedBy != baseline.ProducedByDump {
				t.Errorf("the readable table reported %q, want %q", row.ProducedBy, baseline.ProducedByDump)
			}
		}
	}
	if seen != 2 {
		t.Fatalf("tables = %+v, want both the readable and the unreadable one", got.Tables)
	}
}
