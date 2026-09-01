package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// writeProvenanceSnapshot writes one real baseline Parquet under
// <root>/<stamp>/demo/orders.parquet with the footer a run of `producer` leaves,
// plus the _MANIFEST carryForward validates against.
func writeProvenanceSnapshot(t *testing.T, root, stamp, producer string) string {
	t.Helper()
	snapDir := filepath.Join(root, stamp)
	path := filepath.Join(snapDir, "demo", "orders.parquet")
	at, ok := snapshotdir.ParseTime(stamp)
	if !ok {
		t.Fatalf("fixture stamp %q is not a snapshot directory name", stamp)
	}
	// Multi-line: the schema parser is a line scanner over mydumper's own
	// -schema.sql layout, and a one-line CREATE TABLE yields no columns.
	cols, err := baseline.ParseSchemaText("CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n")
	if err != nil {
		t.Fatalf("parse fixture schema: %v", err)
	}
	w, werr := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 10,
		// Every provenance key, not just the two the verdict turns on. Three of
		// them reached ProvenanceOf only as hand-set struct fields, so a reader
		// that stopped looking one of them up off a REAL file changed nothing
		// any test could see.
		Metadata: map[string]string{
			baseline.MetaKeySnapshotTimestamp: at.Format(time.RFC3339),
			baseline.MetaKeySnapshotProducer:  producer,
			baseline.MetaKeyDerivedFrom:       at.Add(-7 * 24 * time.Hour).Format(time.RFC3339),
			baseline.MetaKeyDerivedFromPath:   "/b/ancestor/demo/orders.parquet",
			baseline.MetaKeyMydumperFormat:    "csv",
		},
	})
	if werr != nil {
		t.Fatalf("baseline writer: %v", werr)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatalf("write row: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := baselineintegrity.WriteManifest(snapDir); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path
}

// TestCarryForward_readsBackAsCarriedNotAsItsAncestor is the wiring behind
// baseline.ProvenanceOf.
//
// carryForward HARD LINKS the previous file, so the carried file's footer is
// the ancestor's, byte for byte, and cannot be stamped: rewriting it would edit
// the older snapshot through the same inode. The derivation therefore rests on
// a property of the real filesystem operation — that the footer's instant stays
// behind while the directory moves on — which a test over hand-built metadata
// asserts about itself and proves nothing about.
//
// Without this, a change that made carryForward COPY-and-restamp, or that moved
// the snapshot instant out of the footer, would leave every unit test green
// while the page reported a carried table as a fold.
func TestCarryForward_readsBackAsCarriedNotAsItsAncestor(t *testing.T) {
	root := t.TempDir()
	const oldStamp, newStamp = "2026-06-03T12-00-00Z", "2026-06-10T12-00-00Z"
	src := writeProvenanceSnapshot(t, root, oldStamp, baseline.ProducerReconstruct)

	newDir := filepath.Join(root, newStamp)
	if err := carryForward(context.Background(), src, newDir, "demo", "orders"); err != nil {
		t.Fatalf("carryForward: %v", err)
	}
	dst := filepath.Join(newDir, "demo", "orders.parquet")

	md, err := baseline.ReadParquetMetadata(dst)
	if err != nil {
		t.Fatalf("read the carried file's footer: %v", err)
	}
	// The reader, off a REAL file, not off a struct a test filled in. Three of
	// these keys are handled in two places (a closure over pf.Lookup locally, a
	// switch for S3) and reached ProvenanceOf only as hand-set fields, so a
	// reader that quietly stopped looking one of them up broke nothing visible.
	if md.DerivedFromPath == "" {
		t.Error("derived_from_path did not survive the footer read; a fold's ancestor file is " +
			"silently dropped from every real snapshot")
	}
	if md.DerivedFrom.IsZero() {
		t.Error("derived_from_snapshot did not survive the footer read")
	}
	if md.MydumperFormat == "" {
		t.Error("mydumper_format did not survive the footer read; it is the one signal that " +
			"dates every pre-#1545 MySQL dump, so losing it regrades them all to unknown")
	}
	newAt, _ := snapshotdir.ParseTime(newStamp)
	got := baseline.ProvenanceOf(newAt, md)
	if got.ProducedBy != baseline.ProducedByCarriedForward {
		t.Fatalf("a carried file reads as %q; it was neither dumped nor folded into this snapshot, "+
			"and calling it a fold credits it with a replay that never ran (footer stamp %v, snapshot %v)",
			got.ProducedBy, md.SnapshotTimestamp, newAt)
	}
	oldAt, _ := snapshotdir.ParseTime(oldStamp)
	if !got.From.Equal(oldAt) {
		t.Errorf("From = %v, want %v — the ancestor whose bytes these are is the actionable half", got.From, oldAt)
	}

	// The other half of the same fixture: the ORIGINAL file, read under its own
	// directory, is a fold. Without this the test above would pass on a
	// derivation that called everything carried.
	if got := baseline.ProvenanceOf(oldAt, md); got.ProducedBy != baseline.ProducedByFold {
		t.Errorf("the source file under its OWN snapshot reads as %q, want %q", got.ProducedBy, baseline.ProducedByFold)
	}
}

// And the fold's own writer is stamped, read back off a file it really wrote.
func TestParquetWriter_stampsItsProducer(t *testing.T) {
	root := t.TempDir()
	const stamp = "2026-06-10T12-00-00Z"
	path := writeProvenanceSnapshot(t, root, stamp, SnapshotProducerReconstruct)
	md, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	if md.Producer != baseline.ProducerReconstruct {
		t.Errorf("producer = %q, want %q", md.Producer, baseline.ProducerReconstruct)
	}
}

// TestDumpWritersStampTheirProducer guards the two stamps that make a NEW dump
// self-identifying.
//
// A source guard, deliberately, and the limitation is the point: driving
// baseline.Run needs mydumper output and pgbaseline needs a live PostgreSQL, so
// neither stamp has an executing test. Worse, deleting either is SILENT in
// production too — a real mydumper file still carries mydumper_format and grades
// `dump` through the legacy fallback, so the loss shows up only years later on a
// PostgreSQL snapshot whose LSN happens to be absent.
//
// Text is what is available; the alternative was no guard at all.
func TestDumpWritersStampTheirProducer(t *testing.T) {
	for path, want := range map[string]string{
		"../baseline/baseline.go":     "MetaKeySnapshotProducer: ProducerDump",
		"../pgbaseline/pgbaseline.go": "baseline.MetaKeySnapshotProducer: baseline.ProducerDump",
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if !strings.Contains(string(raw), want) {
			t.Errorf("%s no longer stamps %q into the footers it writes; new snapshots from it "+
				"stop saying they came from the source, and nothing else reports the loss", path, want)
		}
	}
}
