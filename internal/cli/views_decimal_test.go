package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

// TestResolveBaselineViews_carriesDecimalTypes covers the seam, not the parts.
//
// The generator casts whatever it is handed and the footer reader reads
// whatever it is pointed at; both are tested on their own. What neither proves
// is that the views COMMAND connects them, and an Input that reaches Generate
// with no Decimals produces exactly the uncast `SELECT *` this whole change
// exists to stop shipping.
func TestResolveBaselineViews_carriesDecimalTypes(t *testing.T) {
	root := t.TempDir()
	snapDir := filepath.Join(root, "2026-04-30T03-00-00Z")
	writeSnapshotTable(t, snapDir, "shop", "orders",
		"CREATE TABLE `orders` (\n"+
			"  `id` int NOT NULL,\n"+
			"  `total` decimal(10,2) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			");\n",
		[][]string{{"1", "10.50"}})
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}

	prev := vBaselineDir
	t.Cleanup(func() { vBaselineDir = prev })
	vBaselineDir = root

	var in views.Input
	if err := resolveBaselineViews(context.Background(), &in); err != nil {
		t.Fatalf("resolveBaselineViews: %v", err)
	}
	if len(in.Baselines) != 1 {
		t.Fatalf("got %d baseline tables, want 1", len(in.Baselines))
	}
	bt := in.Baselines[0]
	if !bt.SchemaKnown {
		t.Fatal("SchemaKnown is false: the command did not read the table's column types, " +
			"so its state view would ship uncast")
	}
	if len(bt.Decimals) != 1 || bt.Decimals[0].Name != "total" ||
		bt.Decimals[0].Precision != 10 || bt.Decimals[0].Scale != 2 {
		t.Fatalf("got Decimals %+v, want one {total 10 2}", bt.Decimals)
	}

	// And the whole point: the emitted view casts it.
	sqlText := views.Generate(in)
	if want := `CAST("total" AS DECIMAL(10,2))`; !strings.Contains(sqlText, want) {
		t.Errorf("generated file does not contain %s\n--- generated ---\n%s", want, sqlText)
	}
}

// writeSnapshotTable lays down one table of a baseline snapshot exactly where
// reconstruct.ListBaselines looks for it, through the real writer.
func writeSnapshotTable(t *testing.T, snapDir, schema, table, createSQL string, rows [][]string) {
	t.Helper()
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(snapDir, schema), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	w, err := baseline.NewWriter(filepath.Join(snapDir, schema, table+".parquet"), cols,
		baseline.WriterConfig{
			Compression:  "none",
			RowGroupSize: 100,
			Metadata:     map[string]string{baseline.MetaKeyCreateTableSQL: createSQL},
		})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, make([]bool, len(r))); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
