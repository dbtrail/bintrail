package console

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// TestViewsAPI_castsDecimalColumns is the console half of the #1486 seam.
//
// The CLI and the console resolve the same layout through two different
// functions, and a fix that reached only one of them would leave the console's
// downloadable file, and its SQL panel session, serving the uncast views the
// CLI stopped serving. Both surfaces are pinned so neither can drift back
// alone.
//
// Note the OTHER views tests write zero-byte fixture files, which means they
// exercise the degraded path (no footer to read, no casts) and prove it does
// not break the endpoint. This one writes a real baseline Parquet so the
// working path is covered too.
func TestViewsAPI_castsDecimalColumns(t *testing.T) {
	dir := t.TempDir()
	createSQL := "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `total` decimal(10,2) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"
	writeRealBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders", createSQL)

	srv := newViewsServer(t, dir, false)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if want := `CAST("total" AS DECIMAL(10,2))`; !strings.Contains(sql, want) {
		t.Errorf("the console's views.sql does not cast the money column (%s missing); "+
			"its state view would fail sum() the way #1486 reported:\n%s", want, sql)
	}
}

// writeRealBaselineFixture writes an actual baseline Parquet, with the CREATE
// TABLE embedded in the footer, at the path ListBaselines expects.
func writeRealBaselineFixture(t *testing.T, root, snapshot, schema, table, createSQL string) {
	t.Helper()
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
		Metadata:     map[string]string{baseline.MetaKeyCreateTableSQL: createSQL},
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1", "10.50"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
