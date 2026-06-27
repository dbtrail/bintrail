package baseline

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/consistency"
)

// TestRun_PersistsContentDigestAndRowCount verifies that a baseline run writes
// the per-table row count and a content digest into the Parquet metadata, that
// they round-trip through ReadParquetMetadata, and that the digest equals an
// independent consistency.Hasher over the same ingested values. No live DB or
// mydumper needed — this exercises the persist+read path on a synthetic dump.
func TestRun_PersistsContentDigestAndRowCount(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	const schema = "CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `name` varchar(64) DEFAULT NULL,\n" +
		"  `amount` decimal(10,2) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"
	const data = "INSERT INTO `t` VALUES(1,'alice','1.50'),(2,NULL,'2.00');\n"

	mustWrite(t, filepath.Join(inputDir, "metadata"), sampleMetadata)
	mustWrite(t, filepath.Join(inputDir, "shop.t-schema.sql"), schema)
	mustWrite(t, filepath.Join(inputDir, "shop.t.00000.sql"), data)

	stats, err := Run(context.Background(), Config{
		InputDir: inputDir, OutputDir: outputDir, Compression: "none", RowGroupSize: 100,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.RowsWritten != 2 {
		t.Fatalf("RowsWritten = %d, want 2", stats.RowsWritten)
	}

	meta, err := ReadParquetMetadata(findParquet(t, outputDir))
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if meta.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", meta.RowCount)
	}

	// Independent recomputation of the digest over the same ingested rows.
	h := consistency.NewHasher()
	h.AddStrings([]string{"1", "alice", "1.50"}, []bool{false, false, false})
	h.AddStrings([]string{"2", "", "2.00"}, []bool{false, true, false})
	if meta.ContentDigest != h.Digest() {
		t.Errorf("persisted digest %q != recomputed %q", meta.ContentDigest, h.Digest())
	}
	if meta.ContentDigest == "" {
		t.Error("ContentDigest is empty")
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func findParquet(t *testing.T, dir string) string {
	t.Helper()
	var p string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && filepath.Ext(path) == ".parquet" {
			p = path
		}
		return nil
	})
	if p == "" {
		t.Fatal("no .parquet file found in output directory")
	}
	return p
}
