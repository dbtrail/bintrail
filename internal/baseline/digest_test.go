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

// TestReadParquetMetadata_CorruptRowCountClearsDigest verifies the contract
// guard: a present digest paired with an unparseable row count must not be
// returned as a trustworthy digest with RowCount=0 (which would read as a
// verified-empty table). The reader clears the digest instead.
func TestReadParquetMetadata_CorruptRowCountClearsDigest(t *testing.T) {
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "shop.t-schema.sql")
	mustWrite(t, schemaPath, "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n")
	cols, err := ParseSchema(schemaPath)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}

	outPath := filepath.Join(dir, "t.parquet")
	w, err := NewWriter(outPath, cols, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	w.SetMetadata(MetaKeyContentDigest, "v1:deadbeefdeadbeef")
	w.SetMetadata(MetaKeyRowCount, "not-a-number") // corrupt
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	meta, err := ReadParquetMetadata(outPath)
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if meta.ContentDigest != "" {
		t.Errorf("ContentDigest = %q, want cleared (corrupt row count)", meta.ContentDigest)
	}
	if meta.RowCount != 0 {
		t.Errorf("RowCount = %d, want 0", meta.RowCount)
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
