package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
)

// TestReadBaselineRows_validatesIntegrity is the cascade read-path (#636) end to
// end through REAL DuckDB: a baseline that carries a manifest reads its rows
// normally (proving a present manifest does NOT false-positive), and a corrupted
// file fails loud with ErrIntegrity before parquet_scan returns anything. It runs
// a real baseline.Run so the snapshot-dir↔layout coupling (ValidateLocalFile's
// grandparent vs Run's <ts>/<db>/<table>.parquet) is exercised, not mocked, and
// it locks the validate-before-rows ordering against a future refactor. No MySQL:
// Run is mydumper-SQL→Parquet and ReadBaselineRows is DuckDB-only.
func TestReadBaselineRows_validatesIntegrity(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()
	writeF := func(name, data string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(inputDir, name), []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeF("metadata", "Started dump at: 2025-02-28 00:00:00\nSHOW MASTER STATUS:\n\tLog: binlog.000001\n\tPos: 100\n\tGTID:\n")
	writeF("shop.orders-schema.sql", "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(64) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n")
	writeF("shop.orders.00000.sql", "INSERT INTO `orders` VALUES(1,'shipped'),(2,'pending');\n")

	if _, err := baseline.Run(context.Background(), baseline.Config{InputDir: inputDir, OutputDir: outputDir, Compression: "none", RowGroupSize: 100}); err != nil {
		t.Fatalf("baseline.Run: %v", err)
	}
	var parquetPath string
	_ = filepath.Walk(outputDir, func(p string, info os.FileInfo, err error) error {
		if err == nil && filepath.Ext(p) == ".parquet" {
			parquetPath = p
		}
		return nil
	})
	if parquetPath == "" {
		t.Fatal("no .parquet produced")
	}

	// Clean + manifest present → rows come back through DuckDB (no false positive).
	rows, err := ReadBaselineRows(context.Background(), parquetPath, nil, 0)
	if err != nil || len(rows) != 2 {
		t.Fatalf("a clean baseline with a manifest must read normally: err=%v rows=%d", err, len(rows))
	}

	// Flip a byte in the middle of the file → fail loud before parquet_scan.
	b, err := os.ReadFile(parquetPath)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xff
	if err := os.WriteFile(parquetPath, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBaselineRows(context.Background(), parquetPath, map[string]string{"id": "1"}, 0); !errors.Is(err, baselineintegrity.ErrIntegrity) {
		t.Errorf("a corrupt baseline must fail loud with ErrIntegrity, got %v", err)
	}
}
