//go:build integration

package baseline

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestBaselineDigest_MatchesLiveSource is the keystone of issue #633: the digest
// a baseline persists must be byte-identical to a live ConsistentTableChecksum
// (#632) of the same rows — that identity is what makes the verify capstone
// (#634) able to compare a baseline against its source.
//
// It inserts a type-matrix of rows into a live MySQL table, fingerprints the
// live table (#632), then builds an equivalent mydumper dump of those rows, runs
// the baseline conversion, and asserts the persisted digest equals the live one.
// A divergence on any type is a real parser-vs-text-protocol bug, not a flaky
// test.
func TestBaselineDigest_MatchesLiveSource(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, "CREATE TABLE `t` ("+
		"`id` INT PRIMARY KEY,"+
		"`name` VARCHAR(64),"+
		"`big` BIGINT UNSIGNED,"+
		"`amount` DECIMAL(10,2),"+
		"`ts` DATETIME(6),"+
		"`d` DATE,"+
		"`note` VARCHAR(64)"+
		") CHARACTER SET utf8mb4")
	// Row 2's note is NULL; values span unsigned>2^63, decimals, fractional
	// datetime, date, multibyte utf8mb4 — including the temporal types the
	// keystone caught rendering as RFC3339 before the CAST fix.
	testutil.MustExec(t, db, "INSERT INTO `t` VALUES"+
		"(1,'café',18446744073709551615,'1.50','2021-01-01 00:00:00.123456','2021-03-04','note'),"+
		"(2,'日本語',9223372036854775808,'2.00','2022-06-15 12:30:45.000000','1999-12-31',NULL)")

	ctx := context.Background()
	live, err := consistency.ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("live checksum: %v", err)
	}

	// Pull the exact CREATE TABLE so the baseline schema matches the source.
	var tblName, ddl string
	if err := db.QueryRow("SHOW CREATE TABLE `t`").Scan(&tblName, &ddl); err != nil {
		t.Fatalf("show create table: %v", err)
	}

	inputDir := t.TempDir()
	outputDir := t.TempDir()
	// mydumper dump equivalent to the inserted rows (text-protocol form).
	data := "INSERT INTO `t` VALUES" +
		"(1,'café',18446744073709551615,'1.50','2021-01-01 00:00:00.123456','2021-03-04','note')," +
		"(2,'日本語',9223372036854775808,'2.00','2022-06-15 12:30:45.000000','1999-12-31',NULL);\n"
	mustWrite(t, filepath.Join(inputDir, "metadata"), sampleMetadata)
	mustWrite(t, filepath.Join(inputDir, schema+".t-schema.sql"), ddl+";\n")
	mustWrite(t, filepath.Join(inputDir, schema+".t.00000.sql"), data)

	if _, err := Run(ctx, Config{InputDir: inputDir, OutputDir: outputDir, Compression: "none", RowGroupSize: 100}); err != nil {
		t.Fatalf("baseline Run: %v", err)
	}

	meta, err := ReadParquetMetadata(findParquet(t, outputDir))
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}

	if meta.RowCount != live.RowCount {
		t.Errorf("row count: baseline=%d live=%d", meta.RowCount, live.RowCount)
	}
	if meta.ContentDigest != live.Digest {
		t.Errorf("digest mismatch:\n  baseline=%s\n  live    =%s\n(a real parser-vs-text-protocol divergence if non-empty)",
			meta.ContentDigest, live.Digest)
	}
}
