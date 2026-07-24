//go:build integration

package verify

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestVerifyTable_liveSource_mariadb is the #620 sweep's live-source verify
// guard for MariaDB: no prior test exercised VerifyTable (the reconstruct-
// vs-source consistency check, #634) against a real MariaDB SOURCE, despite
// ConsistentTableChecksum's own doc comment being explicit that MariaDB's JSON
// handling differs from MySQL's ("JSON is normalized (MySQL; MariaDB stores
// JSON as LONGTEXT and renders it verbatim)"). That asymmetry is deliberately
// mitigated one layer up — VerifyTable calls
// ConsistentTableChecksumNormalized with normalizeRenderedBytes, which
// canonicalizes JSON key order on BOTH sides — but it had never been proven
// against a live MariaDB connection.
//
// This wires a real MariaDB source (CreateTestMariaDB) as cfg.SourceDB against
// the usual MySQL index DB (CreateTestDB) as cfg.IndexDB — mirroring the
// architecture's own split (index is always MySQL; only the source varies) —
// and covers two things in one table: a JSON column whose baseline/event value
// has different key order than the live MariaDB text (proving the
// canonicalization hook fires for MariaDB's verbatim LONGTEXT rendering, not
// just MySQL's native-JSON normalization), and a BIGINT UNSIGNED column above
// 2^63 (the #490 class) that must fingerprint identically between the live
// MariaDB source and the reconstructed side.
func TestVerifyTable_liveSource_mariadb(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	if err := indexer.EnsureSchema(indexDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"details", "", "longtext", "longtext", 2},
		{"big", "", "bigint", "bigint unsigned", 3},
	} {
		testutil.MustExec(t, indexDB, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'audit_log', ?, ?, ?, ?, ?, 'YES', 0)`,
			sourceName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	// ── live MariaDB SOURCE: JSON stored verbatim (no server normalization),
	// key order deliberately non-alphabetical; BIGINT UNSIGNED at its max ──
	testutil.MustExec(t, sourceDB, fmt.Sprintf(
		"CREATE TABLE `%s`.`audit_log` (`id` INT PRIMARY KEY, `details` LONGTEXT, `big` BIGINT UNSIGNED)", sourceName))
	testutil.MustExec(t, sourceDB, fmt.Sprintf(
		"INSERT INTO `%s`.`audit_log` (id,details,big) VALUES (1,'{\"c\":3,\"a\":1,\"b\":2}',18446744073709551615)", sourceName))

	// ── baseline Parquet holding the same logical values ──
	createSQL := "CREATE TABLE `audit_log` (\n  `id` INT NOT NULL,\n  `details` LONGTEXT,\n  `big` BIGINT UNSIGNED,\n  PRIMARY KEY (`id`)\n);\n"
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	h2 := curHour
	tsDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, sourceName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "details", MySQLType: "longtext", ParquetType: baseline.MysqlToParquetNode("longtext")},
		{Name: "big", MySQLType: "bigint", Unsigned: true, ParquetType: baseline.MysqlToParquetNode2("bigint", true)},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "audit_log.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := bw.WriteRow([]string{"1", `{"c":3,"a":1,"b":2}`, "18446744073709551615"}, []bool{false, false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── binlog event on the INDEX db: an UPDATE touching details+big with the
	// SAME logical values, embedded as marshalRow would store them ──
	testutil.SetupPartitionedTable(t, indexDB, dbName, []time.Time{h1, h2})
	ts := now.Add(-1 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, indexDB, "binlog.000001", 100, 200, ts, nil, sourceName, "audit_log", 2 /*UPDATE*/, "1",
		[]byte(`["details","big"]`),
		[]byte(`{"id":1,"details":{"c":3,"a":1,"b":2},"big":18446744073709551615}`),
		[]byte(`{"id":1,"details":{"c":3,"a":1,"b":2},"big":18446744073709551615}`))

	resolver, err := metadata.NewResolver(indexDB, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: sourceDB, IndexDB: indexDB, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}

	got, err := VerifyTable(context.Background(), cfg, sourceName, "audit_log")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	// MariaDB has no @@global.gtid_executed, so indexCovers takes the
	// coverage-unverified branch (proceeds without GTID containment) rather
	// than blocking — the comparison still runs to completion. It must land on
	// a genuine MATCH: the JSON-canonicalization hook must reconcile MariaDB's
	// verbatim (non-normalized) LONGTEXT JSON storage against the baseline's
	// differently-ordered keys, and the BIGINT UNSIGNED max must render
	// identically on both sides.
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — MariaDB's JSON-as-LONGTEXT (no server-side key normalization) and BIGINT UNSIGNED must still reconcile via the canonicalization hook\n  source=%s recon=%s",
			got.Status, got.Detail, got.SourceDigest, got.ReconstructDigest)
	}
}
