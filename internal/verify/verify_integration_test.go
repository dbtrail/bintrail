//go:build integration

package verify

import (
	"context"
	"encoding/base64"
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

// TestVerifyTable_MatchesAndDetectsDivergence is the keystone of #634: a table
// reconstructed from baseline + binlog must fingerprint byte-identically to the
// live source (proving a recovery would reproduce it), and a divergence must be
// reported. It exercises the renderer agreement against the real CAST-fixed
// ConsistentTableChecksum across baseline-origin (DuckDB time.Time/int/string)
// and event-origin (json.Number/string) values, including DATETIME(6) precision.
func TestVerifyTable_MatchesAndDetectsDivergence(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// ── schema snapshot so the resolver has columns + PK + datetime precision ──
	for _, c := range []struct {
		name             string
		ord              int
		key, dt, colType string
		isGenerated      int
	}{
		{"id", 1, "PRI", "int", "int", 0},
		{"status", 2, "", "varchar", "varchar(64)", 0},
		{"ts", 3, "", "datetime", "datetime(6)", 0},
		{"amount", 5, "", "double", "double", 0},
		// created_at is an ordinary DEFAULT CURRENT_TIMESTAMP column. The schema
		// snapshotter's substring capture mis-flags it is_generated=1 (the
		// DEFAULT_GENERATED trap) — set that here to lock that verify still hashes
		// it, by following ConsistentTableChecksum's column set rather than
		// tm.IsGenerated. Omitting it would be the C1 false-mismatch.
		{"created_at", 4, "", "datetime", "datetime", 1},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'NO', ?)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType, c.isGenerated)
	}

	// ── live SOURCE table at the FINAL (post-event) state ──
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`orders` (`id` INT PRIMARY KEY, `status` VARCHAR(64), `ts` DATETIME(6),"+
			" `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP, `amount` DOUBLE)", dbName))
	testutil.MustExec(t, db, fmt.Sprintf("INSERT INTO `%s`.`orders` (id,status,ts,created_at,amount) VALUES"+
		"(1,'a','2021-01-01 00:00:00.123456','2020-01-01 00:00:00',1.5),"+
		"(2,'shipped','2021-01-02 00:00:00.000000','2020-01-02 00:00:00',2.25),"+
		"(4,'new','2021-06-15 12:30:45.000000','2020-06-15 00:00:00',9)", dbName))

	// ── baseline Parquet at the INITIAL state {1:a, 2:b, 3:c} ──
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  `ts` DATETIME(6),\n  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,\n  `amount` DOUBLE,\n  PRIMARY KEY (`id`)\n);\n"
	baselineDir := t.TempDir()
	// Keep the whole window recent and partition-covered: the baseline anchors at
	// the previous hour and events land a couple of minutes ago, so [snapshot,
	// now] spans only the previous+current hour (both partitioned) — no spurious
	// "rotated and not archived" gap.
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	h2 := curHour
	snapshotTS := h1
	tsDir := strings.ReplaceAll(snapshotTS.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "ts", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
		{Name: "created_at", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
		{Name: "amount", MySQLType: "double", ParquetType: baseline.MysqlToParquetNode("double")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "orders.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, row := range [][]string{
		{"1", "a", "2021-01-01 00:00:00.123456", "2020-01-01 00:00:00", "1.5"},
		{"2", "b", "2021-01-02 00:00:00.000000", "2020-01-02 00:00:00", "2.25"},
		{"3", "c", "2021-01-03 00:00:00.000000", "2020-01-03 00:00:00", "3.5"},
	} {
		if err := bw.WriteRow(row, []bool{false, false, false, false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── binlog events: 2 updated, 3 deleted, 4 inserted ──
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2})
	// Events strictly before now (so a Until=now reconstruct includes them) and
	// inside the partitioned hours.
	ts1 := now.Add(-2 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := now.Add(-1 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b","ts":"2021-01-02 00:00:00.000000","created_at":"2020-01-02 00:00:00","amount":2.25}`),
		[]byte(`{"id":2,"status":"shipped","ts":"2021-01-02 00:00:00.000000","created_at":"2020-01-02 00:00:00","amount":2.25}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts1, nil, dbName, "orders", 3 /*DELETE*/, "3", nil,
		[]byte(`{"id":3,"status":"c","ts":"2021-01-03 00:00:00.000000","created_at":"2020-01-03 00:00:00","amount":3.5}`), nil)
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts2, nil, dbName, "orders", 1 /*INSERT*/, "4", nil,
		nil, []byte(`{"id":4,"status":"new","ts":"2021-06-15 12:30:45.000000","created_at":"2020-06-15 00:00:00","amount":9}`))

	// ── stream_state with a GTID superset so the coverage check passes ──
	var uuid string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1)`, uuid+":1-1000000")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}
	ctx := context.Background()

	// MATCH: reconstruct == live source.
	got, err := VerifyTable(ctx, cfg, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match\n  source=%s rows=%d\n  recon =%s rows=%d",
			got.Status, got.Detail, got.SourceDigest, got.SourceRows, got.ReconstructDigest, got.ReconstructRows)
	}

	// MISMATCH: corrupt the live source so it no longer matches the reconstruction.
	testutil.MustExec(t, db, fmt.Sprintf("UPDATE `%s`.`orders` SET status='TAMPERED' WHERE id=1", dbName))
	got2, err := VerifyTable(ctx, cfg, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyTable (2): %v", err)
	}
	if got2.Status != StatusMismatch {
		t.Errorf("after tampering status = %q (%s); want mismatch", got2.Status, got2.Detail)
	}
}

// TestVerifyTable_TextEventDecoded is the #672 regression for the live-source
// path: a TEXT column changed by an in-window event must decode before
// comparison, the same fix TestVerifyBaselinePair_TextOnlyChange_Match proves
// for the baseline-anchored path. Neither
// TestVerifyTable_MatchesAndDetectsDivergence above nor any other existing
// test in this package exercises a TEXT/BLOB column through VerifyTable, so
// this closes the only one of the three #672 call sites that had zero
// coverage. If VerifyTable's DecodeEventBinaries call were missing, the
// reconstructed digest would hash the event's raw base64 instead of "updated
// text" and this would report StatusMismatch.
func TestVerifyTable_TextEventDecoded(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), ?, 'orders', 'id', 1, 'PRI', 'int', 'int', 'NO', 0)`, dbName)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), ?, 'orders', 'body', 2, '', 'text', 'text', 'YES', 0)`, dbName)

	// ── live SOURCE table at the FINAL (post-event) state ──
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`orders` (`id` INT PRIMARY KEY, `body` TEXT)", dbName))
	testutil.MustExec(t, db, fmt.Sprintf(
		"INSERT INTO `%s`.`orders` (id,body) VALUES (1,'updated text')", dbName))

	// ── baseline Parquet at the INITIAL state {1:hello} ──
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `body` TEXT,\n  PRIMARY KEY (`id`)\n);\n"
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	h2 := curHour
	tsDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "body", MySQLType: "text", ParquetType: baseline.MysqlToParquetNode("text")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "orders.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := bw.WriteRow([]string{"1", "hello"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── binlog event: body updated, base64-encoded as TEXT is delivered ──
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2})
	ts := now.Add(-1 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, dbName, "orders", 2 /*UPDATE*/, "1", []byte(`["body"]`),
		[]byte(fmt.Sprintf(`{"id":1,"body":"%s"}`, b64("hello"))),
		[]byte(fmt.Sprintf(`{"id":1,"body":"%s"}`, b64("updated text"))))

	// ── stream_state with a GTID superset so the coverage check passes ──
	var uuid string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1)`, uuid+":1-1000000")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}

	got, err := VerifyTable(context.Background(), cfg, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match (TEXT body should decode to \"updated text\", matching the live source)", got.Status, got.Detail)
	}
}
