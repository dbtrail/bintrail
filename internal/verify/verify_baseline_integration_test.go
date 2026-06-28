//go:build integration

package verify

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// writeTestBaseline writes a one-table baseline snapshot (Parquet + _SUCCESS
// marker) under baseDir at snapshot time ts. anchorFile/anchorPos, when set,
// record the binlog anchor in the Parquet metadata (#633).
func writeTestBaseline(t *testing.T, baseDir string, ts time.Time, dbName, table, createSQL string,
	cols []baseline.Column, rows [][]string, anchorFile string, anchorPos int64) {
	t.Helper()
	tsDir := strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-")
	snapDir := filepath.Join(baseDir, tsDir)
	parquetDir := filepath.Join(snapDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	md := map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}
	if anchorFile != "" {
		md[baseline.MetaKeyBinlogFile] = anchorFile
		md[baseline.MetaKeyBinlogPos] = strconv.FormatInt(anchorPos, 10)
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, table+".parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, row := range rows {
		nulls := make([]bool, len(row))
		if err := bw.WriteRow(row, nulls); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
}

// TestVerifyBaselinePair_MatchAndMismatch is the keystone of #642: reconstructing
// the previous baseline forward to the new baseline's anchor must fingerprint
// byte-identically to the new baseline itself (the recovery chain reproduces a
// fresh dump), drift-free — no live source is read. Tampering the new baseline
// then surfaces as a mismatch.
func TestVerifyBaselinePair_MatchAndMismatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// schema snapshot so the resolver has columns + PK + datetime precision.
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"status", "", "varchar", "varchar(64)", 2},
		{"ts", "", "datetime", "datetime(6)", 3},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  `ts` DATETIME(6),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "ts", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	}

	// PREV baseline: initial state {1:a, 2:b, 3:c}.
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a", "2021-01-01 00:00:00.000000"},
		{"2", "b", "2021-01-02 00:00:00.000000"},
		{"3", "c", "2021-01-03 00:00:00.000000"},
	}, "binlog.000001", 200)
	// NEW baseline: state at anchor binlog.000001:500 → {1:a, 2:shipped, 4:new}.
	newRows := [][]string{
		{"1", "a", "2021-01-01 00:00:00.000000"},
		{"2", "shipped", "2021-01-02 00:00:00.000000"},
		{"4", "new", "2021-06-15 12:30:45.000000"},
	}
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, newRows, "binlog.000001", 500)

	// Binlog events between prev and the anchor: id=2 updated, id=3 deleted, id=4 inserted.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b","ts":"2021-01-02 00:00:00.000000"}`),
		[]byte(`{"id":2,"status":"shipped","ts":"2021-01-02 00:00:00.000000"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ets, nil, dbName, "orders", 3 /*DELETE*/, "3", nil,
		[]byte(`{"id":3,"status":"c","ts":"2021-01-03 00:00:00.000000"}`), nil)
	testutil.InsertEvent(t, db, "binlog.000001", 400, 500, ets, nil, dbName, "orders", 1 /*INSERT*/, "4", nil,
		nil, []byte(`{"id":4,"status":"new","ts":"2021-06-15 12:30:45.000000"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()

	pairs, err := FindBaselinePair(ctx, baseDir)
	if err != nil {
		t.Fatalf("FindBaselinePair: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair (orders), got %d", len(pairs))
	}

	// MATCH: reconstruct(prev → anchor) == the new baseline.
	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match\n  new   =%s rows=%d\n  recon =%s rows=%d",
			got.Status, got.Detail, got.SourceDigest, got.SourceRows, got.ReconstructDigest, got.ReconstructRows)
	}

	// MISMATCH: tamper the new baseline so it no longer equals the reconstruction.
	tampered := make([][]string, len(newRows))
	copy(tampered, newRows)
	tampered[0] = []string{"1", "TAMPERED", "2021-01-01 00:00:00.000000"}
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, tampered, "binlog.000001", 500)

	got2, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair (2): %v", err)
	}
	if got2.Status != StatusMismatch {
		t.Errorf("after tampering status = %q (%s); want mismatch", got2.Status, got2.Detail)
	}
}
