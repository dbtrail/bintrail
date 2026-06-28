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
		ord, isGen             int
	}{
		{"id", "PRI", "int", "int", 1, 0},
		{"status", "", "varchar", "varchar(64)", 2, 0},
		{"ts", "", "datetime", "datetime(6)", 3, 0},
		// created_at is an ordinary DEFAULT CURRENT_TIMESTAMP column the snapshotter
		// mis-flags is_generated=1 (DEFAULT_GENERATED trap). It IS in the baseline
		// Parquet, so it must be hashed — locking that the column set comes from the
		// Parquet, not the is_generated flag (else corruption here would read MATCH).
		{"created_at", "", "datetime", "datetime", 4, 1},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', ?)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType, c.isGen)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  `ts` DATETIME(6),\n  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "ts", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
		{Name: "created_at", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	}

	// PREV baseline: initial state {1:a, 2:b, 3:c}.
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a", "2021-01-01 00:00:00.000000", "2020-01-01 00:00:00"},
		{"2", "b", "2021-01-02 00:00:00.000000", "2020-01-02 00:00:00"},
		{"3", "c", "2021-01-03 00:00:00.000000", "2020-01-03 00:00:00"},
	}, "binlog.000001", 200)
	// NEW baseline: state at anchor binlog.000001:500 → {1:a, 2:shipped, 4:new}.
	newRows := [][]string{
		{"1", "a", "2021-01-01 00:00:00.000000", "2020-01-01 00:00:00"},
		{"2", "shipped", "2021-01-02 00:00:00.000000", "2020-01-02 00:00:00"},
		{"4", "new", "2021-06-15 12:30:45.000000", "2020-06-15 00:00:00"},
	}
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, newRows, "binlog.000001", 500)

	// Binlog events between prev and the anchor: id=2 updated, id=3 deleted, id=4 inserted.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b","ts":"2021-01-02 00:00:00.000000","created_at":"2020-01-02 00:00:00"}`),
		[]byte(`{"id":2,"status":"shipped","ts":"2021-01-02 00:00:00.000000","created_at":"2020-01-02 00:00:00"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ets, nil, dbName, "orders", 3 /*DELETE*/, "3", nil,
		[]byte(`{"id":3,"status":"c","ts":"2021-01-03 00:00:00.000000","created_at":"2020-01-03 00:00:00"}`), nil)
	testutil.InsertEvent(t, db, "binlog.000001", 400, 500, ets, nil, dbName, "orders", 1 /*INSERT*/, "4", nil,
		nil, []byte(`{"id":4,"status":"new","ts":"2021-06-15 12:30:45.000000","created_at":"2020-06-15 00:00:00"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()

	pairs, unpaired, prevOnly, err := FindBaselinePair(ctx, baseDir)
	if err != nil {
		t.Fatalf("FindBaselinePair: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair (orders), got %d", len(pairs))
	}
	if len(unpaired) != 0 {
		t.Errorf("expected no unpaired tables, got %v", unpaired)
	}
	if len(prevOnly) != 0 {
		t.Errorf("expected no prev-only tables, got %v", prevOnly)
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

	// MISMATCH: tamper ONLY the created_at column (the DEFAULT_GENERATED one) in
	// the new baseline. This must surface as a mismatch — proving created_at is in
	// the digest. With the buggy is_generated-based column set it would be excluded
	// and the tamper would falsely read MATCH.
	tampered := make([][]string, len(newRows))
	copy(tampered, newRows)
	tampered[0] = []string{"1", "a", "2021-01-01 00:00:00.000000", "1999-12-31 00:00:00"}
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, tampered, "binlog.000001", 500)

	got2, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair (2): %v", err)
	}
	if got2.Status != StatusMismatch {
		t.Errorf("after tampering status = %q (%s); want mismatch", got2.Status, got2.Detail)
	}

	// A zero anchor position must refuse (inconclusive), not bound at position 0.
	badAnchor := pairs[0]
	badAnchor.NewAnchor.Pos = 0
	gotZero, err := VerifyBaselinePair(ctx, cfg, badAnchor)
	if err != nil {
		t.Fatalf("VerifyBaselinePair (zero anchor): %v", err)
	}
	if gotZero.Status != StatusInconclusive {
		t.Errorf("zero anchor: status = %q (%s); want inconclusive", gotZero.Status, gotZero.Detail)
	}
}

// TestFindBaselinePair_UnpairedAndSelection locks two things: a table present
// only in the new snapshot lands in `unpaired` (not silently dropped), and the
// pair is built from the two most recent snapshots, ignoring an older third.
func TestFindBaselinePair_UnpairedAndSelection(t *testing.T) {
	baseDir := t.TempDir()
	now := time.Now().UTC()
	createSQL := "CREATE TABLE `t` (\n  `id` INT NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")}}
	rows := [][]string{{"1"}}
	oldTS := now.Truncate(time.Hour).Add(-3 * time.Hour)
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := now.Truncate(time.Hour).Add(-1 * time.Hour)

	writeTestBaseline(t, baseDir, oldTS, "db", "orders", createSQL, cols, rows, "binlog.000001", 100) // ignored (older)
	writeTestBaseline(t, baseDir, prevTS, "db", "orders", createSQL, cols, rows, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, "db", "orders", createSQL, cols, rows, "binlog.000001", 300)
	writeTestBaseline(t, baseDir, newTS, "db", "fresh", createSQL, cols, rows, "binlog.000001", 300) // new-only

	pairs, unpaired, _, err := FindBaselinePair(context.Background(), baseDir)
	if err != nil {
		t.Fatalf("FindBaselinePair: %v", err)
	}
	if len(pairs) != 1 || pairs[0].Table != "orders" {
		t.Fatalf("expected one pair for orders (newest two snapshots), got %+v", pairs)
	}
	// The pair must use the prev (not the older) snapshot's path.
	if !pairs[0].PrevSnapshot.Equal(prevTS) {
		t.Errorf("pair PrevSnapshot = %v, want %v (must ignore the older snapshot)", pairs[0].PrevSnapshot, prevTS)
	}
	if len(unpaired) != 1 || unpaired[0].Table != "fresh" {
		t.Errorf("expected 'fresh' in unpaired (new since prev), got %+v", unpaired)
	}
}

// TestFindBaselinePair_PrevOnly locks the symmetric reverse of the unpaired
// case: a table present in the previous snapshot but absent from the newest one
// (a drop, or a subset "--tables" re-baseline) must surface in prevOnly, never
// silently vanish. Without it a default verify could report a clean pass while
// such a table was never checked or even printed.
func TestFindBaselinePair_PrevOnly(t *testing.T) {
	baseDir := t.TempDir()
	now := time.Now().UTC()
	createSQL := "CREATE TABLE `t` (\n  `id` INT NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")}}
	rows := [][]string{{"1"}}
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := now.Truncate(time.Hour).Add(-1 * time.Hour)

	// prev snapshot carries two tables; the newest re-snapshots only orders.
	writeTestBaseline(t, baseDir, prevTS, "db", "orders", createSQL, cols, rows, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, prevTS, "db", "customers", createSQL, cols, rows, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, "db", "orders", createSQL, cols, rows, "binlog.000001", 300)

	pairs, unpaired, prevOnly, err := FindBaselinePair(context.Background(), baseDir)
	if err != nil {
		t.Fatalf("FindBaselinePair: %v", err)
	}
	if len(pairs) != 1 || pairs[0].Table != "orders" {
		t.Fatalf("expected one pair for orders, got %+v", pairs)
	}
	if len(unpaired) != 0 {
		t.Errorf("expected no unpaired tables, got %+v", unpaired)
	}
	if len(prevOnly) != 1 || prevOnly[0].Table != "customers" {
		t.Errorf("expected 'customers' in prevOnly (in prev, absent from newest), got %+v", prevOnly)
	}
}

// TestVerifyBaselinePair_UnchangedTable covers the most common pass case: the
// prev baseline is content-identical to the new one and no events fall in the
// window, so the reconstruction is pure baseline passthrough ⇒ match.
func TestVerifyBaselinePair_UnchangedTable(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"status", "", "varchar", "varchar(64)", 2},
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
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	rows := [][]string{{"1", "a"}, {"2", "b"}}
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, rows, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, rows, "binlog.000001", 200)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	pairs, _, _, err := FindBaselinePair(context.Background(), baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}
	got, err := VerifyBaselinePair(context.Background(), cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Errorf("unchanged table: status = %q (%s); want match", got.Status, got.Detail)
	}
}
