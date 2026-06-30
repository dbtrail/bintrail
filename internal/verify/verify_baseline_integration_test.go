//go:build integration

package verify

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
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

// TestExplainBaselinePairMismatch is the #644 acceptance: on a known divergence
// between two at-rest baselines, the drill-down names the exact differing primary
// keys and, for a changed row, the column with recovery-vs-baseline values — all
// from the same reconstructed streams the digest used (no live source, scratch
// DB, or external tool). It exercises all three diff kinds at once.
func TestExplainBaselinePairMismatch(t *testing.T) {
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
	// prev {1:a, 2:b, 5:e, 8:x}; new (truth) {1:a, 2:shipped, 7:g, 8:""}. id=8 has
	// an EMPTY-string status in the new baseline (not NULL) — the NULL-vs-empty
	// case that bytes.Equal would silently miss.
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a"}, {"2", "b"}, {"5", "e"}, {"8", "x"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a"}, {"2", "shipped"}, {"7", "g"}, {"8", ""},
	}, "binlog.000001", 300)

	// Events in (prev, anchor]: id=2 updated b→"wrong" (diverges from the new
	// baseline's "shipped"); id=8 updated x→NULL (diverges from the baseline's
	// empty string). Nothing touches 5 (recovery keeps it; truth dropped it) or 7
	// (truth has it; recovery never gets it) → changed + extra + missing + the
	// NULL↔'' changed case.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 250, ets, nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"wrong"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 250, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "8", nil,
		[]byte(`{"id":8,"status":"x"}`), []byte(`{"id":8,"status":null}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	// Precondition: it is a real mismatch.
	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMismatch {
		t.Fatalf("precondition: want mismatch, got %q (%s)", got.Status, got.Detail)
	}

	ex, err := ExplainBaselinePairMismatch(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("ExplainBaselinePairMismatch: %v", err)
	}

	changed := map[string]RowDiff{}
	missing := map[string]bool{}
	extra := map[string]bool{}
	for _, d := range ex.Diffs {
		switch d.Kind {
		case diffChanged:
			changed[d.PK] = d
		case diffMissing:
			missing[d.PK] = true
		case diffExtra:
			extra[d.PK] = true
		}
	}
	if ex.Total != 4 {
		t.Errorf("want 4 differing rows (2 changed, 1 extra, 1 missing), got Total=%d: %+v", ex.Total, ex.Diffs)
	}
	if d, ok := changed["id=2"]; !ok {
		t.Errorf("want a changed row for id=2, got changed=%v", changed)
	} else if len(d.Cells) != 1 || d.Cells[0].Column != "status" || d.Cells[0].Recovery != "wrong" || d.Cells[0].Baseline != "shipped" {
		t.Errorf("id=2 cell diff = %+v; want status recovery=wrong baseline=shipped", d.Cells)
	}
	// id=8 is the NULL-vs-empty case: recovery rendered NULL, baseline an empty
	// string — cellEqual must NOT collapse them (bytes.Equal would, missing it).
	if d, ok := changed["id=8"]; !ok {
		t.Errorf("want a changed row for id=8 (NULL vs empty), got changed=%v", changed)
	} else if len(d.Cells) != 1 || d.Cells[0].Column != "status" || d.Cells[0].Recovery != "NULL" || d.Cells[0].Baseline != "" {
		t.Errorf("id=8 cell diff = %+v; want status recovery=NULL baseline=(empty)", d.Cells)
	}
	if !extra["id=5"] {
		t.Errorf("want id=5 as extra (in recovery, not the new baseline), got extra=%v", extra)
	}
	if !missing["id=7"] {
		t.Errorf("want id=7 as missing (in the new baseline, not reproduced), got missing=%v", missing)
	}
}

// TestExplainBaselinePairMismatch_CompositePK exercises the most fragile path —
// the multi-column PK join. With a shared-prefix pair (tenant_id=1, id=1) and
// (tenant_id=1, id=2), a single UPDATE touching ONLY (1,2) must land on (1,2) and
// NOT (1,1): the drill-down must report exactly one changed row whose full PK
// label is "tenant_id=1, id=2". A dropped or reordered PK column in any of the
// three coordinated encodings (FetchMerged pk_values, SnapshotFullTableImages
// canonicalization, pkKeyAndDisplay) would misfire and this would catch it.
func TestExplainBaselinePairMismatch_CompositePK(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"tenant_id", "PRI", "int", "int", 1},
		{"id", "PRI", "int", "int", 2},
		{"status", "", "varchar", "varchar(64)", 3},
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
	createSQL := "CREATE TABLE `orders` (\n  `tenant_id` INT NOT NULL,\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`tenant_id`, `id`)\n);\n"
	cols := []baseline.Column{
		{Name: "tenant_id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "1", "a"}, {"1", "2", "b"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "1", "a"}, {"1", "2", "shipped"},
	}, "binlog.000001", 300)

	// UPDATE only (1,2): b→"wrong". (1,1) is untouched and identical on both sides.
	// pk_values for a composite key is "|"-joined in PK ordinal order (BuildPKValues).
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "1|2", nil,
		[]byte(`{"tenant_id":1,"id":2,"status":"b"}`), []byte(`{"tenant_id":1,"id":2,"status":"wrong"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}
	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMismatch {
		t.Fatalf("precondition: want mismatch, got %q (%s)", got.Status, got.Detail)
	}

	ex, err := ExplainBaselinePairMismatch(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("ExplainBaselinePairMismatch: %v", err)
	}
	if ex.Total != 1 || len(ex.Diffs) != 1 {
		t.Fatalf("want exactly 1 differing row (the change landed only on (1,2)), got Total=%d Diffs=%+v", ex.Total, ex.Diffs)
	}
	d := ex.Diffs[0]
	if d.Kind != diffChanged || d.PK != "tenant_id=1, id=2" {
		t.Errorf("diff = {Kind:%s PK:%q}; want a changed row PK 'tenant_id=1, id=2' (dropped/reordered PK column misfires)", d.Kind, d.PK)
	}
	if len(d.Cells) != 1 || d.Cells[0].Column != "status" || d.Cells[0].Recovery != "wrong" || d.Cells[0].Baseline != "shipped" {
		t.Errorf("cell diff = %+v; want status recovery=wrong baseline=shipped", d.Cells)
	}
}

// TestExplainBaselinePairMismatch_DeferredDrift exercises the deferred-type path
// end to end: an ENUM column that drifts between the two baselines with NO
// in-window event (the at-rest silent-drift case). Both sides are mydumper labels,
// directly comparable, so the drill-down must show them RAW ("active" vs
// "inactive") — never blanked, which would hide exactly the drift this command
// exists to catch — and surface the representation caveat because a deferred-type
// column is among the diffs.
func TestExplainBaselinePairMismatch_DeferredDrift(t *testing.T) {
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
		{"state", "", "enum", "enum('active','inactive')", 2},
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
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `state` ENUM('active','inactive'),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "state", MySQLType: "enum", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "active"}}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{{"1", "inactive"}}, "binlog.000001", 300)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}
	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMismatch {
		t.Fatalf("precondition: an ENUM drift with no in-window event must be a mismatch (not inconclusive), got %q (%s)", got.Status, got.Detail)
	}

	ex, err := ExplainBaselinePairMismatch(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("ExplainBaselinePairMismatch: %v", err)
	}
	if ex.Total != 1 || len(ex.Diffs) != 1 {
		t.Fatalf("want exactly 1 changed row, got Total=%d Diffs=%+v", ex.Total, ex.Diffs)
	}
	// Shown RAW (the revert): the marker would have blanked this real drift.
	d := ex.Diffs[0]
	if len(d.Cells) != 1 || d.Cells[0].Column != "state" || d.Cells[0].Recovery != "active" || d.Cells[0].Baseline != "inactive" {
		t.Errorf("want state shown raw (recovery=active baseline=inactive), got %+v", d.Cells)
	}
	if !ex.deferredSeen {
		t.Error("a deferred-type column was among the diffs; the caveat flag must be set")
	}
	var buf bytes.Buffer
	ex.Write(&buf)
	if !strings.Contains(buf.String(), "deferred-type column") {
		t.Errorf("want the deferred caveat in the output, got:\n%s", buf.String())
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

// TestVerifyBaselinePair_TextEventDecoded is the #672 regression: a TEXT
// column's in-window event value (stored base64, since go-mysql delivers
// TEXT as []byte and marshalRow base64-encodes it) must be decoded before
// SnapshotFullTableImages compares it to the baseline/source's plain text —
// neither VerifyBaselinePair's digest nor ExplainBaselinePairMismatch's
// drill-down decoded it before this fix, so a TEXT-only change (id=1 below)
// would surface as a false mismatch even though nothing actually diverged.
//
// id=2's status is deliberately, genuinely wrong (unrelated to #672) so the
// table-level result is a real mismatch and ExplainBaselinePairMismatch has
// something to drill into — proving the TEXT decode neither masks a real
// divergence nor gets masked by one.
func TestVerifyBaselinePair_TextEventDecoded(t *testing.T) {
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
		{"body", "", "text", "text", 3},
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
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  `body` TEXT,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "body", MySQLType: "text", ParquetType: baseline.MysqlToParquetNode("text")},
	}
	// prev {1:a/hello, 2:b/static}; new (truth) {1:a/"updated text", 2:shipped/static}.
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a", "hello"}, {"2", "b", "static"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a", "updated text"}, {"2", "shipped", "static"},
	}, "binlog.000001", 300)

	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	// Events in (prev, anchor]: id=1's body is updated to "updated text" (matches
	// truth once decoded); id=2's status is updated to a WRONG value ("wrong",
	// diverges from truth's "shipped") while body is carried unchanged — every
	// column appears in row_after under binlog_row_image=FULL, so body is
	// base64-encoded here too even though its value didn't change.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 250, ets, nil, dbName, "orders", 2 /*UPDATE*/, "1", nil,
		[]byte(fmt.Sprintf(`{"id":1,"status":"a","body":"%s"}`, b64("hello"))),
		[]byte(fmt.Sprintf(`{"id":1,"status":"a","body":"%s"}`, b64("updated text"))))
	testutil.InsertEvent(t, db, "binlog.000001", 250, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(fmt.Sprintf(`{"id":2,"status":"b","body":"%s"}`, b64("static"))),
		[]byte(fmt.Sprintf(`{"id":2,"status":"wrong","body":"%s"}`, b64("static"))))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	// Mismatch is expected: id=2's status genuinely diverges (unrelated to #672,
	// predates it). If id=1's body decode were broken, this would ALSO be a
	// mismatch, for the wrong reason — the explain assertions below are what
	// distinguish "id=1 correctly excluded" from "id=1 spuriously included".
	if got.Status != StatusMismatch {
		t.Fatalf("status = %q (%s); want mismatch (id=2's status genuinely diverges)", got.Status, got.Detail)
	}

	ex, err := ExplainBaselinePairMismatch(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("ExplainBaselinePairMismatch: %v", err)
	}

	var id1Diff, id2Diff *RowDiff
	for i := range ex.Diffs {
		switch ex.Diffs[i].PK {
		case "id=1":
			id1Diff = &ex.Diffs[i]
		case "id=2":
			id2Diff = &ex.Diffs[i]
		}
	}

	// id=1 (TEXT-only change) must NOT appear in the diff at all: its body was
	// decoded from the event's stored base64 to "updated text", matching truth
	// exactly. Pre-#672, the undecoded raw base64 would mismatch truth's plain
	// "updated text" and id=1 would show up here as a spurious diffChanged.
	if id1Diff != nil {
		t.Errorf("id=1 (TEXT-only change) should not appear in diffs — body should have decoded to match truth, got: %+v", *id1Diff)
	}

	// id=2 (real status divergence) must still be reported, with EXACTLY the
	// status cell — not also a spurious body cell. Pre-#672, body's undecoded
	// base64 ("c3RhdGlj") would mismatch truth's plain "static" too, adding a
	// second, spurious cell diff alongside the real one.
	if id2Diff == nil {
		t.Fatalf("id=2 (real status divergence) must appear in diffs")
	}
	if id2Diff.Kind != diffChanged {
		t.Fatalf("id=2 diff kind = %q, want %q", id2Diff.Kind, diffChanged)
	}
	if len(id2Diff.Cells) != 1 || id2Diff.Cells[0].Column != "status" {
		t.Fatalf("id=2 cells = %+v, want exactly one status cell (body must not appear — it decoded and matched)", id2Diff.Cells)
	}
	if id2Diff.Cells[0].Recovery != "wrong" || id2Diff.Cells[0].Baseline != "shipped" {
		t.Errorf("id=2 status cell = %+v, want recovery=%q baseline=%q", id2Diff.Cells[0], "wrong", "shipped")
	}
}

// TestVerifyBaselinePair_TextOnlyChange_Match isolates VerifyBaselinePair's
// OWN decode call (#672): a TEXT column changed by an in-window event, with
// NO other divergence in the table, must report StatusMatch directly. Unlike
// TestVerifyBaselinePair_TextEventDecoded above (which needs a genuine,
// unrelated mismatch for ExplainBaselinePairMismatch to drill into, and so
// can't tell VerifyBaselinePair's own decode apart from ExplainBaselinePair-
// Mismatch's independent one), this test has nothing else that could produce
// a mismatch — if VerifyBaselinePair's decode call were missing, the digest
// would hash the raw base64 instead of "updated text" and this would report
// StatusMismatch instead.
func TestVerifyBaselinePair_TextOnlyChange_Match(t *testing.T) {
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
		{"body", "", "text", "text", 2},
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
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `body` TEXT,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "body", MySQLType: "text", ParquetType: baseline.MysqlToParquetNode("text")},
	}
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "hello"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "updated text"},
	}, "binlog.000001", 300)

	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	// changed_columns is populated realistically here (unlike the nil used
	// elsewhere in this file): there is no unrelated deferred-type column for
	// deferredReprChanged to gate on, so this exercises the real indexer's
	// ChangedColumns path without it affecting the outcome.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "1", []byte(`["body"]`),
		[]byte(fmt.Sprintf(`{"id":1,"body":"%s"}`, b64("hello"))),
		[]byte(fmt.Sprintf(`{"id":1,"body":"%s"}`, b64("updated text"))))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match (TEXT body should decode to \"updated text\", matching the new baseline)", got.Status, got.Detail)
	}
}
