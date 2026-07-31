//go:build integration

package reconstruct_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationResolvePKMetasAt_epochWidth pins the #1159 fix: the PK metas
// that drive the fixed BINARY(n) pad width must come from the schema snapshot
// in effect at the anchor instant (metadata.EpochAt), not from the latest
// snapshot. Scenario: a BINARY(16) PK is widened to BINARY(32) after the
// baseline was taken; the baseline file stores 16-byte values forever, so a
// pad width read from the latest snapshot (32) makes the retry look for a
// 32-byte key the baseline cannot contain — a silent miss.
func TestIntegrationResolvePKMetasAt_epochWidth(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Snapshot history: BINARY(16) on 06-01, widened to BINARY(32) on 06-03.
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, column_type, is_nullable)
		VALUES (1, '2026-06-01 00:00:00', 'app', 'vault', 'k',   1, 'PRI', 'binary',  'binary(16)',  'NO'),
		       (1, '2026-06-01 00:00:00', 'app', 'vault', 'val', 2, '',    'varchar', 'varchar(32)', 'YES'),
		       (2, '2026-06-03 00:00:00', 'app', 'vault', 'k',   1, 'PRI', 'binary',  'binary(32)',  'NO'),
		       (2, '2026-06-03 00:00:00', 'app', 'vault', 'val', 2, '',    'varchar', 'varchar(32)', 'YES')`)

	// Control first, so the scenario is proven non-vacuous: the LATEST
	// snapshot really does declare the wider width — the answer the pre-#1159
	// resolver would have used.
	latestRes, err := metadata.NewResolver(db, 0)
	if err != nil {
		t.Fatalf("NewResolver(latest): %v", err)
	}
	latestTM, err := latestRes.Resolve("app", "vault")
	if err != nil {
		t.Fatalf("resolve latest: %v", err)
	}
	latestMetas := latestTM.PKColumnMetas()
	if w := reconstruct.FixedBinaryWidth(latestMetas[0].ColumnType); w != 32 {
		t.Fatalf("latest snapshot width = %d, want 32 — the fixture no longer distinguishes the epoch anchor from the latest snapshot", w)
	}

	// The anchor: the baseline was taken on 06-02, between the two snapshots.
	baselineTime := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
	metas := reconstruct.ResolvePKMetasAt(db, "app", "vault", baselineTime)
	if len(metas) != 1 {
		t.Fatalf("ResolvePKMetasAt returned %d PK metas, want 1", len(metas))
	}
	if w := reconstruct.FixedBinaryWidth(metas[0].ColumnType); w != 16 {
		t.Fatalf("width at the baseline instant = %d, want 16 (the snapshot in effect on 06-02 declares binary(16))", w)
	}

	// An instant predating every snapshot falls back to the FIRST epoch — the
	// closest available description of the schema.
	early := reconstruct.ResolvePKMetasAt(db, "app", "vault", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	if len(early) != 1 || reconstruct.FixedBinaryWidth(early[0].ColumnType) != 16 {
		t.Errorf("pre-history instant: metas = %+v, want the first epoch's binary(16)", early)
	}

	// End to end through ReadBaselineRow: the width-16 padded baseline row
	// must resolve by its stripped pk_values spelling with the epoch metas.
	dir := t.TempDir()
	cols := []baseline.Column{
		{Name: "k", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
		{Name: "val", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	path := filepath.Join(dir, "vault.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"0x11223344556677889900AABB00000000", "sealed"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	stripped := map[string]string{"k": "0x11223344556677889900AABB"}
	row, err := reconstruct.ReadBaselineRow(ctx, path, stripped, metas)
	if err != nil {
		t.Fatalf("ReadBaselineRow (epoch metas): %v", err)
	}
	if row == nil || row["val"] != "sealed" {
		t.Fatalf("epoch metas did not resolve the width-16 baseline row, got %v", row)
	}

	// The discriminating control: the latest snapshot's width-32 metas pad the
	// same key to 32 bytes, which the width-16 baseline cannot contain — the
	// silent miss #1159 files. If this ever starts matching, the anchor stopped
	// mattering and the test above passes vacuously.
	row, err = reconstruct.ReadBaselineRow(ctx, path, stripped, latestMetas)
	if err != nil {
		t.Fatalf("ReadBaselineRow (latest metas): %v", err)
	}
	if row != nil {
		t.Fatalf("latest-snapshot metas resolved the row (%v) — the fixture no longer exercises the epoch anchor", row)
	}
}
