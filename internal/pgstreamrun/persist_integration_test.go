//go:build integration

package pgstreamrun

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestPersistSlotLostPG pins the #532 upsert contract (white-box, MySQL index only):
//   - with NO stream_state row (a slot detected lost before the first checkpoint), the
//     stamp must SEED a complete row — a bare UPDATE would match zero rows and record
//     nothing (the false-negative the CRITICAL review caught);
//   - with an existing checkpoint row, the stamp must set gap_lost_* WITHOUT clobbering
//     the saved position/checkpoint.
func TestPersistSlotLostPG(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	indexDB, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, indexDB, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	logger := slog.New(slog.DiscardHandler)

	// (1) No row yet → the stamp must seed one.
	persistSlotLostPG(indexDB, 51, "replication slot invalidated (wal_status=lost)", logger)

	var gapAt sql.NullTime
	var gapDetail sql.NullString
	var serverID uint32
	var mode string
	if err := indexDB.QueryRowContext(ctx,
		"SELECT mode, server_id, gap_lost_at, gap_lost_detail FROM stream_state WHERE id=1").
		Scan(&mode, &serverID, &gapAt, &gapDetail); err != nil {
		t.Fatalf("no row was seeded by persistSlotLostPG (the CRITICAL no-op): %v", err)
	}
	if !gapAt.Valid || gapDetail.String != "replication slot invalidated (wal_status=lost)" {
		t.Errorf("seeded row missing gap_lost: at=%v detail=%q", gapAt, gapDetail.String)
	}
	if serverID != 51 || mode != "gtid" {
		t.Errorf("seeded row has wrong NOT NULL fields: server_id=%d mode=%q", serverID, mode)
	}

	// (2) Existing checkpoint row → the stamp must preserve the position/checkpoint and
	// only update gap_lost_*. Write a real checkpoint first.
	if _, err := indexDB.ExecContext(ctx,
		`UPDATE stream_state SET binlog_position=999, binlog_file='5/ABC', gap_lost_at=NULL, gap_lost_detail=NULL WHERE id=1`); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	persistSlotLostPG(indexDB, 51, "lost again", logger)

	var pos uint64
	var file string
	if err := indexDB.QueryRowContext(ctx,
		"SELECT binlog_position, binlog_file, gap_lost_detail FROM stream_state WHERE id=1").
		Scan(&pos, &file, &gapDetail); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if pos != 999 || file != "5/ABC" {
		t.Errorf("stamp clobbered the checkpoint position: pos=%d file=%q (want 999 / 5/ABC)", pos, file)
	}
	if gapDetail.String != "lost again" {
		t.Errorf("gap_lost_detail not updated on the existing row: %q", gapDetail.String)
	}
}
