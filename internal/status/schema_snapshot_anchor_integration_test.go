//go:build integration

package status_test

import (
	"context"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The snapshot anchor (#1312) is what lets the console tell a capture failure
// that is STILL HAPPENING from a record of one already fixed — the monotonic
// tally cannot, which is why pressing "Refresh schema snapshot" changed nothing
// on screen. The verdict is unit-tested against fixtures; what those cannot
// cover is that LoadStreamState actually READS the column. Delete the
// loadSchemaSnapshotTime call and every fixture test still passes, because they
// set the field by hand. This one goes to a real database.

func TestLoadStreamState_ReadsTheNewestSchemaSnapshotTime(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO stream_state (id, mode, server_id, last_checkpoint) VALUES (1, 'gtid', 7, UTC_TIMESTAMP())"); err != nil {
		t.Fatalf("seed stream_state: %v", err)
	}

	// With no snapshot at all the anchor stays invalid — "cannot tell", never a
	// zero time that would date every skip as "after the snapshot".
	st, err := status.LoadStreamState(ctx, db)
	if err != nil {
		t.Fatalf("LoadStreamState: %v", err)
	}
	if st.SchemaSnapshotAt.Valid {
		t.Errorf("an index with no schema snapshot must leave the anchor invalid, got %v", st.SchemaSnapshotAt.Time)
	}

	// Two snapshots: the anchor is the NEWEST, and deliberately not the highest
	// snapshot_id — the id is an auto-increment, the comparison is about time.
	older := time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	for i, ts := range []time.Time{newer, older} {
		if _, err := db.ExecContext(ctx, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable)
			VALUES (?, ?, 'shop', 'orders', 'id', 1, 'PRI', 'int', 'NO')`, i+1, ts); err != nil {
			t.Fatalf("seed schema_snapshots: %v", err)
		}
	}

	st, err = status.LoadStreamState(ctx, db)
	if err != nil {
		t.Fatalf("LoadStreamState: %v", err)
	}
	if !st.SchemaSnapshotAt.Valid {
		t.Fatal("the anchor must be loaded once a snapshot exists — without it the console cannot go quiet")
	}
	if !st.SchemaSnapshotAt.Time.Equal(newer) {
		t.Errorf("anchor = %v, want the newest snapshot %v (the later row carries the LOWER snapshot_id on purpose)",
			st.SchemaSnapshotAt.Time, newer)
	}
}

// An index old enough to have no schema_snapshots table at all must still load:
// the anchor only sharpens the capture verdict, so failing to read it may never
// cost the caller the stream state — including a gap-loss record — it already
// had. Same tolerance contract as the source_health and capture_skips loaders.
func TestLoadStreamState_MissingSnapshotTableIsTolerated(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO stream_state (id, mode, server_id, last_checkpoint) VALUES (1, 'gtid', 7, UTC_TIMESTAMP())"); err != nil {
		t.Fatalf("seed stream_state: %v", err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE schema_snapshots"); err != nil {
		t.Fatalf("drop schema_snapshots: %v", err)
	}

	st, err := status.LoadStreamState(ctx, db)
	if err != nil {
		t.Fatalf("LoadStreamState must tolerate a missing schema_snapshots table, got: %v", err)
	}
	if st == nil || st.ServerID != 7 {
		t.Fatalf("the stream state itself must survive the missing table: %+v", st)
	}
	if st.SchemaSnapshotAt.Valid {
		t.Error("no table means no anchor, not a zero-time anchor")
	}
}
