//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestClearCheckpoint_Integration covers the index half of `bintrail-pg reset` against
// a real MySQL index: a present checkpoint clears (1 row), a second clear is a no-op
// (0 rows), and a missing stream_state table reports tableMissing rather than erroring.
func TestClearCheckpoint_Integration(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}

	if _, err := db.ExecContext(ctx,
		"INSERT INTO stream_state (id, mode, server_id, last_checkpoint) VALUES (1, 'gtid', 9, UTC_TIMESTAMP())"); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	rows, missing, err := clearCheckpoint(ctx, db)
	if err != nil || missing || rows != 1 {
		t.Fatalf("clear present checkpoint = (rows=%d, missing=%t, err=%v), want (1, false, nil)", rows, missing, err)
	}

	rows, missing, err = clearCheckpoint(ctx, db)
	if err != nil || missing || rows != 0 {
		t.Fatalf("second clear = (rows=%d, missing=%t, err=%v), want (0, false, nil)", rows, missing, err)
	}

	if _, err := db.ExecContext(ctx, "DROP TABLE stream_state"); err != nil {
		t.Fatalf("drop stream_state: %v", err)
	}
	rows, missing, err = clearCheckpoint(ctx, db)
	if err != nil || !missing {
		t.Fatalf("clear with no table = (rows=%d, missing=%t, err=%v), want (_, true, nil)", rows, missing, err)
	}
}
