//go:build integration

package indexer

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

func TestCreateBinlogEventsTable(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	if err := createBinlogEventsTable(db, 3, false); err != nil {
		t.Fatalf("createBinlogEventsTable failed: %v", err)
	}

	// Verify the table has 4 partitions (3 hourly + p_future).
	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'`,
		dbName).Scan(&count); err != nil {
		t.Fatalf("query partitions failed: %v", err)
	}
	if count != 4 {
		t.Errorf("expected 4 partitions, got %d", count)
	}
}
