//go:build integration

package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func resetRestoreIndexGlobals(t *testing.T) {
	t.Helper()
	sDSN, sDir, sS3, sRegion := riIndexDSN, riArchiveDir, riArchiveS3, riRegion
	sBatch, sParts, sFmt := riBatch, riPartitions, riFormat
	t.Cleanup(func() {
		riIndexDSN, riArchiveDir, riArchiveS3, riRegion = sDSN, sDir, sS3, sRegion
		riBatch, riPartitions, riFormat = sBatch, sParts, sFmt
	})
	riIndexDSN, riArchiveDir, riArchiveS3, riRegion = "", "", "", ""
	riBatch, riPartitions, riFormat = 5000, 48, "json"
}

// TestIntegrationRestoreIndex_roundTrip archives a real index's partitions
// (plus the durable-state sidecar), rebuilds a FRESH index from them, and
// checks the inventory: events, archive_state, snapshots — and that a second
// run is refused.
func TestIntegrationRestoreIndex_roundTrip(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	// ── Source index with events across two partitions ────────────────────
	srcDB, srcName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, srcDB)
	if err := indexer.EnsureSchema(srcDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	// The sidecar dumps bintrail_servers too; the test helper set does not
	// create it, so use the real DDL.
	testutil.MustExec(t, srcDB, serverid.DDLBintrailServers)
	testutil.MustExec(t, srcDB, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'shop', 'orders', 'id', 1, 'PRI', 'int', 'NO', 0)`)

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	testutil.SetupPartitionedTable(t, srcDB, srcName, []time.Time{h1, h2})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, srcDB, "binlog.000001", 100, 200, ts1, nil,
		"shop", "orders", 1, "1", nil, nil, []byte(`{"id":1,"status":"a"}`))
	testutil.InsertEvent(t, srcDB, "binlog.000001", 200, 300, ts1, nil,
		"shop", "orders", 2, "1", nil, []byte(`{"id":1,"status":"a"}`), []byte(`{"id":1,"status":"b"}`))
	testutil.InsertEvent(t, srcDB, "binlog.000001", 300, 400, ts2, nil,
		"shop", "orders", 1, "2", nil, nil, []byte(`{"id":2,"status":"c"}`))

	// ── Archive both partitions + the sidecar (the rotation layout) ───────
	archiveDir := t.TempDir()
	const id = "restore-roundtrip-id"
	for _, h := range []time.Time{h1, h2} {
		name := indexer.PartitionName(h)
		outPath, err := rotation.HiveArchivePath(archiveDir, id, name)
		if err != nil {
			t.Fatalf("HiveArchivePath: %v", err)
		}
		if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.ArchivePartition(ctx, srcDB, srcName, name, outPath, "zstd"); err != nil {
			t.Fatalf("ArchivePartition %s: %v", name, err)
		}
	}
	if err := archive.WriteMetaSidecar(ctx, srcDB, filepath.Join(archiveDir, "bintrail_id="+id)); err != nil {
		t.Fatalf("WriteMetaSidecar: %v", err)
	}

	// ── Rebuild into a FRESH index ────────────────────────────────────────
	dstDB, dstName := testutil.CreateTestDB(t)
	resetRestoreIndexGlobals(t)
	riIndexDSN, riArchiveDir, riPartitions = testutil.IntegrationDSN(dstName), archiveDir, 4
	if err := runRestoreIndex(newQueryTestCmd(), nil); err != nil {
		t.Fatalf("runRestoreIndex: %v", err)
	}

	var events int64
	if err := dstDB.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 3 {
		t.Fatalf("restored events = %d, want 3", events)
	}
	// event_id identity survives the round trip (MergeResults dedups by it).
	var maxSrc, maxDst int64
	if err := srcDB.QueryRow("SELECT MAX(event_id) FROM binlog_events").Scan(&maxSrc); err != nil {
		t.Fatal(err)
	}
	if err := dstDB.QueryRow("SELECT MAX(event_id) FROM binlog_events").Scan(&maxDst); err != nil {
		t.Fatal(err)
	}
	if maxSrc != maxDst {
		t.Fatalf("event_id identity lost: src max %d, dst max %d", maxSrc, maxDst)
	}
	var stateRows int
	if err := dstDB.QueryRow("SELECT COUNT(*) FROM archive_state").Scan(&stateRows); err != nil {
		t.Fatal(err)
	}
	if stateRows != 2 {
		t.Fatalf("archive_state rows = %d, want 2", stateRows)
	}
	var snaps int
	if err := dstDB.QueryRow("SELECT COUNT(*) FROM schema_snapshots").Scan(&snaps); err != nil {
		t.Fatal(err)
	}
	if snaps != 1 {
		t.Fatalf("restored snapshots = %d, want 1 (sidecar)", snaps)
	}

	// A second run must be refused: the index now holds events.
	err := runRestoreIndex(newQueryTestCmd(), nil)
	if err == nil || !strings.Contains(err.Error(), "already holds events") {
		t.Fatalf("second run must be refused: %v", err)
	}
}
