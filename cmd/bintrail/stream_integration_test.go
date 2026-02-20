//go:build integration

package main

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/bintrail/bintrail/internal/indexer"
	"github.com/bintrail/bintrail/internal/metadata"
	"github.com/bintrail/bintrail/internal/parser"
	"github.com/bintrail/bintrail/internal/testutil"
)

// ─── stream_state persistence ────────────────────────────────────────────────

func TestStreamState_loadEmpty(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	state, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState failed: %v", err)
	}
	if state != nil {
		t.Errorf("expected nil for empty stream_state, got %+v", state)
	}
}

func TestStreamState_upsertAndLoad(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	state := &streamState{
		mode:          "position",
		binlogFile:    "binlog.000001",
		binlogPos:     1024,
		eventsIndexed: 50,
		serverID:      99999,
		lastEventTime: sql.NullTime{
			Time:  time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
			Valid: true,
		},
	}

	if err := saveCheckpoint(db, state); err != nil {
		t.Fatalf("saveCheckpoint failed: %v", err)
	}

	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil state after save")
	}

	if loaded.mode != "position" {
		t.Errorf("mode: expected position, got %q", loaded.mode)
	}
	if loaded.binlogFile != "binlog.000001" {
		t.Errorf("binlogFile: expected binlog.000001, got %q", loaded.binlogFile)
	}
	if loaded.binlogPos != 1024 {
		t.Errorf("binlogPos: expected 1024, got %d", loaded.binlogPos)
	}
	if loaded.eventsIndexed != 50 {
		t.Errorf("eventsIndexed: expected 50, got %d", loaded.eventsIndexed)
	}
	if loaded.serverID != 99999 {
		t.Errorf("serverID: expected 99999, got %d", loaded.serverID)
	}
}

func TestStreamState_upsertUpdate(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// First checkpoint.
	s1 := &streamState{
		mode: "position", binlogFile: "binlog.000001", binlogPos: 100,
		eventsIndexed: 10, serverID: 1,
	}
	if err := saveCheckpoint(db, s1); err != nil {
		t.Fatalf("first saveCheckpoint: %v", err)
	}

	// Second checkpoint — advances position.
	s2 := &streamState{
		mode: "position", binlogFile: "binlog.000002", binlogPos: 500,
		eventsIndexed: 250, serverID: 1,
	}
	if err := saveCheckpoint(db, s2); err != nil {
		t.Fatalf("second saveCheckpoint: %v", err)
	}

	// Verify only one row exists and it reflects the latest state.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM stream_state").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row in stream_state, got %d", count)
	}

	loaded, _ := loadStreamState(db)
	if loaded.binlogFile != "binlog.000002" {
		t.Errorf("expected binlog.000002, got %q", loaded.binlogFile)
	}
	if loaded.eventsIndexed != 250 {
		t.Errorf("expected 250 events, got %d", loaded.eventsIndexed)
	}
}

func TestStreamState_gtidMode(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	gtidSet := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-42"
	state := &streamState{
		mode:          "gtid",
		gtidSet:       gtidSet,
		eventsIndexed: 42,
		serverID:      12345,
	}

	if err := saveCheckpoint(db, state); err != nil {
		t.Fatalf("saveCheckpoint: %v", err)
	}

	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded.mode != "gtid" {
		t.Errorf("mode: expected gtid, got %q", loaded.mode)
	}
	if loaded.gtidSet != gtidSet {
		t.Errorf("gtidSet: expected %q, got %q", gtidSet, loaded.gtidSet)
	}
}

// ─── streamLoop (in-memory, no live replication) ─────────────────────────────

// TestStreamLoop_flushAndCheckpoint verifies that streamLoop correctly batches
// events, flushes them, and saves a checkpoint — using a live index database
// but no replication connection.
func TestStreamLoop_flushAndCheckpoint(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Insert a minimal schema snapshot so the indexer can run.
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00",
		"testdb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00",
		"testdb", "orders", "amount", 2, "", "decimal", "YES")

	idx := indexer.New(db, 10)

	events := make(chan parser.Event, 20)
	ts := time.Now().UTC()

	// Send 3 synthetic events (fewer than the batch size of 10).
	for i := range 3 {
		events <- parser.Event{
			BinlogFile: "binlog.000001",
			StartPos:   uint64(i * 100),
			EndPos:     uint64((i + 1) * 100),
			Timestamp:  ts,
			Schema:     "testdb",
			Table:      "orders",
			EventType:  parser.EventInsert,
			PKValues:   strconv.Itoa(i + 1),
			RowAfter:   map[string]any{"id": int64(i + 1), "amount": 9.99},
		}
	}
	close(events)

	state := &streamState{
		mode:     "position",
		serverID: 1,
	}

	if err := streamLoop(context.Background(), events, idx, db, time.Hour, state); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	if state.eventsIndexed != 3 {
		t.Errorf("expected 3 events indexed, got %d", state.eventsIndexed)
	}

	// Verify checkpoint was written to the DB.
	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved")
	}
	if loaded.eventsIndexed != 3 {
		t.Errorf("checkpoint: expected 3 events, got %d", loaded.eventsIndexed)
	}

	// Verify rows are in binlog_events.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&count); err != nil {
		t.Fatalf("count binlog_events: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows in binlog_events, got %d", count)
	}
}

// ─── streamLoop live replication ─────────────────────────────────────────────

// TestStreamLoop_liveReplication is a full end-to-end test that connects as a
// replica to the Docker MySQL, streams events, and verifies they are indexed.
// It is skipped gracefully if binary logging or replication privileges are unavailable.
func TestStreamLoop_liveReplication(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestDB(t)

	// Create a table on the source.
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		amount DECIMAL(10,2) NOT NULL
	)`)

	// Skip if binary logging is not enabled.
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}

	// Capture current binlog position before any inserts.
	var binlogFile string
	var binlogPos uint32
	var dummy sql.NullString
	if err := sourceDB.QueryRow("SHOW MASTER STATUS").Scan(
		&binlogFile, &binlogPos, &dummy, &dummy, &dummy,
	); err != nil {
		t.Skipf("skipping: SHOW MASTER STATUS failed: %v", err)
	}

	// Take schema snapshot into the index DB.
	if _, err := ensureResolver(indexDB, sourceDB, []string{sourceName}); err != nil {
		t.Fatalf("ensureResolver: %v", err)
	}

	// Parse source DSN to get connection details for the syncer.
	host, port, user, password, err := parseSourceDSN(testutil.IntegrationDSN(sourceName))
	if err != nil {
		t.Fatalf("parseSourceDSN: %v", err)
	}

	// Insert rows so the streamer has something to receive.
	for i := range 5 {
		testutil.MustExec(t, sourceDB,
			"INSERT INTO orders (amount) VALUES (?)", float64(i+1)*10.0)
	}

	// Start BinlogSyncer from the captured position.
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 99998,
		Flavor:   "mysql",
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
	})
	defer syncer.Close()

	streamer, syncErr := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if syncErr != nil {
		t.Skipf("skipping: StartSync failed (replication may not be granted): %v", syncErr)
	}

	// Build resolver and filters for the source schema.
	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	filters := buildIndexFilters(sourceName, "")

	sp := parser.NewStreamParser(resolver, filters)
	idx := indexer.New(indexDB, 100)

	// Run with a short deadline — just long enough to capture the 5 inserts.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	state := &streamState{mode: "position", serverID: 99998}
	if err := streamLoop(ctx, events, idx, indexDB, time.Minute, state); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	parseErr := <-parseErrCh
	if parseErr != nil &&
		!errors.Is(parseErr, context.DeadlineExceeded) &&
		!errors.Is(parseErr, context.Canceled) {
		t.Fatalf("StreamParser error: %v", parseErr)
	}

	if state.eventsIndexed < 5 {
		t.Errorf("expected at least 5 events indexed, got %d", state.eventsIndexed)
	}

	// Verify the checkpoint reflects the streamed position.
	loaded, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved after streaming")
	}
}
