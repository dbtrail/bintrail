//go:build integration

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/bintrail/bintrail/internal/indexer"
	"github.com/bintrail/bintrail/internal/metadata"
	"github.com/bintrail/bintrail/internal/parser"
	"github.com/bintrail/bintrail/internal/testutil"
)

// ─── stream_state persistence ────────────────────────────────────────────────────────

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

// ─── stream_state: bintrail_id round-trip ────────────────────────────────────────────

func TestStreamState_bintrailID(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	const id = "aabbccdd-0000-0000-0000-000000000002"
	state := &streamState{mode: "position", binlogFile: "binlog.000001", binlogPos: 100, serverID: 1, bintrailID: id}
	if err := saveCheckpoint(db, state); err != nil {
		t.Fatalf("saveCheckpoint: %v", err)
	}
	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil state")
	}
	if loaded.bintrailID != id {
		t.Errorf("bintrailID: expected %q, got %q", id, loaded.bintrailID)
	}
}

func TestStreamState_emptyBintrailIDStoresNULL(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	state := &streamState{mode: "position", binlogFile: "binlog.000001", serverID: 1, bintrailID: ""}
	if err := saveCheckpoint(db, state); err != nil {
		t.Fatalf("saveCheckpoint: %v", err)
	}
	var got sql.NullString
	if err := db.QueryRow("SELECT bintrail_id FROM stream_state WHERE id = 1").Scan(&got); err != nil {
		t.Fatalf("query bintrail_id: %v", err)
	}
	if got.Valid {
		t.Errorf("expected bintrail_id to be NULL, got %q", got.String)
	}
}

// ─── streamLoop (in-memory, no live replication) ─────────────────────────────────────────

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

// ─── streamLoop live replication ───────────────────────────────────────────────────────

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

	sp := parser.NewStreamParser(resolver, filters, nil)
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

// ─── streamLoop — additional behaviour ────────────────────────────────────────────────

// TestStreamLoop_contextCancel verifies that cancelling the context causes
// streamLoop to flush the in-flight batch and write a checkpoint before returning.
func TestStreamLoop_contextCancel(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := indexer.New(db, 100) // large batch — events stay in batch until cancel

	events := make(chan parser.Event, 10)
	ts := time.Now().UTC()

	// Enqueue 2 events but do NOT close the channel — simulates an active stream.
	for i := range 2 {
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

	state := &streamState{mode: "position", serverID: 1}
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay — the 2 buffered events are consumed first,
	// then the batch sits idle until context cancellation triggers the flush.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if err := streamLoop(ctx, events, idx, db, time.Hour, state); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	if state.eventsIndexed != 2 {
		t.Errorf("expected 2 events indexed, got %d", state.eventsIndexed)
	}

	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved on context cancel")
	}
	if loaded.eventsIndexed != 2 {
		t.Errorf("checkpoint: expected 2 events, got %d", loaded.eventsIndexed)
	}
}

// TestStreamLoop_tickerCheckpoint verifies that the periodic ticker fires a
// checkpoint even when the events channel stays open and receives no further events.
func TestStreamLoop_tickerCheckpoint(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := indexer.New(db, 100)

	events := make(chan parser.Event, 10)
	ts := time.Now().UTC()

	// Enqueue 1 event but keep the channel open.
	events <- parser.Event{
		BinlogFile: "binlog.000001",
		StartPos:   0,
		EndPos:     100,
		Timestamp:  ts,
		Schema:     "testdb",
		Table:      "orders",
		EventType:  parser.EventInsert,
		PKValues:   "1",
		RowAfter:   map[string]any{"id": int64(1), "amount": 9.99},
	}

	state := &streamState{mode: "position", serverID: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		// 5 ms interval guarantees the ticker fires well before the 50 ms sleep.
		done <- streamLoop(ctx, events, idx, db, 5*time.Millisecond, state)
	}()

	// Wait for: (1) event consumed, (2) ticker to fire and save checkpoint.
	time.Sleep(50 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved by ticker")
	}
	if loaded.eventsIndexed != 1 {
		t.Errorf("expected 1 event in checkpoint, got %d", loaded.eventsIndexed)
	}
}

// TestStreamLoop_positionTracking verifies that state.binlogFile and
// state.binlogPos are updated from each event, reflecting the last event processed.
func TestStreamLoop_positionTracking(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := indexer.New(db, 100)

	events := make(chan parser.Event, 10)
	ts := time.Now().UTC()

	// Three events advancing across two binlog files.
	testEvents := []parser.Event{
		{
			BinlogFile: "binlog.000001", StartPos: 0, EndPos: 100, Timestamp: ts,
			Schema: "testdb", Table: "orders", EventType: parser.EventInsert,
			PKValues: "1", RowAfter: map[string]any{"id": int64(1)},
		},
		{
			BinlogFile: "binlog.000002", StartPos: 4, EndPos: 200, Timestamp: ts,
			Schema: "testdb", Table: "orders", EventType: parser.EventInsert,
			PKValues: "2", RowAfter: map[string]any{"id": int64(2)},
		},
		{
			BinlogFile: "binlog.000002", StartPos: 200, EndPos: 350, Timestamp: ts,
			Schema: "testdb", Table: "orders", EventType: parser.EventInsert,
			PKValues: "3", RowAfter: map[string]any{"id": int64(3)},
		},
	}
	for _, ev := range testEvents {
		events <- ev
	}
	close(events)

	state := &streamState{mode: "position", serverID: 1}

	if err := streamLoop(context.Background(), events, idx, db, time.Hour, state); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	// Final position should reflect the last event consumed.
	if state.binlogFile != "binlog.000002" {
		t.Errorf("binlogFile: expected binlog.000002, got %q", state.binlogFile)
	}
	if state.binlogPos != 350 {
		t.Errorf("binlogPos: expected 350, got %d", state.binlogPos)
	}
	if state.eventsIndexed != 3 {
		t.Errorf("eventsIndexed: expected 3, got %d", state.eventsIndexed)
	}
}

// TestStreamLoop_batchOverflow verifies that when the batch size is reached
// before the channel closes, events are flushed mid-stream and all rows are written.
func TestStreamLoop_batchOverflow(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	const batchSize = 3
	idx := indexer.New(db, batchSize)

	events := make(chan parser.Event, 20)
	ts := time.Now().UTC()

	// 7 events requires ceil(7/3) = 3 flushes (3 + 3 + 1 on close).
	const total = 7
	for i := range total {
		events <- parser.Event{
			BinlogFile: "binlog.000001",
			StartPos:   uint64(i * 100),
			EndPos:     uint64((i + 1) * 100),
			Timestamp:  ts,
			Schema:     "testdb",
			Table:      "orders",
			EventType:  parser.EventInsert,
			PKValues:   strconv.Itoa(i + 1),
			RowAfter:   map[string]any{"id": int64(i + 1)},
		}
	}
	close(events)

	state := &streamState{mode: "position", serverID: 1}

	if err := streamLoop(context.Background(), events, idx, db, time.Hour, state); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	if state.eventsIndexed != total {
		t.Errorf("eventsIndexed: expected %d, got %d", total, state.eventsIndexed)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&count); err != nil {
		t.Fatalf("count binlog_events: %v", err)
	}
	if count != total {
		t.Errorf("binlog_events rows: expected %d, got %d", total, count)
	}
}

// TestStreamLoop_gtidAccumulation verifies that event GTID values are accumulated
// into state.gtidSet and persisted in the checkpoint.
func TestStreamLoop_gtidAccumulation(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := indexer.New(db, 100)

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562" // go-mysql lowercases UUIDs

	// Seed the accumulated GTID set at uuid:1.
	gs, err := gomysql.ParseMysqlGTIDSet(uuid + ":1")
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet: %v", err)
	}
	acc := gs.(*gomysql.MysqlGTIDSet)

	state := &streamState{
		mode:     "gtid",
		serverID: 1,
		accGTID:  acc,
		gtidSet:  uuid + ":1",
	}

	ts := time.Now().UTC()
	events := make(chan parser.Event, 10)

	// Events carry consecutive GTIDs — the loop should accumulate them.
	for i, gno := range []int64{2, 3, 4} {
		events <- parser.Event{
			BinlogFile: "binlog.000001",
			StartPos:   uint64(i * 100),
			EndPos:     uint64((i + 1) * 100),
			Timestamp:  ts,
			GTID:       fmt.Sprintf("%s:%d", uuid, gno),
			Schema:     "testdb",
			Table:      "orders",
			EventType:  parser.EventInsert,
			PKValues:   strconv.Itoa(i + 1),
			RowAfter:   map[string]any{"id": int64(i + 1)},
		}
	}
	close(events)

	if err := streamLoop(context.Background(), events, idx, db, time.Hour, state); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	// state.gtidSet should now encode uuid:1-4 (1 from seed + 2,3,4 from events).
	if !strings.Contains(state.gtidSet, uuid) {
		t.Errorf("state.gtidSet: expected UUID, got %q", state.gtidSet)
	}
	if !strings.Contains(state.gtidSet, "1-4") {
		t.Errorf("state.gtidSet: expected range 1-4, got %q", state.gtidSet)
	}

	// Checkpoint should also persist the accumulated GTID.
	loaded, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved")
	}
	if !strings.Contains(loaded.gtidSet, "1-4") {
		t.Errorf("checkpoint gtidSet: expected 1-4 range, got %q", loaded.gtidSet)
	}
}
