//go:build integration

package parser_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

// setupBinlog creates a test table, performs DML, flushes binlogs, and copies
// the sealed binlog file to a temp directory. Returns the temp directory and
// the binlog filename.
func setupBinlog(t *testing.T) (binlogDir, binlogFile, schemaName string, resolver *metadata.Resolver) {
	t.Helper()

	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)

	// Create the schema_snapshots table in the index DB.
	testutil.InitIndexTables(t, indexDB)

	// Create a test table.
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id       INT PRIMARY KEY AUTO_INCREMENT,
		customer VARCHAR(100) NOT NULL,
		status   VARCHAR(20)  NOT NULL DEFAULT 'new',
		amount   DECIMAL(10,2) NOT NULL
	)`)

	// Take a snapshot so we can build a resolver.
	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	res, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	// Flush to get a clean binlog boundary.
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	// Note the current binlog file.
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition failed: %v", err)
	}

	// Perform DML: 2 INSERTs, 1 UPDATE, 1 DELETE = 4 events.
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, status, amount) VALUES ('Alice', 'new', 99.99)")
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, status, amount) VALUES ('Bob', 'pending', 50.00)")
	testutil.MustExec(t, sourceDB, "UPDATE orders SET status = 'shipped', amount = 109.99 WHERE customer = 'Alice'")
	testutil.MustExec(t, sourceDB, "DELETE FROM orders WHERE customer = 'Bob'")

	// Seal the binlog file.
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	// Copy the binlog file from Docker.
	tmpDir := t.TempDir()
	cpCmd := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog),
	)
	if out, err := cpCmd.CombinedOutput(); err != nil {
		t.Fatalf("docker cp %s failed: %v\n%s", currentBinlog, err, out)
	}

	return tmpDir, currentBinlog, sourceName, res
}

// drainEvents reads all events from the channel and returns them.
func drainEvents(ch <-chan parser.Event) []parser.Event {
	var events []parser.Event
	for ev := range ch {
		events = append(events, ev)
	}
	return events
}

// dmlEvents returns only the INSERT/UPDATE/DELETE events. Count assertions in
// these tests must ignore DDL: under parallel `go test -tags integration ./...`
// runs, concurrent test packages' setup DDL (CREATE TABLE index_state, ...)
// lands in the shared container's binlog window, and DDL events bypass the
// schema filter by design — DDL is emitted unconditionally for audit and
// auto-snapshot purposes (#415; contract pinned by
// TestStreamParser_ddlBypassesSchemaFilter). Row events ARE schema-filtered
// and each test owns its schema, so DML counts stay exact.
func dmlEvents(events []parser.Event) []parser.Event {
	var dml []parser.Event
	for _, ev := range events {
		switch ev.EventType {
		case parser.EventInsert, parser.EventUpdate, parser.EventDelete:
			dml = append(dml, ev)
		}
	}
	return dml
}

// ─── Tests ──────────────────────────────────────────────────────────────────

func TestParseFile_realBinlog(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	binlogDir, binlogFile, schemaName, resolver := setupBinlog(t)

	p := parser.New(binlogDir, resolver, parser.Filters{
		Schemas: map[string]bool{schemaName: true},
	}, nil)

	events := make(chan parser.Event, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(events)
		errCh <- p.ParseFile(context.Background(), binlogFile, events)
	}()

	got := dmlEvents(drainEvents(events))

	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile returned error: %v", err)
	}

	// Expect 4 DML events: 2 INSERT + 1 UPDATE + 1 DELETE.
	if len(got) != 4 {
		t.Fatalf("expected 4 DML events, got %d", len(got))
	}

	// Count by type.
	typeCounts := map[parser.EventType]int{}
	for _, ev := range got {
		typeCounts[ev.EventType]++
	}
	if typeCounts[parser.EventInsert] != 2 {
		t.Errorf("expected 2 INSERT events, got %d", typeCounts[parser.EventInsert])
	}
	if typeCounts[parser.EventUpdate] != 1 {
		t.Errorf("expected 1 UPDATE event, got %d", typeCounts[parser.EventUpdate])
	}
	if typeCounts[parser.EventDelete] != 1 {
		t.Errorf("expected 1 DELETE event, got %d", typeCounts[parser.EventDelete])
	}

	// Verify event fields.
	for i, ev := range got {
		if ev.Schema != schemaName {
			t.Errorf("event[%d]: expected schema %q, got %q", i, schemaName, ev.Schema)
		}
		if ev.Table != "orders" {
			t.Errorf("event[%d]: expected table 'orders', got %q", i, ev.Table)
		}
		if ev.BinlogFile != binlogFile {
			t.Errorf("event[%d]: expected binlog file %q, got %q", i, binlogFile, ev.BinlogFile)
		}
		if ev.PKValues == "" {
			t.Errorf("event[%d]: expected non-empty PKValues", i)
		}
		// The test MySQL runs with gtid_mode=OFF (no --gtid-mode flag in CI's
		// docker run, and OFF is the stock default) — every transaction is
		// still wrapped in an ANONYMOUS_GTID_LOG_EVENT, which must format to
		// an empty GTID, not the fake "00000000-...-000000000000:0" #678
		// produced before the fix.
		if ev.GTID != "" {
			t.Errorf("event[%d]: expected empty GTID (gtid_mode=OFF source), got %q", i, ev.GTID)
		}

		switch ev.EventType {
		case parser.EventInsert:
			if ev.RowAfter == nil {
				t.Errorf("event[%d] INSERT: expected non-nil RowAfter", i)
			}
			if ev.RowBefore != nil {
				t.Errorf("event[%d] INSERT: expected nil RowBefore", i)
			}
		case parser.EventDelete:
			if ev.RowBefore == nil {
				t.Errorf("event[%d] DELETE: expected non-nil RowBefore", i)
			}
			if ev.RowAfter != nil {
				t.Errorf("event[%d] DELETE: expected nil RowAfter", i)
			}
		case parser.EventUpdate:
			if ev.RowBefore == nil {
				t.Errorf("event[%d] UPDATE: expected non-nil RowBefore", i)
			}
			if ev.RowAfter == nil {
				t.Errorf("event[%d] UPDATE: expected non-nil RowAfter", i)
			}
		}
	}
}

func TestParseFile_withFilters(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	binlogDir, binlogFile, _, resolver := setupBinlog(t)

	// Use a filter for a nonexistent schema — should emit 0 events.
	p := parser.New(binlogDir, resolver, parser.Filters{
		Schemas: map[string]bool{"nonexistent_schema": true},
	}, nil)

	events := make(chan parser.Event, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(events)
		errCh <- p.ParseFile(context.Background(), binlogFile, events)
	}()

	got := dmlEvents(drainEvents(events))

	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile returned error: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("expected 0 DML events with nonexistent schema filter, got %d", len(got))
	}
}

func TestParseFiles_multiple(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)

	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE items (
		id   INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	tmpDir := t.TempDir()
	var binlogFiles []string

	// Generate 2 binlog files with DML in each.
	for batch := range 2 {
		testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

		currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
		if err != nil {
			t.Fatalf("CurrentBinlogPosition failed: %v", err)
		}

		testutil.MustExec(t, sourceDB,
			fmt.Sprintf("INSERT INTO items (name) VALUES ('item_%d')", batch))

		testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

		cpCmd := exec.Command("docker", "cp",
			fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
			filepath.Join(tmpDir, currentBinlog),
		)
		if out, err := cpCmd.CombinedOutput(); err != nil {
			t.Fatalf("docker cp %s failed: %v\n%s", currentBinlog, err, out)
		}

		binlogFiles = append(binlogFiles, currentBinlog)
	}

	p := parser.New(tmpDir, resolver, parser.Filters{
		Schemas: map[string]bool{sourceName: true},
	}, nil)

	events := make(chan parser.Event, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(events)
		errCh <- p.ParseFiles(context.Background(), binlogFiles, events)
	}()

	raw := drainEvents(events)
	got := dmlEvents(raw)

	if err := <-errCh; err != nil {
		t.Fatalf("ParseFiles returned error: %v", err)
	}

	// Each batch has 1 INSERT, so 2 DML events total.
	if len(got) != 2 {
		t.Errorf("expected 2 DML events from 2 binlog files, got %d", len(got))
		// Dump the RAW set so the failure shows what dmlEvents filtered out.
		for i, ev := range raw {
			t.Logf("  event[%d]: type=%d file=%s schema=%s table=%s pk=%s",
				i, ev.EventType, ev.BinlogFile, ev.Schema, ev.Table, ev.PKValues)
		}
	}

	// Verify events come from different binlog files.
	if len(got) == 2 && got[0].BinlogFile == got[1].BinlogFile {
		t.Error("expected events from different binlog files")
	}
}

// TestParseFile_compressedTransactions is the end-to-end regression guard for
// binlog_transaction_compression=ON: transactions arrive wrapped in
// zstd-compressed Transaction_payload events, and the parser must dispatch the
// inner row events through the normal pipeline. Before the fix, compressed
// transactions were silently dropped (zero events, no error).
func TestParseFile_compressedTransactions(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id       INT PRIMARY KEY AUTO_INCREMENT,
		customer VARCHAR(100) NOT NULL,
		notes    TEXT
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	// Session-scoped compression on a dedicated connection: every transaction
	// on THIS conn gets the Transaction_payload wrapper without affecting the
	// rest of the suite's binlog traffic.
	ctx := context.Background()
	conn, err := sourceDB.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn failed: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET SESSION binlog_transaction_compression = ON"); err != nil {
		// Skip ONLY on ER_UNKNOWN_SYSTEM_VARIABLE (pre-8.0.20 server). Any
		// other failure (connection drop, permissions) must FAIL, not skip —
		// a broad skip would silently turn this regression guard green.
		var myErr *drivermysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1193 {
			t.Skipf("binlog_transaction_compression not supported on this server (needs MySQL 8.0.20+): %v", err)
		}
		t.Fatalf("SET SESSION binlog_transaction_compression failed for a non-version reason: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition failed: %v", err)
	}

	// Highly repetitive ~1KB filler makes every transaction reliably
	// compressible, so MySQL picks ZSTD. (Incompressible transactions get a
	// NONE-type payload wrapper that go-mysql v1.13.0 refuses to decode —
	// a loud error, tracked separately, but it would fail this test for the
	// wrong reason.)
	filler := strings.Repeat("bintrail compresses fine ", 40)

	// One multi-row INSERT transaction + one UPDATE + one DELETE, all on the
	// compressing connection. The UPDATE/DELETE rows carry the filler in
	// their before-images (binlog_row_image=FULL), keeping them compressible.
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	for i := range 20 {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO orders (customer, notes) VALUES (?, ?)",
			fmt.Sprintf("customer_%d", i), filler); err != nil {
			t.Fatalf("INSERT %d failed: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "UPDATE orders SET customer = 'updated_1' WHERE id = 1"); err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "DELETE FROM orders WHERE id = 2"); err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cpCmd := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog),
	)
	if out, err := cpCmd.CombinedOutput(); err != nil {
		t.Fatalf("docker cp %s failed: %v\n%s", currentBinlog, err, out)
	}

	// Vacuity guard: the file must actually contain Transaction_payload
	// events, or this test would pass without exercising the new code path.
	sawPayload := false
	rawParser := replication.NewBinlogParser()
	if err := rawParser.ParseFile(filepath.Join(tmpDir, currentBinlog), 0, func(e *replication.BinlogEvent) error {
		if e.Header.EventType == replication.TRANSACTION_PAYLOAD_EVENT {
			sawPayload = true
		}
		return nil
	}); err != nil {
		t.Fatalf("raw payload scan failed: %v", err)
	}
	if !sawPayload {
		t.Fatal("binlog contains no Transaction_payload events — compression did not engage, test is vacuous")
	}
	fileInfo, err := os.Stat(filepath.Join(tmpDir, currentBinlog))
	if err != nil {
		t.Fatalf("stat binlog: %v", err)
	}

	p := parser.New(tmpDir, resolver, parser.Filters{
		Schemas: map[string]bool{sourceName: true},
	}, nil)

	events := make(chan parser.Event, 100)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		errCh <- p.ParseFile(context.Background(), currentBinlog, events)
	}()
	got := drainEvents(events)
	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile returned error: %v", err)
	}

	// 20 INSERT + 1 UPDATE + 1 DELETE — the positive count IS the regression
	// guard (the bug produced exactly zero). DML-only via dmlEvents (#415).
	dml := dmlEvents(got)
	typeCounts := map[parser.EventType]int{}
	for _, ev := range dml {
		typeCounts[ev.EventType]++
	}
	if typeCounts[parser.EventInsert] != 20 || typeCounts[parser.EventUpdate] != 1 || typeCounts[parser.EventDelete] != 1 {
		t.Fatalf("event mix = %d INSERT / %d UPDATE / %d DELETE, want 20/1/1",
			typeCounts[parser.EventInsert], typeCounts[parser.EventUpdate], typeCounts[parser.EventDelete])
	}

	for i, ev := range dml {
		// Positions must be the payload event's FILE coordinates: real MySQL
		// zeroes the inner events' end_log_pos, so an unrewritten start_pos
		// (0 - EventSize) would underflow to ~2^64.
		if ev.StartPos >= ev.EndPos {
			t.Errorf("event[%d]: StartPos %d >= EndPos %d (underflow or bad rewrite)", i, ev.StartPos, ev.EndPos)
		}
		if ev.EndPos > uint64(fileInfo.Size()) {
			t.Errorf("event[%d]: EndPos %d exceeds binlog file size %d", i, ev.EndPos, fileInfo.Size())
		}
		// Inner-event timestamps are real commit times, not zero.
		if ev.Timestamp.Before(time.Now().Add(-time.Hour)) {
			t.Errorf("event[%d]: Timestamp %v looks wrong (zero inner header?)", i, ev.Timestamp)
		}
		// The BEGIN inside the payload carries the connection id.
		if ev.ConnectionID == 0 {
			t.Errorf("event[%d]: ConnectionID = 0, want pseudo_thread_id from inner BEGIN", i)
		}
	}
}

func TestParseFile_contextCancellation(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	binlogDir, binlogFile, _, resolver := setupBinlog(t)

	p := parser.New(binlogDir, resolver, parser.Filters{}, nil)

	// Cancel immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	events := make(chan parser.Event, 100)
	err := p.ParseFile(ctx, binlogFile, events)

	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
	if !os.IsTimeout(err) && err != context.Canceled {
		// go-mysql may wrap the error — just verify it's a cancellation.
		if ctx.Err() != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	}
}
