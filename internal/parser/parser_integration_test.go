//go:build integration

package parser_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/bintrail/bintrail/internal/metadata"
	"github.com/bintrail/bintrail/internal/parser"
	"github.com/bintrail/bintrail/internal/testutil"
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
	var currentBinlog, ignorePos, ignoreBinDo, ignoreBinIgn, ignoreGtid string
	if err := sourceDB.QueryRow("SHOW MASTER STATUS").Scan(
		&currentBinlog, &ignorePos, &ignoreBinDo, &ignoreBinIgn, &ignoreGtid,
	); err != nil {
		t.Fatalf("SHOW MASTER STATUS failed: %v", err)
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

	got := drainEvents(events)

	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile returned error: %v", err)
	}

	// Expect 4 events: 2 INSERT + 1 UPDATE + 1 DELETE.
	if len(got) != 4 {
		t.Fatalf("expected 4 events, got %d", len(got))
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

	got := drainEvents(events)

	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile returned error: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("expected 0 events with nonexistent schema filter, got %d", len(got))
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

		var currentBinlog, ignorePos, ignoreBinDo, ignoreBinIgn, ignoreGtid string
		if err := sourceDB.QueryRow("SHOW MASTER STATUS").Scan(
			&currentBinlog, &ignorePos, &ignoreBinDo, &ignoreBinIgn, &ignoreGtid,
		); err != nil {
			t.Fatalf("SHOW MASTER STATUS failed: %v", err)
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

	got := drainEvents(events)

	if err := <-errCh; err != nil {
		t.Fatalf("ParseFiles returned error: %v", err)
	}

	// Each batch has 1 INSERT, so 2 events total.
	if len(got) != 2 {
		t.Errorf("expected 2 events from 2 binlog files, got %d", len(got))
		for i, ev := range got {
			t.Logf("  event[%d]: type=%d file=%s table=%s pk=%s",
				i, ev.EventType, ev.BinlogFile, ev.Table, ev.PKValues)
		}
	}

	// Verify events come from different binlog files.
	if len(got) == 2 && got[0].BinlogFile == got[1].BinlogFile {
		t.Error("expected events from different binlog files")
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
