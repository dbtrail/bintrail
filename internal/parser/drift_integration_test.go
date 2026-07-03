//go:build integration

package parser_test

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestParseFile_schemaDriftFailsLoud is the #700 end-to-end guard against a
// REAL binlog: take a snapshot, RENAME a column (same column count — invisible
// to the count guard), write rows, and parse with the now-stale resolver.
// With binlog_row_metadata=FULL the TABLE_MAP carries the new name and
// ParseFile must fail loud instead of silently indexing values under the old
// column name.
func TestParseFile_schemaDriftFailsLoud(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// GLOBAL-only variable: read original, set FULL, restore in Cleanup.
	var original string
	if err := sourceDB.QueryRow("SELECT @@binlog_row_metadata").Scan(&original); err != nil {
		var myErr *drivermysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1193 {
			t.Skipf("binlog_row_metadata not supported on this server: %v", err)
		}
		t.Fatalf("read binlog_row_metadata failed: %v", err)
	}
	testutil.MustExec(t, sourceDB, "SET GLOBAL binlog_row_metadata = 'FULL'")
	t.Cleanup(func() {
		if _, err := sourceDB.Exec("SET GLOBAL binlog_row_metadata = ?", original); err != nil {
			t.Errorf("restore binlog_row_metadata=%s failed (later packages' binlogs are affected): %v", original, err)
		}
	})

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id       INT PRIMARY KEY AUTO_INCREMENT,
		customer VARCHAR(100) NOT NULL,
		amount   DECIMAL(10,2) NOT NULL
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	staleResolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition failed: %v", err)
	}

	// The stale-vs-historical distinction compares the binlog event
	// timestamp (source clock, 1s granularity) against the snapshot's
	// creation time (bintrail host clock): sleep past both the granularity
	// and modest container/host clock skew so the post-rename events are
	// unambiguously NEWER than the snapshot (the hard-error side).
	time.Sleep(2 * time.Second)

	// Positive-path traffic FIRST: with names matching the snapshot, rows
	// must parse cleanly under FULL metadata.
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, amount) VALUES ('Alice', 99.99)")

	// The same-count drift: rename a column AFTER the snapshot. The count
	// guard cannot see this; only the FULL-metadata name check can.
	testutil.MustExec(t, sourceDB, "ALTER TABLE orders RENAME COLUMN customer TO client")
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (client, amount) VALUES ('Bob', 50.00)")

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cpCmd := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog),
	)
	if out, err := cpCmd.CombinedOutput(); err != nil {
		t.Fatalf("docker cp %s failed: %v\n%s", currentBinlog, err, out)
	}

	p := parser.New(tmpDir, staleResolver, parser.Filters{
		Schemas: map[string]bool{sourceName: true},
	}, nil)

	events := make(chan parser.Event, 100)
	done := make(chan error, 1)
	go func() {
		defer close(events)
		done <- p.ParseFile(context.Background(), currentBinlog, events)
	}()
	all := drainEvents(events)
	parseErr := <-done

	// The pre-drift INSERT must have emitted normally.
	dml := dmlEvents(all)
	if len(dml) != 1 {
		t.Errorf("expected exactly 1 pre-drift DML event, got %d", len(dml))
	} else if dml[0].RowAfter["customer"] == nil {
		t.Errorf("pre-drift event must map the snapshot's column name, got %v", dml[0].RowAfter)
	}

	// The post-rename rows must abort the parse with the drift error —
	// naming both sides and the remediation.
	if parseErr == nil {
		t.Fatal("expected ParseFile to fail loud on the renamed column, got nil (silent corruption)")
	}
	for _, want := range []string{"schema drift", "client", "customer", "bintrail snapshot"} {
		if !strings.Contains(parseErr.Error(), want) {
			t.Errorf("drift error missing %q: %v", want, parseErr)
		}
	}
}

// TestParseFile_historicalDriftWarnsAndIndexes pins the review-forced
// distinction end-to-end: events written BEFORE the snapshot whose TABLE_MAP
// names differ (the snapshot post-dates a rename) are a routine historical
// state — re-indexing old files must WARN and index them under the
// snapshot's current names, never dead-end on a hard error whose remediation
// (re-snapshot) would be a no-op.
func TestParseFile_historicalDriftWarnsAndIndexes(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	var original string
	if err := sourceDB.QueryRow("SELECT @@binlog_row_metadata").Scan(&original); err != nil {
		var myErr *drivermysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1193 {
			t.Skipf("binlog_row_metadata not supported on this server: %v", err)
		}
		t.Fatalf("read binlog_row_metadata failed: %v", err)
	}
	testutil.MustExec(t, sourceDB, "SET GLOBAL binlog_row_metadata = 'FULL'")
	t.Cleanup(func() {
		if _, err := sourceDB.Exec("SET GLOBAL binlog_row_metadata = ?", original); err != nil {
			t.Errorf("restore binlog_row_metadata=%s failed: %v", original, err)
		}
	})

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id       INT PRIMARY KEY,
		customer VARCHAR(100) NOT NULL
	)`)

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition failed: %v", err)
	}

	// History: a row written under the OLD column name...
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (id, customer) VALUES (1, 'Alice')")
	// ...then the rename, then the snapshot — taken AFTER both, so the
	// insert above is a pre-snapshot event whose TABLE_MAP carries a name
	// the snapshot no longer has. Sleep past the 1s binlog-timestamp
	// granularity + modest clock skew so the event is unambiguously OLDER.
	testutil.MustExec(t, sourceDB, "ALTER TABLE orders RENAME COLUMN customer TO client")
	time.Sleep(2 * time.Second)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}
	if resolver.SnapshotTime().IsZero() {
		t.Fatal("NewResolver must load the snapshot creation time (the drift guard depends on it)")
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

	p := parser.New(tmpDir, resolver, parser.Filters{
		Schemas: map[string]bool{sourceName: true},
	}, nil)

	events := make(chan parser.Event, 100)
	done := make(chan error, 1)
	go func() {
		defer close(events)
		done <- p.ParseFile(context.Background(), currentBinlog, events)
	}()
	all := drainEvents(events)
	if err := <-done; err != nil {
		t.Fatalf("re-indexing history through a rename must not hard-error (permanent dead end), got: %v", err)
	}

	dml := dmlEvents(all)
	if len(dml) != 1 {
		t.Fatalf("expected the historical INSERT to index, got %d DML events", len(dml))
	}
	// Values land under the snapshot's CURRENT name — pre-#700 behavior,
	// positionally correct for a pure rename.
	if dml[0].RowAfter["client"] != "Alice" {
		t.Errorf("historical event must index under the snapshot's current column name, got %v", dml[0].RowAfter)
	}
}
