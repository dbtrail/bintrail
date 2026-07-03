//go:build integration

package parser_test

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestParseFile_queryTextCapture drives the #699 capture path against a REAL
// binlog: with binlog_rows_query_log_events=ON the server writes a
// ROWS_QUERY_EVENT before each statement's row events, and every emitted DML
// event must carry that statement in QueryText — per-statement, so the UPDATE's
// rows must NOT carry the INSERT's text.
func TestParseFile_queryTextCapture(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id       INT PRIMARY KEY AUTO_INCREMENT,
		customer VARCHAR(100) NOT NULL,
		amount   DECIMAL(10,2) NOT NULL
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	// Session-scoped statement logging on a dedicated connection: only THIS
	// conn's statements get ROWS_QUERY events, no global pollution of the
	// suite's shared binlog (#415 discipline).
	ctx := context.Background()
	conn, err := sourceDB.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn failed: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET SESSION binlog_rows_query_log_events = ON"); err != nil {
		// Skip ONLY on ER_UNKNOWN_SYSTEM_VARIABLE (a server without the
		// variable, e.g. MariaDB which spells it binlog_annotate_row_events).
		// Any other failure must FAIL, not skip.
		var myErr *drivermysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1193 {
			t.Skipf("binlog_rows_query_log_events not supported on this server: %v", err)
		}
		t.Fatalf("SET SESSION binlog_rows_query_log_events failed for a non-version reason: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition failed: %v", err)
	}

	// Two distinct statements on the logging connection. The multi-row INSERT
	// produces two row events sharing ONE statement text; the UPDATE's row
	// must carry its own.
	insertSQL := "INSERT INTO orders (customer, amount) VALUES ('Alice', 99.99), ('Bob', 50.00)"
	updateSQL := "UPDATE orders SET amount = 109.99 WHERE customer = 'Alice'"
	if _, err := conn.ExecContext(ctx, insertSQL); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if _, err := conn.ExecContext(ctx, updateSQL); err != nil {
		t.Fatalf("UPDATE failed: %v", err)
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
		t.Fatalf("ParseFile failed: %v", err)
	}

	dml := dmlEvents(all)
	if len(dml) != 3 {
		t.Fatalf("expected 3 DML events (2 INSERT rows + 1 UPDATE), got %d", len(dml))
	}

	// Both INSERT rows share the multi-row statement's text.
	for i := 0; i < 2; i++ {
		if dml[i].EventType != parser.EventInsert {
			t.Fatalf("event[%d]: type = %d, want INSERT", i, dml[i].EventType)
		}
		if dml[i].QueryText != insertSQL {
			t.Errorf("INSERT row %d: QueryText = %q, want %q", i, dml[i].QueryText, insertSQL)
		}
	}
	// The UPDATE row carries its OWN statement, not the INSERT's.
	if dml[2].EventType != parser.EventUpdate {
		t.Fatalf("event[2]: type = %d, want UPDATE", dml[2].EventType)
	}
	if dml[2].QueryText != updateSQL {
		t.Errorf("UPDATE row: QueryText = %q, want %q", dml[2].QueryText, updateSQL)
	}
}

// TestParseFile_queryTextAbsentWhenVariableOff pins the degradation contract:
// with the variable at its default (OFF), events parse exactly as before and
// QueryText stays empty.
func TestParseFile_queryTextAbsentWhenVariableOff(t *testing.T) {
	binlogDir, binlogFile, schemaName, resolver := setupBinlog(t)

	p := parser.New(binlogDir, resolver, parser.Filters{
		Schemas: map[string]bool{schemaName: true},
	}, nil)

	events := make(chan parser.Event, 100)
	done := make(chan error, 1)
	go func() {
		defer close(events)
		done <- p.ParseFile(context.Background(), binlogFile, events)
	}()
	all := drainEvents(events)
	if err := <-done; err != nil {
		t.Fatalf("ParseFile failed: %v", err)
	}

	dml := dmlEvents(all)
	if len(dml) == 0 {
		t.Fatal("expected DML events from setupBinlog traffic")
	}
	for i, ev := range dml {
		if ev.QueryText != "" {
			t.Errorf("event[%d]: QueryText = %q, want empty (binlog_rows_query_log_events is OFF)", i, ev.QueryText)
		}
	}
}

// TestParseFile_queryTextMidTransactionToggle is the regression guard for the
// stale-attribution bug the #699 review reproduced: MySQL allows
// SET SESSION binlog_rows_query_log_events=OFF INSIDE an open transaction, so
// a later statement in the SAME transaction emits rows with no ROWS_QUERY of
// its own. Those rows must carry NO text — never the previous statement's
// (a confidently wrong forensic attribution). The STMT_END_F clear is what
// makes this hold; GTID/QUERY boundaries do not exist mid-transaction.
func TestParseFile_queryTextMidTransactionToggle(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id     INT PRIMARY KEY,
		amount DECIMAL(10,2) NOT NULL
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	ctx := context.Background()
	conn, err := sourceDB.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn failed: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET SESSION binlog_rows_query_log_events = ON"); err != nil {
		var myErr *drivermysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1193 {
			t.Skipf("binlog_rows_query_log_events not supported on this server: %v", err)
		}
		t.Fatalf("SET SESSION failed for a non-version reason: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition failed: %v", err)
	}

	firstSQL := "INSERT INTO orders (id, amount) VALUES (1, 10.00)"
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, firstSQL); err != nil {
		t.Fatalf("INSERT #1 failed: %v", err)
	}
	// Toggle OFF inside the open transaction — allowed by MySQL.
	if _, err := tx.ExecContext(ctx, "SET SESSION binlog_rows_query_log_events = OFF"); err != nil {
		t.Fatalf("mid-transaction SET SESSION OFF failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO orders (id, amount) VALUES (2, 20.00)"); err != nil {
		t.Fatalf("INSERT #2 failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
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
		t.Fatalf("ParseFile failed: %v", err)
	}

	dml := dmlEvents(all)
	if len(dml) != 2 {
		t.Fatalf("expected 2 DML events, got %d", len(dml))
	}
	if dml[0].QueryText != firstSQL {
		t.Errorf("row 1: QueryText = %q, want %q", dml[0].QueryText, firstSQL)
	}
	if dml[1].QueryText != "" {
		t.Errorf("row 2: QueryText = %q, want EMPTY — a statement that logged no ROWS_QUERY must never inherit the previous statement's text", dml[1].QueryText)
	}
}
