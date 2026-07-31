//go:build integration

package parser_test

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestParseFile_commitTimestampCapture drives the #18 capture path against a
// REAL binlog. Two facts are worth proving on a live server rather than on a
// hand-built event:
//
//  1. The value the server actually writes is microseconds since epoch — a
//     unit error here would be invisible in a unit test that feeds back the
//     same constant, and would produce timestamps off by a factor of 1000.
//  2. MySQL 8.0 writes it into the ANONYMOUS_GTID_EVENT too, so capture does
//     NOT require gtid_mode=ON. The suite's container runs with GTIDs off,
//     which makes this test the evidence for that claim rather than an
//     assumption in a doc comment.
func TestParseFile_commitTimestampCapture(t *testing.T) {
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

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition failed: %v", err)
	}

	// Bracket the writes with the server's own clock: the captured commit
	// timestamp must land inside this window, which is what actually pins the
	// UNIT (a value in milliseconds or nanoseconds falls outside it by orders
	// of magnitude). Read from the source, not the test host, so a clock skew
	// between the container and the host cannot fail the assertion.
	before := serverNowMicros(t, sourceDB)
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, amount) VALUES ('Alice', 99.99)")
	testutil.MustExec(t, sourceDB, "UPDATE orders SET amount = 109.99 WHERE customer = 'Alice'")
	after := serverNowMicros(t, sourceDB)

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
		t.Fatalf("expected 2 DML events (1 INSERT + 1 UPDATE), got %d", len(dml))
	}

	for i, ev := range dml {
		if ev.CommitTsUS == 0 {
			t.Fatalf("event[%d]: CommitTsUS = 0 — this server writes a commit timestamp "+
				"(MySQL 8.0.1+ stamps it on the ANONYMOUS_GTID_EVENT even with gtid_mode=OFF); "+
				"a zero here means the parser dropped it", i)
		}
		if ev.CommitTsUS < before || ev.CommitTsUS > after {
			t.Errorf("event[%d]: CommitTsUS = %d, outside the [%d, %d] microsecond window the "+
				"statements ran in — the value is not microseconds since epoch",
				i, ev.CommitTsUS, before, after)
		}
		// The whole point of the column: sub-second resolution the
		// one-second common header cannot express — so both values must be
		// readings of the same clock. Exact same-second equality is too
		// strict: the header timestamp is stamped at statement execution
		// and the commit timestamp at commit, so a transaction that
		// straddles a second boundary legitimately truncates to a later
		// second (observed on a loaded CI runner: commit second N+1 vs
		// header second N, #1164). Allow a small forward-only skew; a unit
		// error (ms/ns) misses by orders of magnitude and is already
		// pinned by the [before, after] window above.
		sec := uint64(ev.Timestamp.Unix())
		if commitSec := ev.CommitTsUS / 1_000_000; commitSec < sec || commitSec-sec > 5 {
			t.Errorf("event[%d]: CommitTsUS %d truncates to epoch second %d, but the event's own "+
				"timestamp is %d — the two clocks disagree",
				i, ev.CommitTsUS, commitSec, sec)
		}
	}

	// Two separate transactions commit at different instants; equal
	// microsecond stamps would mean the value is being carried over rather
	// than read per transaction.
	if dml[0].CommitTsUS == dml[1].CommitTsUS {
		t.Errorf("both events carry CommitTsUS = %d; two separate transactions cannot share "+
			"a microsecond commit instant — the value is stale, not per-transaction", dml[0].CommitTsUS)
	}
}

// serverNowMicros reads the SOURCE server's clock in microseconds since epoch,
// so the window assertion above compares the captured stamp against the clock
// that produced it rather than the test host's.
func serverNowMicros(t *testing.T, db *sql.DB) uint64 {
	t.Helper()
	var us uint64
	if err := db.QueryRow("SELECT ROUND(UNIX_TIMESTAMP(NOW(6)) * 1000000)").Scan(&us); err != nil {
		t.Fatalf("read the source server clock: %v", err)
	}
	return us
}
