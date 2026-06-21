//go:build integration

package pgstreamrun_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestOne_EndToEnd_PostgresToIndexToRecovery is the permanent form of the capture
// spike's Part C: a live PostgreSQL change stream flows through pgstreamrun.One into
// the real MySQL index (binlog_events), is read back by the query engine, and
// produces reversal SQL — with an out-of-line TOAST value round-tripping all the way.
// This is what closes #530's end-to-end acceptance.
//
// Needs BOTH a live MySQL index (BINTRAIL_TEST_DSN, the CI default) and a live
// PostgreSQL source (BINTRAIL_TEST_PG_DSN; not set on the MySQL CI jobs → skips).
func TestOne_EndToEnd_PostgresToIndexToRecovery(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgsr_it"
	const pub = "bintrail_pgsr_it_pub"
	const tbl = "pgsr_it_t"
	const bigSize = 6000

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pg.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, small_col text, big_col text)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN big_col SET STORAGE EXTERNAL", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN:    indexDSN,
		ReplDSN:     replDSN(pgDSN),
		QueryDSN:    pgDSN,
		SlotName:    slot,
		Publication: pub,
		ServerID:    42,
		Tables:      "public." + tbl,
		BatchSize:   100,
		Checkpoint:  200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()

	// Wait until the consumer's capturer has the slot active, then do DML so every
	// change is captured.
	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")

	bigVal := strings.Repeat("X", bigSize)
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'orig', $1)", tbl), bigVal)
	mustExec(fmt.Sprintf("UPDATE %s SET small_col='changed' WHERE id=1", tbl)) // big_col untouched

	// Wait until both row events are indexed (flushed on the 200ms ticker).
	waitFor(t, 15*time.Second, func() bool {
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ?", tbl).Scan(&n); err != nil {
			return false
		}
		return n >= 2
	}, "INSERT+UPDATE indexed into binlog_events")

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One returned error: %v", err)
	}

	// Read back through the real query engine.
	rows, err := query.New(indexDB).Fetch(ctx, query.Options{Schema: "public", Table: tbl, Order: "ASC"})
	if err != nil {
		t.Fatalf("query Fetch: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("indexed %d events, want 2 (INSERT + UPDATE)", len(rows))
	}

	var upd *query.ResultRow
	for i := range rows {
		if rows[i].EventType == event.EventUpdate {
			upd = &rows[i]
		}
	}
	if upd == nil {
		t.Fatal("no UPDATE event read back from the index")
	}
	// The #530 headline: the out-of-line TOAST value round-trips into the index.
	if got := upd.RowBefore["big_col"]; got != bigVal {
		t.Errorf("UPDATE RowBefore[big_col] is %d bytes (%T), want the real %d-byte value", lenOf(got), got, bigSize)
	}
	// Option B: the after-image carries the real (unchanged) value, not a sentinel.
	if got := upd.RowAfter["big_col"]; got != bigVal {
		t.Errorf("UPDATE RowAfter[big_col] not resolved to the real value (Option B): %d bytes (%T)", lenOf(got), got)
	}

	// Recovery SQL: the TOAST value round-trips all the way into reversal SQL.
	var buf bytes.Buffer
	n, err := recovery.New(indexDB, nil).GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("GenerateSQLFromRows: %v", err)
	}
	if n == 0 {
		t.Fatal("no reversal statements generated")
	}
	if !strings.Contains(buf.String(), bigVal) {
		t.Error("reversal SQL does not contain the TOAST value — it did not round-trip into recovery")
	}
}

// TestOne_CapturerFailureSurfaces proves the cancellation bridge in One: when the
// capturer fails on its own (here: a non-existent publication makes cap.Run return
// before streaming), One must surface that error PROMPTLY rather than hang forever
// on a never-closed events channel under a never-cancelled parent ctx (the silent
// hung-stream class). The 15s timeout is the hang detector.
func TestOne_CapturerFailureSurfaces(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	_, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	cfg := pgstreamrun.Config{
		IndexDSN:    indexDSN,
		ReplDSN:     replDSN(pgDSN),
		QueryDSN:    pgDSN,
		SlotName:    "bintrail_pgsr_fail_slot",
		Publication: "bintrail_pgsr_nonexistent_pub", // does not exist → cap.Run fails
		ServerID:    7,
		Checkpoint:  200 * time.Millisecond,
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pgstreamrun.One(runCtx, cfg) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("One returned nil; expected the capturer's publication failure to surface")
		}
		if !strings.Contains(err.Error(), "does not exist") {
			t.Errorf("unexpected error (want publication failure): %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("One hung — a capturer failure was not surfaced (cancellation bridge missing)")
	}
}

// ── helpers ──

func replDSN(base string) string {
	if strings.Contains(base, "?") {
		return base + "&replication=database"
	}
	return base + "?replication=database"
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func lenOf(v any) int {
	if s, ok := v.(string); ok {
		return len(s)
	}
	return -1
}
