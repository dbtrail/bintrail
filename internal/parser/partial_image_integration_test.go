//go:build integration

package parser_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestParseFile_partialRowImageFailsLoud is the end-to-end repro for #493: when a
// session sets binlog_row_image=MINIMAL (bypassing the server-global SHOW
// VARIABLES pre-flight), the resulting UPDATE/DELETE events carry partial row
// images — go-mysql pads the absent columns to nil. Without the per-row guard
// those NULLs would be indexed as genuine column values, silently corrupting the
// before/after images that `recover` trusts. The guard must instead abort
// parsing with a clear error and emit nothing for the partial event.
func TestParseFile_partialRowImageFailsLoud(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// A PK is required for MINIMAL to actually drop columns: without one, MySQL
	// logs the full before-image and no skip occurs.
	testutil.MustExec(t, sourceDB, `CREATE TABLE p (
		id   INT PRIMARY KEY,
		qty  INT NOT NULL,
		note VARCHAR(64) NOT NULL
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	res, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	// SET SESSION is per-connection, so pin ONE connection for the whole
	// seed→MINIMAL→mutate sequence — running it on the pooled *sql.DB would not
	// guarantee the SET and the UPDATE land on the same connection.
	conn, err := sourceDB.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire pinned conn: %v", err)
	}
	defer conn.Close()
	mustExecConn := func(q string) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	// Seed under FULL so the row exists, then switch the SESSION to MINIMAL and
	// mutate — this is exactly the bypass the server-global check cannot catch.
	mustExecConn("SET SESSION binlog_row_image = FULL")
	mustExecConn(`INSERT INTO p (id, qty, note) VALUES (1, 10, 'a')`)

	mustExecConn("SET SESSION binlog_row_image = MINIMAL")
	mustExecConn("FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition failed: %v", err)
	}
	// Update a non-PK column: the UPDATE before-image is PK-only (qty + note
	// absent) — the partial-image case.
	mustExecConn(`UPDATE p SET qty = 99 WHERE id = 1`)
	// Restore so we don't leak the session setting; FLUSH closes the test binlog.
	mustExecConn("SET SESSION binlog_row_image = FULL")
	mustExecConn("FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cpCmd := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog),
	)
	if out, err := cpCmd.CombinedOutput(); err != nil {
		t.Fatalf("docker cp %s failed: %v\n%s", currentBinlog, err, out)
	}

	p := parser.New(tmpDir, res, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	events := make(chan parser.Event, 50)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		errCh <- p.ParseFile(ctx, currentBinlog, events)
	}()

	var got []parser.Event
	for ev := range events {
		if ev.Table == "p" {
			got = append(got, ev)
		}
	}
	err = <-errCh
	if err == nil {
		t.Fatalf("expected ParseFile to fail loud on the MINIMAL UPDATE, got nil (events: %+v)", got)
	}
	if !strings.Contains(err.Error(), "partial binlog row image") {
		t.Errorf("error should name the partial-image cause, got: %v", err)
	}
	if !strings.Contains(err.Error(), "FULL") {
		t.Errorf("error should mention binlog_row_image=FULL, got: %v", err)
	}
	// Critically: the partial UPDATE must NOT have been emitted as an event whose
	// absent columns are stored as NULL.
	for _, ev := range got {
		if ev.EventType == parser.EventUpdate {
			t.Errorf("partial UPDATE must not be emitted (it would store absent columns as NULL); got %+v", ev)
		}
	}
}
