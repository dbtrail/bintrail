//go:build integration

package parser_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestParseFile_realBinlog_mariadbLogBinCompress is the #520 fixture: a MariaDB
// source with log_bin_compress=ON emits MARIADB_WRITE/UPDATE/DELETE_ROWS_
// COMPRESSED_EVENT_V1 instead of the standard rows events. Before #520 these
// hit handleRows' warn-and-skip default arm — the rows were never indexed
// (silent data loss). go-mysql decompresses them during decode, so the fix is
// dispatch-only; this test proves the whole path against a REAL compressed
// binlog: it produces one on the source container, verifies the compressed
// event types are actually present in the file (so the test cannot pass
// vacuously if compression fails to engage), then parses it with bintrail's
// file parser and asserts the exact DML mix WITH before/after images.
//
// Mirrors TestParseFile_realBinlog_mariadb (the uncompressed sibling) against
// bintrail-test-mariadb (13307). The stream path shares handleRows — the one
// dispatch this exercises — so the file-based fixture covers both.
func TestParseFile_realBinlog_mariadbLogBinCompress(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// log_bin_compress only compresses events LARGER than
	// log_bin_compress_min_len (default 256 bytes), so pin the threshold to its
	// minimum (10) and use a padded payload — compression then engages
	// deterministically. Both are dynamic GLOBALs; capture and restore the
	// previous values so the shared container is left exactly as found. (This
	// t.Cleanup is registered after CreateTestMariaDB's, so it runs BEFORE the
	// connection is closed.)
	var prevCompress, prevMinLen int
	if err := sourceDB.QueryRow(
		"SELECT @@GLOBAL.log_bin_compress, @@GLOBAL.log_bin_compress_min_len",
	).Scan(&prevCompress, &prevMinLen); err != nil {
		t.Fatalf("reading log_bin_compress globals: %v", err)
	}
	testutil.MustExec(t, sourceDB, "SET GLOBAL log_bin_compress = ON")
	testutil.MustExec(t, sourceDB, "SET GLOBAL log_bin_compress_min_len = 10")
	t.Cleanup(func() {
		sourceDB.Exec(fmt.Sprintf("SET GLOBAL log_bin_compress = %d", prevCompress))
		sourceDB.Exec(fmt.Sprintf("SET GLOBAL log_bin_compress_min_len = %d", prevMinLen))
	})

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id       INT PRIMARY KEY AUTO_INCREMENT,
		customer VARCHAR(100)  NOT NULL,
		amount   INT           NOT NULL,
		pad      VARCHAR(1000) NOT NULL
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	res, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	// Flush to a clean boundary, note the file, do DML, seal it.
	pad := strings.Repeat("x", 400)
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition (MariaDB): %v", err)
	}
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, amount, pad) VALUES ('Alice', 1, ?)", pad)
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, amount, pad) VALUES ('Bob', 2, ?)", pad)
	testutil.MustExec(t, sourceDB, "UPDATE orders SET amount = 42 WHERE customer = 'Alice'")
	testutil.MustExec(t, sourceDB, "DELETE FROM orders WHERE customer = 'Bob'")
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cpCmd := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mariadb:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog),
	)
	if out, err := cpCmd.CombinedOutput(); err != nil {
		testutil.SkipOrFailMariaDB(t, "docker cp %s from bintrail-test-mariadb failed: %v\n%s", currentBinlog, err, out)
	}

	// Vacuity guard: assert the binlog REALLY contains compressed row events.
	// Without this, a compression knob that silently failed to engage would
	// leave only standard rows events and the test would green-light the
	// pre-#520 warn-and-skip code. Raw mode reads headers without decoding.
	var compressedRowEvents int
	rawP := replication.NewBinlogParser()
	rawP.SetRawMode(true)
	err = rawP.ParseFile(filepath.Join(tmpDir, currentBinlog), 0, func(ev *replication.BinlogEvent) error {
		switch ev.Header.EventType {
		case replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1,
			replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1,
			replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
			compressedRowEvents++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("raw header scan of %s: %v", currentBinlog, err)
	}
	if compressedRowEvents == 0 {
		t.Fatalf("fixture produced no MARIADB_*_ROWS_COMPRESSED_EVENT_V1 in %s — log_bin_compress did not engage; the test would be vacuous", currentBinlog)
	}

	p := parser.New(tmpDir, res, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	events := make(chan parser.Event, 100)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		errCh <- p.ParseFile(context.Background(), currentBinlog, events)
	}()
	all := drainEvents(events)
	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile: %v", err)
	}

	dml := dmlEvents(all)
	var ins, upd, del []parser.Event
	for _, ev := range dml {
		switch ev.EventType {
		case parser.EventInsert:
			ins = append(ins, ev)
		case parser.EventUpdate:
			upd = append(upd, ev)
		case parser.EventDelete:
			del = append(del, ev)
		}
	}
	if len(ins) != 2 || len(upd) != 1 || len(del) != 1 {
		t.Fatalf("expected 2 INSERT/1 UPDATE/1 DELETE from compressed MariaDB binlog, got ins=%d upd=%d del=%d (total dml %d) — compressed row events were skipped, not indexed (#520)",
			len(ins), len(upd), len(del), len(dml))
	}

	// Images must be complete and correct — decompressed content, not just
	// event counts.
	for _, ev := range ins {
		if ev.RowBefore != nil || ev.RowAfter == nil {
			t.Fatalf("INSERT image shape wrong: before=%v after=%v", ev.RowBefore, ev.RowAfter)
		}
		if got := fmt.Sprint(ev.RowAfter["pad"]); got != pad {
			t.Fatalf("INSERT after-image pad mismatch: got %d bytes, want %d", len(got), len(pad))
		}
		if ev.PKValues == "" {
			t.Fatal("INSERT event carries no PK values")
		}
	}
	u := upd[0]
	if u.RowBefore == nil || u.RowAfter == nil {
		t.Fatalf("UPDATE must carry both images: before=%v after=%v", u.RowBefore, u.RowAfter)
	}
	if b, a := fmt.Sprint(u.RowBefore["amount"]), fmt.Sprint(u.RowAfter["amount"]); b != "1" || a != "42" {
		t.Fatalf("UPDATE before/after amount = %s/%s, want 1/42", b, a)
	}
	d := del[0]
	if d.RowBefore == nil || d.RowAfter != nil {
		t.Fatalf("DELETE must carry only a before image: before=%v after=%v", d.RowBefore, d.RowAfter)
	}
	if got := fmt.Sprint(d.RowBefore["customer"]); got != "Bob" {
		t.Fatalf("DELETE before-image customer = %q, want Bob", got)
	}
}
