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

// TestParseFile_realBinlog_mariadb is the file-based MariaDB guard for the
// `bintrail index` path that docs/streaming.md advertises ("File-based bintrail
// index over MariaDB binlog files works too"). It produces a real MariaDB binlog
// on the source container, copies it out, parses it, and asserts the exact DML
// mix AND that the file-parser MariadbGTIDEvent case populated domain-server-seq
// GTIDs on the indexed rows. Mirrors TestParseFile_realBinlog (MySQL) against
// bintrail-test-mariadb (13307).
func TestParseFile_realBinlog_mariadb(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id       INT PRIMARY KEY AUTO_INCREMENT,
		customer VARCHAR(100)  NOT NULL,
		amount   DECIMAL(10,2) NOT NULL
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
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition (MariaDB): %v", err)
	}
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, amount) VALUES ('Alice', 99.99)")
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, amount) VALUES ('Bob', 50.00)")
	testutil.MustExec(t, sourceDB, "UPDATE orders SET amount = 109.99 WHERE customer = 'Alice'")
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
	var ins, upd, del int
	for _, ev := range dml {
		switch ev.EventType {
		case parser.EventInsert:
			ins++
		case parser.EventUpdate:
			upd++
		case parser.EventDelete:
			del++
		}
	}
	if ins != 2 || upd != 1 || del != 1 {
		t.Fatalf("expected 2 INSERT/1 UPDATE/1 DELETE from MariaDB binlog, got ins=%d upd=%d del=%d (total dml %d)", ins, upd, del, len(dml))
	}

	// The file-parser MariadbGTIDEvent case must populate domain-server-seq GTIDs.
	for _, ev := range dml {
		if ev.GTID == "" {
			t.Fatalf("DML event carries no GTID — the file-parser MariadbGTIDEvent case is not firing")
		}
		if strings.Count(ev.GTID, "-") != 2 || strings.Contains(ev.GTID, ":") {
			t.Fatalf("indexed GTID %q is not MariaDB domain-server-seq form", ev.GTID)
		}
	}

	// #1117: MariaDB 11.4+ writes cache-buffered events (TABLE_MAP, rows,
	// ANNOTATE) with end_log_pos=0 IN THE FILE ITSELF — the running-offset fill
	// in ParseFile must reconstruct real positions, never emit the underflowed
	// start_pos = 2^64-EventSize / end_pos = 0 shape.
	//
	// The assertion is EXACT, not merely sane/monotonic: true offsets are
	// reconstructed independently with a raw go-mysql parser by accumulating
	// EventSize (a binlog file is contiguous), and every genuine stored
	// end_log_pos in the file must agree with the accumulation — the
	// fill-chain → directly-written junction check that catches any
	// constant-offset inflation a monotonicity check would miss.
	type span struct{ start, end uint64 }
	var want []span
	acc := uint32(4)
	raw := replication.NewBinlogParser()
	rawErr := raw.ParseFile(filepath.Join(tmpDir, currentBinlog), 0, func(e *replication.BinlogEvent) error {
		prev := acc
		acc += e.Header.EventSize
		if e.Header.LogPos != 0 && e.Header.LogPos != acc {
			t.Errorf("accumulated offset %d disagrees with the genuine stored end_log_pos %d of %s — junction mismatch",
				acc, e.Header.LogPos, e.Header.EventType)
		}
		switch e.Header.EventType {
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			want = append(want, span{uint64(prev), uint64(acc)})
		}
		return nil
	})
	if rawErr != nil {
		t.Fatalf("raw ground-truth parse: %v", rawErr)
	}
	if len(want) != len(dml) {
		t.Fatalf("ground truth found %d row events, bintrail emitted %d DML events", len(want), len(dml))
	}
	for i, ev := range dml {
		if ev.StartPos != want[i].start || ev.EndPos != want[i].end {
			t.Errorf("dml[%d]: positions [%d, %d], want exactly [%d, %d]",
				i, ev.StartPos, ev.EndPos, want[i].start, want[i].end)
		}
	}
}
