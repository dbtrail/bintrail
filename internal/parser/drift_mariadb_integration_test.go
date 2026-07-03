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

// TestParseFile_schemaDrift_mariadb mirrors TestParseFile_schemaDriftFailsLoud
// against MariaDB (container bintrail-test-mariadb, 13307). It verifies the
// load-bearing cross-flavor assumption the doctor check and docs advertise:
// MariaDB's binlog_row_metadata=FULL TABLE_MAP decodes through the same
// go-mysql ColumnName path, so the drift guard genuinely fires there — if the
// decode differed, the guard would degrade to a silent no-op on MariaDB
// (fail-open, exactly the corruption class #700 exists to stop) and only a
// real-binlog test can notice.
func TestParseFile_schemaDrift_mariadb(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// GLOBAL-only variable on MariaDB too: read original (default NO_LOG),
	// set FULL, restore in Cleanup.
	var original string
	if err := sourceDB.QueryRow("SELECT @@binlog_row_metadata").Scan(&original); err != nil {
		var myErr *drivermysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1193 {
			t.Skipf("binlog_row_metadata not supported on this MariaDB: %v", err)
		}
		testutil.SkipOrFailMariaDB(t, "read binlog_row_metadata failed: %v", err)
	}
	testutil.MustExec(t, sourceDB, "SET GLOBAL binlog_row_metadata = 'FULL'")
	t.Cleanup(func() {
		if _, err := sourceDB.Exec("SET GLOBAL binlog_row_metadata = ?", original); err != nil {
			t.Errorf("restore binlog_row_metadata=%s failed: %v", original, err)
		}
	})

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id       INT PRIMARY KEY AUTO_INCREMENT,
		customer VARCHAR(100) NOT NULL,
		amount   DECIMAL(10,2) NOT NULL
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	staleResolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition (MariaDB): %v", err)
	}

	// Same stale-vs-historical timing discipline as the MySQL test: the
	// post-rename events must be unambiguously NEWER than the snapshot.
	time.Sleep(2 * time.Second)

	// Matching traffic parses cleanly under FULL metadata...
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, amount) VALUES ('Alice', 99.99)")
	// ...then the same-count rename the count guard can't see.
	testutil.MustExec(t, sourceDB, "ALTER TABLE orders RENAME COLUMN customer TO client")
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (client, amount) VALUES ('Bob', 50.00)")

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cpCmd := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mariadb:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog),
	)
	if out, err := cpCmd.CombinedOutput(); err != nil {
		testutil.SkipOrFailMariaDB(t, "docker cp %s from bintrail-test-mariadb failed: %v\n%s", currentBinlog, err, out)
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

	dml := dmlEvents(all)
	if len(dml) != 1 {
		t.Errorf("expected exactly 1 pre-drift DML event, got %d", len(dml))
	} else if dml[0].RowAfter["customer"] == nil {
		t.Errorf("pre-drift event must map the snapshot's column name, got %v", dml[0].RowAfter)
	}

	if parseErr == nil {
		t.Fatal("MariaDB FULL metadata must trip the drift guard on the renamed column — a nil error means the guard silently no-ops on this flavor (fail-open)")
	}
	for _, want := range []string{"schema drift", "client", "customer", "bintrail snapshot"} {
		if !strings.Contains(parseErr.Error(), want) {
			t.Errorf("drift error missing %q: %v", want, parseErr)
		}
	}
}
