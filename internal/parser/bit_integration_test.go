//go:build integration

package parser_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestParseFile_bitColumns verifies the end-to-end fix for #497: BIT columns are
// decoded by go-mysql as a signed int64, so a BIT(64) with the high bit set comes
// back negative; it must be indexed as the correct unsigned value. BIT(<64) values
// are already positive and must be unchanged.
func TestParseFile_bitColumns(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE bt (
		id   INT PRIMARY KEY,
		b64  BIT(64) NOT NULL,
		b8   BIT(8)  NOT NULL
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	res, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition failed: %v", err)
	}

	// b64 = all 64 bits set (18446744073709551615): the high bit is set, so an
	// unfixed decoder returns int64(-1). b8 = 255 (positive; must be unchanged).
	testutil.MustExec(t, sourceDB,
		`INSERT INTO bt (id, b64, b8) VALUES (1, b'1111111111111111111111111111111111111111111111111111111111111111', b'11111111')`)

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

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

	var ins []parser.Event
	for ev := range events {
		if ev.Table == "bt" && ev.EventType == parser.EventInsert {
			ins = append(ins, ev)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile returned error: %v", err)
	}
	if len(ins) != 1 {
		t.Fatalf("expected 1 INSERT for table bt, got %d", len(ins))
	}

	ev := ins[0]
	if got := ev.RowAfter["b64"]; got != uint64(18446744073709551615) {
		t.Errorf("b64 BIT(64) all bits set: want uint64 max, got %#v (%T)", got, got)
	}
	if got := ev.RowAfter["b8"]; got != uint64(255) {
		t.Errorf("b8 BIT(8) = 255: want uint64(255), got %#v (%T)", got, got)
	}
}
