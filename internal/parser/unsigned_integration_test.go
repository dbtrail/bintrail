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

// TestParseFile_unsignedIntegers verifies the end-to-end fix for #490: UNSIGNED
// integer columns whose value has the high bit set (which go-mysql decodes as a
// negative signed int) are indexed as the correct unsigned value, including when
// the column is the primary key.
func TestParseFile_unsignedIntegers(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE u (
		id  BIGINT UNSIGNED PRIMARY KEY,
		n   INT UNSIGNED NOT NULL,
		m   MEDIUMINT UNSIGNED NOT NULL
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

	// Max values for each width: the high bit is set, so an unfixed decoder
	// would return these as negative.
	testutil.MustExec(t, sourceDB,
		`INSERT INTO u (id, n, m) VALUES (18446744073709551615, 4294967295, 16777215)`)
	// Update a non-PK column so the UPDATE before-image carries the unsigned PK —
	// this is what `recover` keys on, so the before-image PK must be correct too.
	testutil.MustExec(t, sourceDB,
		`UPDATE u SET n = 1 WHERE id = 18446744073709551615`)

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

	var ins, upd []parser.Event
	for ev := range events {
		if ev.Table != "u" {
			continue
		}
		switch ev.EventType {
		case parser.EventInsert:
			ins = append(ins, ev)
		case parser.EventUpdate:
			upd = append(upd, ev)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile returned error: %v", err)
	}
	if len(ins) != 1 {
		t.Fatalf("expected 1 INSERT for table u, got %d", len(ins))
	}

	ev := ins[0]
	if got := ev.RowAfter["id"]; got != uint64(18446744073709551615) {
		t.Errorf("id (BIGINT UNSIGNED): want uint64 max, got %#v (%T)", got, got)
	}
	if got := ev.RowAfter["n"]; got != uint32(4294967295) {
		t.Errorf("n (INT UNSIGNED): want uint32 max, got %#v (%T)", got, got)
	}
	if got := ev.RowAfter["m"]; got != uint32(16777215) {
		t.Errorf("m (MEDIUMINT UNSIGNED): want 16777215, got %#v (%T)", got, got)
	}
	// The unsigned PK must serialize to the correct value, not a negative one.
	if ev.PKValues != "18446744073709551615" {
		t.Errorf("PKValues: want 18446744073709551615, got %q", ev.PKValues)
	}

	// UPDATE before-image: the unsigned PK that recover keys on must be correct,
	// both in RowBefore and in the PKValues built from the before-image.
	if len(upd) != 1 {
		t.Fatalf("expected 1 UPDATE for table u, got %d", len(upd))
	}
	u := upd[0]
	if got := u.RowBefore["id"]; got != uint64(18446744073709551615) {
		t.Errorf("UPDATE RowBefore id: want uint64 max, got %#v (%T)", got, got)
	}
	if u.PKValues != "18446744073709551615" {
		t.Errorf("UPDATE PKValues (from before-image): want 18446744073709551615, got %q", u.PKValues)
	}
}
