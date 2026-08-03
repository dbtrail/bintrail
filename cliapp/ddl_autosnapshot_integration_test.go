//go:build integration

package cliapp

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestDDLAutoSnapshot_degradesLikeStream pins #1199's second half: the
// file-mode DDL hook's snapshot step (ddlAutoSnapshot, called by the SetOnDDL
// closure in runIndex) uses the stream hook's degraded validation semantics.
// One no-PK table in scope — the exact condition that no longer stops the
// stream since #1051 — must not fail file-mode DDL handling either: the
// snapshot succeeds, the invalid table is excluded, and the operator gets the
// same prominent EXCLUDED warning the stream hook emits.
func TestDDLAutoSnapshot_degradesLikeStream(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE ok_tbl (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE nopk_tbl (v INT) ENGINE=InnoDB`)

	// The pre-#1199 hook called strict TakeSnapshot, which fails on this exact
	// scope — asserted here so the test proves the semantic difference, not
	// just that some snapshot succeeded.
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err == nil {
		t.Fatal("fixture invalid: strict TakeSnapshot should fail on a scope with a no-PK table")
	}

	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	stats, err := ddlAutoSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("ddlAutoSnapshot must degrade, not fail, on a one-bad-table scope: %v", err)
	}
	if len(stats.ExcludedTables) != 1 || stats.ExcludedTables[0] != sourceName+".nopk_tbl" {
		t.Errorf("ExcludedTables = %v, want [%s.nopk_tbl]", stats.ExcludedTables, sourceName)
	}
	if stats.TableCount != 1 {
		t.Errorf("TableCount = %d, want 1 (ok_tbl)", stats.TableCount)
	}
	if !strings.Contains(logBuf.String(), "EXCLUDED") {
		t.Errorf("expected the prominent EXCLUDED warning (stream-hook contract), got logs: %s", logBuf.String())
	}
}
