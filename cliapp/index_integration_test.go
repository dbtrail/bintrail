//go:build integration

package cliapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunIndex_failedFileReturnsNonZero verifies that when a binlog file cannot
// be indexed, `bintrail index` exits non-zero instead of logging the failure and
// returning success (exit 0) — the silent failure a cron/CI wrapper read as a
// clean run (#652). The per-file failure is still recorded as status='failed' in
// index_state and --all still processes the remaining files; only the final exit
// code changes.
//
// The failure is forced with a garbage binlog file (invalid magic), which the
// parser rejects — a deterministic stand-in for the oversized-event rejection
// that motivated the issue, reaching the same per-file error aggregation.
func TestRunIndex_failedFileReturnsNonZero(t *testing.T) {
	indexDB, name := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	// A snapshot must exist so EnsureResolver (sourceDB=nil) loads from the index
	// and execution reaches the file loop.
	testutil.InsertSnapshot(t, indexDB, 1, "2026-01-01 00:00:00",
		"testdb", "orders", "id", 1, "PRI", "int", "NO")

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "binlog.000001"), []byte("not a valid binlog file"), 0o644); err != nil {
		t.Fatalf("write garbage binlog: %v", err)
	}

	saved := struct {
		index, source, binlogDir, files, format, schemas, tables string
		all, skip                                                bool
	}{idxIndexDSN, idxSourceDSN, idxBinlogDir, idxFiles, idxFormat, idxSchemas, idxTables, idxAll, idxSkipSourceCheck}
	t.Cleanup(func() {
		idxIndexDSN, idxSourceDSN, idxBinlogDir = saved.index, saved.source, saved.binlogDir
		idxFiles, idxFormat, idxSchemas, idxTables = saved.files, saved.format, saved.schemas, saved.tables
		idxAll, idxSkipSourceCheck = saved.all, saved.skip
	})

	idxIndexDSN = testutil.IntegrationDSN(name)
	idxSourceDSN = ""
	idxSkipSourceCheck = true // offline: no source pre-flight
	idxBinlogDir = dir
	idxFiles = "binlog.000001"
	idxAll = false
	idxFormat = "text"
	idxSchemas = ""
	idxTables = ""

	// runIndex reads cmd.Context(); set it so indexFile's context.WithCancel
	// has a non-nil parent (cobra would set it via ExecuteContext at runtime).
	indexCmd.SetContext(context.Background())
	err := runIndex(indexCmd, nil)
	if err == nil {
		t.Fatal("expected runIndex to return non-zero when a file fails to index, got nil (exit-0 silent failure — #652)")
	}
	// Tight phrase from the aggregation error (index.go) so a setup regression
	// that errors EARLY (e.g. "failed to connect to index database") can't make
	// this pass for the wrong reason.
	if !strings.Contains(err.Error(), "file(s) failed") {
		t.Errorf("expected the per-file aggregation error, got: %v", err)
	}
	// The summary must carry the first per-file failure (%w), or a typed
	// cause loses its usage-telemetry class behind the summary (#1503). This
	// is the loop's firstErr wiring; the helper alone has a unit test.
	if errors.Unwrap(err) == nil {
		t.Errorf("summary lost the per-file cause: %v", err)
	}
}

// TestRunIndex_allContinuesPastFailure verifies the --all resilience contract
// the fix preserves: a failed file does NOT stop the loop (every file is still
// attempted), and the run exits non-zero. With one garbage file the single-file
// test cannot distinguish continue-then-fail from fail-fast; two files can —
// both must get an index_state row (a fail-fast regression would skip file2).
func TestRunIndex_allContinuesPastFailure(t *testing.T) {
	indexDB, name := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	testutil.InsertSnapshot(t, indexDB, 1, "2026-01-01 00:00:00",
		"testdb", "orders", "id", 1, "PRI", "int", "NO")

	dir := t.TempDir()
	for _, f := range []string{"binlog.000001", "binlog.000002"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("not a valid binlog file"), 0o644); err != nil {
			t.Fatalf("write %s: %v", f, err)
		}
	}

	saved := struct {
		index, source, binlogDir, files, format, schemas, tables string
		all, skip                                                bool
	}{idxIndexDSN, idxSourceDSN, idxBinlogDir, idxFiles, idxFormat, idxSchemas, idxTables, idxAll, idxSkipSourceCheck}
	t.Cleanup(func() {
		idxIndexDSN, idxSourceDSN, idxBinlogDir = saved.index, saved.source, saved.binlogDir
		idxFiles, idxFormat, idxSchemas, idxTables = saved.files, saved.format, saved.schemas, saved.tables
		idxAll, idxSkipSourceCheck = saved.all, saved.skip
	})

	idxIndexDSN = testutil.IntegrationDSN(name)
	idxSourceDSN = ""
	idxSkipSourceCheck = true
	idxBinlogDir = dir
	idxFiles = ""
	idxAll = true // process every file in the dir
	idxFormat = "text"
	idxSchemas = ""
	idxTables = ""

	indexCmd.SetContext(context.Background())
	err := runIndex(indexCmd, nil)
	if err == nil {
		t.Fatal("expected non-zero when files fail, got nil")
	}

	// Both files must have been attempted — proves the loop did not fail-fast on
	// the first failure (the --all continue-on-failure contract).
	var attempted int
	if qerr := indexDB.QueryRow(
		"SELECT COUNT(*) FROM index_state WHERE binlog_file IN ('binlog.000001','binlog.000002')",
	).Scan(&attempted); qerr != nil {
		t.Fatalf("count index_state: %v", qerr)
	}
	if attempted != 2 {
		t.Errorf("expected both files attempted (--all continues past a failure), got %d index_state rows", attempted)
	}
}
