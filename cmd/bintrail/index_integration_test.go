//go:build integration

package main

import (
	"context"
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
	if !strings.Contains(err.Error(), "failed") {
		t.Errorf("expected a 'files failed' error, got: %v", err)
	}
}
