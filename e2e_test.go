//go:build integration

package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bintrail/bintrail/internal/testutil"
)

// TestEndToEnd_fullPipeline exercises the bintrail binary through all commands:
// init → snapshot → index → query → recover → status → rotate.
//
// The binary is built with -cover so that running it with GOCOVERDIR captures
// coverage across process boundaries (Go 1.20+). After the pipeline completes,
// the raw coverage data is converted to Go's standard text format and the
// output directory is logged so callers can merge it.
//
// It requires:
//   - Docker container "bintrail-test-mysql" running on port 13306
//   - go build must succeed
func TestEndToEnd_fullPipeline(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	// ── 1. Build the bintrail binary with coverage instrumentation ──────────
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "bintrail")
	build := exec.Command("go", "build", "-cover", "-o", binPath, "./cmd/bintrail")
	build.Dir = projectRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build -cover failed: %v\n%s", err, out)
	}

	// Create a directory for binary coverage data.
	coverDir := filepath.Join(tmpDir, "covdata")
	if err := os.MkdirAll(coverDir, 0755); err != nil {
		t.Fatalf("failed to create coverage dir: %v", err)
	}

	// ── 2. Create test databases ─────────────────────────────────────────────
	sourceDB, sourceName := testutil.CreateTestDB(t)
	_, indexName := testutil.CreateTestDB(t)

	indexDSN := testutil.SnapshotDSN(indexName)
	sourceDSN := testutil.SnapshotDSN(sourceName)

	// ── 3. bintrail init ─────────────────────────────────────────────────────
	run(t, binPath, coverDir, "init", "--index-dsn", indexDSN, "--partitions", "7")

	// ── 4. Create source table ───────────────────────────────────────────────
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id       INT PRIMARY KEY AUTO_INCREMENT,
		customer VARCHAR(100) NOT NULL,
		status   VARCHAR(20)  NOT NULL DEFAULT 'new',
		amount   DECIMAL(10,2) NOT NULL
	)`)

	// ── 5. bintrail snapshot ─────────────────────────────────────────────────
	run(t, binPath, coverDir, "snapshot",
		"--source-dsn", sourceDSN,
		"--index-dsn", indexDSN,
		"--schemas", sourceName,
	)

	// ── 6. Flush to get a clean binlog boundary, then generate DML ──────
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	// Note the current binlog file (the one that will receive our DML).
	var currentBinlog, ignorePos, ignoreBinDo, ignoreBinIgn, ignoreGtid string
	if err := sourceDB.QueryRow("SHOW MASTER STATUS").Scan(
		&currentBinlog, &ignorePos, &ignoreBinDo, &ignoreBinIgn, &ignoreGtid,
	); err != nil {
		t.Fatalf("SHOW MASTER STATUS failed: %v", err)
	}

	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, status, amount) VALUES ('Alice', 'new', 99.99)")
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, status, amount) VALUES ('Bob', 'new', 50.00)")
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (customer, status, amount) VALUES ('Charlie', 'new', 75.00)")
	testutil.MustExec(t, sourceDB, "UPDATE orders SET status = 'shipped', amount = 109.99 WHERE customer = 'Alice'")
	testutil.MustExec(t, sourceDB, "DELETE FROM orders WHERE customer = 'Charlie'")

	// Flush again to seal the binlog file.
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	// ── 7. Copy only the relevant binlog file from container ─────────────
	binlogDir := filepath.Join(tmpDir, "binlogs")
	os.MkdirAll(binlogDir, 0755)

	cpCmd := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(binlogDir, currentBinlog),
	)
	if out, err := cpCmd.CombinedOutput(); err != nil {
		t.Fatalf("docker cp %s failed: %v\n%s", currentBinlog, err, out)
	}

	// ── 8. bintrail index (only the specific binlog file) ────────────────
	run(t, binPath, coverDir, "index",
		"--index-dsn", indexDSN,
		"--source-dsn", sourceDSN,
		"--binlog-dir", binlogDir,
		"--files", currentBinlog,
		"--schemas", sourceName,
	)

	// ── 8b. bintrail baseline (file-only, no DB required) ────────────────────
	mydumperDir := filepath.Join(tmpDir, "mydumper")
	if err := os.MkdirAll(mydumperDir, 0755); err != nil {
		t.Fatalf("create mydumper dir: %v", err)
	}
	mdMetadata := "Started dump at: 2025-02-28 00:00:00\n" +
		"Finished dump at: 2025-02-28 00:01:23\n" +
		"SHOW MASTER STATUS:\n" +
		"\tLog: binlog.000042\n" +
		"\tPos: 12345\n" +
		"\tGTID: 3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100\n"
	if err := os.WriteFile(filepath.Join(mydumperDir, "metadata"), []byte(mdMetadata), 0o644); err != nil {
		t.Fatalf("write mydumper metadata: %v", err)
	}
	mdSchema := "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL AUTO_INCREMENT,\n" +
		"  `customer` varchar(100) NOT NULL,\n" +
		"  `status` varchar(20) NOT NULL DEFAULT 'new',\n" +
		"  `amount` decimal(10,2) NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n"
	if err := os.WriteFile(filepath.Join(mydumperDir, sourceName+".orders-schema.sql"), []byte(mdSchema), 0o644); err != nil {
		t.Fatalf("write mydumper schema: %v", err)
	}
	mdData := "INSERT INTO `orders` VALUES(1,'Alice','new',99.99),(2,'Bob','new',50.00);\n"
	if err := os.WriteFile(filepath.Join(mydumperDir, sourceName+".orders.00000.sql"), []byte(mdData), 0o644); err != nil {
		t.Fatalf("write mydumper data: %v", err)
	}

	parquetDir := filepath.Join(tmpDir, "parquet")
	if err := os.MkdirAll(parquetDir, 0755); err != nil {
		t.Fatalf("create parquet dir: %v", err)
	}

	baselineOut := run(t, binPath, coverDir, "baseline",
		"--input", mydumperDir,
		"--output", parquetDir,
		"--compression", "none",
	)
	if !strings.Contains(baselineOut, "Baseline complete") {
		t.Errorf("expected 'Baseline complete' in baseline output:\n%s", baselineOut)
	}
	if !strings.Contains(baselineOut, "tables    : 1") {
		t.Errorf("expected 'tables    : 1' in baseline output:\n%s", baselineOut)
	}
	foundParquet := false
	_ = filepath.Walk(parquetDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && filepath.Ext(path) == ".parquet" {
			foundParquet = true
		}
		return nil
	})
	if !foundParquet {
		t.Error("expected at least one .parquet file in parquet output directory")
	}

	// ── 9. bintrail query ────────────────────────────────────────────────────
	queryOut := run(t, binPath, coverDir, "query",
		"--index-dsn", indexDSN,
		"--schema", sourceName,
		"--table", "orders",
		"--format", "json",
		"--limit", "100",
	)

	var events []map[string]any
	if err := json.Unmarshal([]byte(queryOut), &events); err != nil {
		t.Fatalf("failed to parse query JSON output: %v\n%s", err, queryOut)
	}
	// Should have 5 events: 3 INSERTs + 1 UPDATE + 1 DELETE.
	if len(events) != 5 {
		t.Errorf("expected 5 events, got %d", len(events))
		for i, e := range events {
			t.Logf("  event[%d]: type=%v schema=%v table=%v pk=%v",
				i, e["event_type"], e["schema_name"], e["table_name"], e["pk_values"])
		}
	}

	// Count by type.
	typeCounts := map[string]int{}
	for _, e := range events {
		if et, ok := e["event_type"].(string); ok {
			typeCounts[et]++
		}
	}
	if typeCounts["INSERT"] != 3 {
		t.Errorf("expected 3 INSERTs, got %d", typeCounts["INSERT"])
	}
	if typeCounts["UPDATE"] != 1 {
		t.Errorf("expected 1 UPDATE, got %d", typeCounts["UPDATE"])
	}
	if typeCounts["DELETE"] != 1 {
		t.Errorf("expected 1 DELETE, got %d", typeCounts["DELETE"])
	}

	// ── 9b. bintrail query --format csv ─────────────────────────────────────
	csvOut := run(t, binPath, coverDir, "query",
		"--index-dsn", indexDSN,
		"--schema", sourceName,
		"--table", "orders",
		"--format", "csv",
		"--limit", "100",
	)
	if !strings.HasPrefix(strings.TrimSpace(csvOut), "event_id") {
		t.Errorf("expected CSV header starting with 'event_id':\n%s", csvOut)
	}
	// 5 data rows + 1 header line.
	csvLines := strings.Split(strings.TrimSpace(csvOut), "\n")
	if len(csvLines) != 6 {
		t.Errorf("expected 6 CSV lines (1 header + 5 data), got %d", len(csvLines))
	}

	// ── 9c. bintrail query --format table ────────────────────────────────────
	tableOut := run(t, binPath, coverDir, "query",
		"--index-dsn", indexDSN,
		"--schema", sourceName,
		"--table", "orders",
		"--format", "table",
		"--limit", "100",
	)
	if !strings.Contains(tableOut, "TYPE") || !strings.Contains(tableOut, "PK_VALUES") {
		t.Errorf("expected table header columns in table output:\n%s", tableOut)
	}
	if !strings.Contains(tableOut, "INSERT") {
		t.Errorf("expected INSERT rows in table output:\n%s", tableOut)
	}

	// ── 9d. bintrail query --pk 1 (Alice: 1 INSERT + 1 UPDATE) ───────────────
	pkOut := run(t, binPath, coverDir, "query",
		"--index-dsn", indexDSN,
		"--schema", sourceName,
		"--table", "orders",
		"--pk", "1",
		"--format", "json",
		"--limit", "100",
	)
	var pkEvents []map[string]any
	if err := json.Unmarshal([]byte(pkOut), &pkEvents); err != nil {
		t.Fatalf("failed to parse pk query JSON: %v\n%s", err, pkOut)
	}
	if len(pkEvents) != 2 {
		t.Errorf("expected 2 events for pk=1 (Alice: INSERT + UPDATE), got %d", len(pkEvents))
	}

	// ── 9e. bintrail query --changed-column status (Alice UPDATE only) ────────
	changedOut := run(t, binPath, coverDir, "query",
		"--index-dsn", indexDSN,
		"--schema", sourceName,
		"--table", "orders",
		"--changed-column", "status",
		"--format", "json",
		"--limit", "100",
	)
	var changedEvents []map[string]any
	if err := json.Unmarshal([]byte(changedOut), &changedEvents); err != nil {
		t.Fatalf("failed to parse changed-column query JSON: %v\n%s", err, changedOut)
	}
	if len(changedEvents) != 1 {
		t.Errorf("expected 1 event with changed column 'status', got %d", len(changedEvents))
	}
	if len(changedEvents) == 1 {
		if et, ok := changedEvents[0]["event_type"].(string); !ok || et != "UPDATE" {
			t.Errorf("expected UPDATE for changed-column=status, got %v", changedEvents[0]["event_type"])
		}
	}

	// ── 10. bintrail recover (DELETE → INSERT) ──────────────────────────────
	recoverDeleteOut := run(t, binPath, coverDir, "recover",
		"--index-dsn", indexDSN,
		"--schema", sourceName,
		"--table", "orders",
		"--event-type", "DELETE",
		"--dry-run",
	)
	if !strings.Contains(recoverDeleteOut, "INSERT INTO") {
		t.Error("expected INSERT INTO in delete recovery output")
	}
	if !strings.Contains(recoverDeleteOut, "Charlie") {
		t.Errorf("expected Charlie's data in recovery output:\n%s", recoverDeleteOut)
	}

	// ── 11. bintrail recover (UPDATE → reverse) ─────────────────────────────
	recoverUpdateOut := run(t, binPath, coverDir, "recover",
		"--index-dsn", indexDSN,
		"--schema", sourceName,
		"--table", "orders",
		"--event-type", "UPDATE",
		"--pk", "1",
		"--dry-run",
	)
	if !strings.Contains(recoverUpdateOut, "UPDATE") {
		t.Error("expected UPDATE in update recovery output")
	}
	// Should restore original values (before image).
	if !strings.Contains(recoverUpdateOut, "'new'") {
		t.Errorf("expected original status 'new' in recovery:\n%s", recoverUpdateOut)
	}

	// ── 11b. bintrail recover --output (DELETE → file) ──────────────────────
	recoverFile := filepath.Join(tmpDir, "recovery.sql")
	run(t, binPath, coverDir, "recover",
		"--index-dsn", indexDSN,
		"--schema", sourceName,
		"--table", "orders",
		"--event-type", "DELETE",
		"--output", recoverFile,
	)
	recoverContent, err := os.ReadFile(recoverFile)
	if err != nil {
		t.Fatalf("expected recovery.sql to be created, read failed: %v", err)
	}
	if !strings.Contains(string(recoverContent), "INSERT INTO") {
		t.Errorf("expected INSERT INTO in recovery file:\n%s", string(recoverContent))
	}
	if !strings.Contains(string(recoverContent), "Charlie") {
		t.Errorf("expected Charlie's data in recovery file:\n%s", string(recoverContent))
	}

	// ── 12. bintrail status ──────────────────────────────────────────────────
	statusOut := run(t, binPath, coverDir, "status", "--index-dsn", indexDSN)
	if !strings.Contains(statusOut, "=== Indexed Files ===") {
		t.Error("expected 'Indexed Files' section in status")
	}
	if !strings.Contains(statusOut, "completed") {
		t.Errorf("expected 'completed' status in output:\n%s", statusOut)
	}
	if !strings.Contains(statusOut, "=== Partitions ===") {
		t.Error("expected 'Partitions' section in status")
	}

	// ── 13. bintrail rotate ──────────────────────────────────────────────────
	rotateOut := run(t, binPath, coverDir, "rotate",
		"--index-dsn", indexDSN,
		"--add-future", "3",
	)
	// rotate writes "added partition p_YYYYMMDD" lines to stdout.
	if !strings.Contains(rotateOut, "added partition") {
		t.Errorf("expected 'added partition' in rotate output:\n%s", rotateOut)
	}

	// ── 14. Convert binary coverage data to text format ──────────────────────
	covOut := filepath.Join(tmpDir, "cover_e2e.out")
	conv := exec.Command("go", "tool", "covdata", "textfmt", "-i="+coverDir, "-o="+covOut)
	if out, err := conv.CombinedOutput(); err != nil {
		t.Logf("warning: covdata textfmt failed (non-fatal): %v\n%s", err, out)
	} else {
		t.Logf("E2E binary coverage written to: %s", covOut)
		// Log summary percentages.
		pct := exec.Command("go", "tool", "covdata", "percent", "-i="+coverDir)
		if pctOut, err := pct.CombinedOutput(); err == nil {
			t.Logf("E2E coverage summary:\n%s", pctOut)
		}
	}

	t.Log("E2E pipeline completed successfully")
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// run executes the bintrail binary with args and returns stdout.
// It fails the test if the command exits with a non-zero status.
// When coverDir is non-empty, GOCOVERDIR is set so the instrumented binary
// writes coverage data on exit.
func run(t *testing.T, binPath, coverDir string, args ...string) string {
	t.Helper()
	cmd := exec.Command(binPath, args...)
	if coverDir != "" {
		cmd.Env = append(os.Environ(), "GOCOVERDIR="+coverDir)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("bintrail %s failed: %v\nstdout:\n%s\nstderr:\n%s",
			strings.Join(args, " "), err, stdout.String(), stderr.String())
	}
	return stdout.String()
}

// projectRoot finds the project root by looking for go.mod.
func projectRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find go.mod in any parent directory")
		}
		dir = parent
	}
}
