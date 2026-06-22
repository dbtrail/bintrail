//go:build integration

package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedCascadeIndex builds a minimal indexed cascade in a fresh index DB: a
// parent DELETE plus two child INSERTs that referenced it, and the fk_constraints
// row marking child.pid -> parent.id ON DELETE CASCADE. Returns (db, dbName, dsn).
func seedCascadeIndex(t *testing.T) (*sql.DB, string, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")

	// child INSERTs (the cascade victims' last indexed state) and the parent
	// DELETE that cascaded them. The cascade child deletes are intentionally
	// NOT inserted — that is the blind spot the command reconstructs.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, childTs, nil,
		dbName, "child", 1 /* INSERT */, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, childTs, nil,
		dbName, "child", 1 /* INSERT */, "11", nil, nil, []byte(`{"id":11,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, parentTs, nil,
		dbName, "parent", 3 /* DELETE */, "1", nil, []byte(`{"id":1}`), nil)

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_child', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`,
		dbName, dbName)

	return db, dbName, testutil.IntegrationDSN(dbName)
}

func runCascadeCmd(t *testing.T) error {
	t.Helper()
	c := &cobra.Command{}
	c.SetContext(context.Background())
	return runRecoverCascade(c, nil)
}

// TestRecoverCascade_endToEnd drives the command over a seeded index and checks
// the emitted SQL re-inserts the parent and its cascade-deleted children inside
// the FK-checks-off wrapper, with the Phase-1 scope header, and reports complete.
func TestRecoverCascade_endToEnd(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	_, dbName, dsn := seedCascadeIndex(t)
	out := t.TempDir() + "/cascade.sql"

	// Reset command globals to a clean state for this run.
	rcIndexDSN, rcSchema, rcTable = dsn, dbName, "parent"
	rcPK, rcPKs, rcSince, rcUntil = "", nil, "", ""
	rcOutput, rcDryRun, rcFormat = out, false, "text"
	rcLookback, rcMaxDepth, rcLimit, rcAllowIncomplete = "30d", 5, 1000, false

	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	for _, want := range []string{
		"SET FOREIGN_KEY_CHECKS=0;",
		"SET FOREIGN_KEY_CHECKS=1;",
		"Phase-1",
		"`" + dbName + "`.`parent`", // parent re-insert
		"`" + dbName + "`.`child`",  // child re-inserts
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("output missing %q\n---\n%s", want, sql)
		}
	}
	// Both children must be re-inserted.
	if c := strings.Count(sql, "`"+dbName+"`.`child`"); c != 2 {
		t.Errorf("want 2 child INSERTs, got %d\n---\n%s", c, sql)
	}
	// A clean cascade with no archives must NOT be flagged incomplete.
	if strings.Contains(sql, "INCOMPLETE RECOVERY") {
		t.Errorf("clean cascade should not be flagged incomplete\n---\n%s", sql)
	}

	// 0 parent deletes (a --pk that matches nothing), no archives: a legitimately
	// empty result is complete and exits 0 (the operator gets a stderr warning).
	rcPK = "999"
	if err := runCascadeCmd(t); err != nil {
		t.Errorf("0 matched parents with no archives must exit 0, got: %v", err)
	}
}

// addArchiveRow makes ResolveArchiveSources return non-empty (no disk
// dependency) by registering an S3-located archived partition.
func addArchiveRow(t *testing.T, db *sql.DB) {
	t.Helper()
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(bintrail_id, partition_name, s3_bucket, s3_key)
		VALUES ('bt', 'p_2026010100', 'bucket', 'bintrail_id=bt/p_2026010100/data.parquet')`)
}

// cleanCascadeFlags sets every recover-cascade global to a valid baseline for an
// integration run, so no stale value leaks in from another test.
func cleanCascadeFlags(dsn, dbName, out string) {
	rcIndexDSN, rcSchema, rcTable = dsn, dbName, "parent"
	rcPK, rcPKs, rcSince, rcUntil = "", nil, "", ""
	rcOutput, rcDryRun, rcFormat = out, false, "text"
	rcLookback, rcMaxDepth, rcLimit, rcAllowIncomplete = "30d", 5, 1000, false
	rcBaselineDir, rcBaselineS3 = "", ""
}

// TestRecoverCascade_incompleteExit proves the dangerous "nothing found" case:
// 0 live parents matched BUT the index has archived partitions (not searched) →
// flagged incomplete, exit non-zero unless --allow-incomplete; SQL still written.
func TestRecoverCascade_incompleteExit(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName, dsn := seedCascadeIndex(t)
	addArchiveRow(t, db)

	out := t.TempDir() + "/cascade.sql"
	cleanCascadeFlags(dsn, dbName, out)
	rcPK = "999" // matches no live parent → the "nothing found, but archives exist" trap

	rcAllowIncomplete = false
	err := runCascadeCmd(t)
	if err == nil {
		t.Fatal("expected a non-nil error when incomplete and --allow-incomplete is false")
	}
	if !strings.Contains(err.Error(), "INCOMPLETE") {
		t.Errorf("error should explain incompleteness, got: %v", err)
	}
	b, rerr := os.ReadFile(out)
	if rerr != nil {
		t.Fatalf("output should still be written on incomplete: %v", rerr)
	}
	if !strings.Contains(string(b), "INCOMPLETE RECOVERY") {
		t.Errorf("output should carry the incomplete header")
	}

	rcAllowIncomplete = true
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("--allow-incomplete should exit 0, got: %v", err)
	}
}

// TestRecoverCascade_archivesWithParentsNotBlocking pins the cry-wolf fix: when
// parents ARE found, the presence of archives is a warning, NOT a hard caveat —
// so a routine archived deployment doesn't force --allow-incomplete (which would
// mask the real coverage gaps). Also exercises --pk filtering (pk=1 matches).
func TestRecoverCascade_archivesWithParentsNotBlocking(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName, dsn := seedCascadeIndex(t)
	addArchiveRow(t, db)

	out := t.TempDir() + "/cascade.sql"
	cleanCascadeFlags(dsn, dbName, out)
	rcPK = "1" // matches the seeded parent delete → parents found

	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("archives present but parents found must NOT block (cry-wolf), got: %v", err)
	}
	b, _ := os.ReadFile(out)
	if strings.Contains(string(b), "INCOMPLETE RECOVERY") {
		t.Errorf("found-parents + archives must not flag INCOMPLETE\n---\n%s", string(b))
	}
}

// TestRecoverCascade_jsonExitParity pins the #568 fix: --format json must honor
// the same exit contract as text — a partial result exits non-zero (the body's
// `complete:false` is on stdout, but consumers gating on exit code must see it).
func TestRecoverCascade_jsonExitParity(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName, dsn := seedCascadeIndex(t)
	addArchiveRow(t, db)

	out := t.TempDir() + "/cascade.sql"
	cleanCascadeFlags(dsn, dbName, out)
	rcFormat = "json"
	rcPK = "999" // 0 parents + archives → incomplete

	rcAllowIncomplete = false
	if err := runCascadeCmd(t); err == nil {
		t.Fatal("JSON mode must exit non-zero when incomplete, like text mode (#568)")
	}
	// --allow-incomplete suppresses the coverage-gap exit in JSON mode too.
	rcAllowIncomplete = true
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("JSON + --allow-incomplete should exit 0, got: %v", err)
	}
}

// writeChildBaseline writes a real Parquet snapshot of the `child` table at
// parquetPath using DuckDB (the same engine ReadBaselineRows queries with).
func writeChildBaseline(t *testing.T, parquetPath string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(parquetPath), 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer d.Close()
	// child 10,11 reference parent 1 (cascade victims); 12 references parent 2.
	// created_at is a TIMESTAMP so the test also verifies a DuckDB time.Time
	// non-PK value renders as a MySQL DATETIME literal in the recovery SQL.
	q := fmt.Sprintf(
		`COPY (SELECT * FROM (VALUES `+
			`(10,1,'keep10',TIMESTAMP '2026-05-01 12:00:00'),`+
			`(11,1,'keep11',TIMESTAMP '2026-05-02 13:00:00'),`+
			`(12,2,'other',TIMESTAMP '2026-05-03 14:00:00')`+
			`) AS t(id,pid,payload,created_at)) TO '%s' (FORMAT PARQUET)`,
		strings.ReplaceAll(parquetPath, "'", "''"))
	if _, err := d.Exec(q); err != nil {
		t.Fatalf("write baseline parquet: %v", err)
	}
}

// TestRecoverCascade_phase2BaselineRecoversUntouchedChild proves Phase-2
// end-to-end through the real provider: children that exist ONLY in a baseline
// snapshot (no binlog event — the gap Phase-1 misses) are recovered, scoped to
// the deleted parent, with the run reported complete.
func TestRecoverCascade_phase2BaselineRecoversUntouchedChild(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// Schema snapshot so the resolver knows child PK (id) + columns.
	snapTs := "2026-06-01 00:00:00"
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "payload", 3, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "created_at", 4, "", "datetime", "YES")

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`, dbName, dbName)

	// Parent DELETE in the binlog; the children have NO binlog events — they
	// live only in the baseline (the Phase-1 blind spot).
	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	parentTs := h.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, parentTs, nil, dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	// Baseline snapshot dated before the parent delete.
	baselineDir := t.TempDir()
	snapDir := filepath.Join(baselineDir, "2026-06-01T00-00-00Z")
	writeChildBaseline(t, filepath.Join(snapDir, dbName, "child.parquet"))
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("write success marker: %v", err)
	}

	out := filepath.Join(t.TempDir(), "cascade.sql")
	cleanCascadeFlags(testutil.IntegrationDSN(dbName), dbName, out)
	rcPK = "1"
	rcBaselineDir = baselineDir
	defer func() { rcBaselineDir = "" }()

	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	for _, want := range []string{
		"keep10", "keep11", // children recovered from the baseline
		"'2026-05-01 12:00:00", // DuckDB time.Time non-PK value → MySQL DATETIME literal
		"Phase-2 baseline fallback ACTIVE",
		"`" + dbName + "`.`parent`", // parent restored
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("output missing %q\n---\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "other") {
		t.Errorf("child 12 (pid=2, different parent) must NOT be recovered\n---\n%s", sql)
	}
	if strings.Contains(sql, "INCOMPLETE RECOVERY") {
		t.Errorf("a baseline-covered cascade must be complete\n---\n%s", sql)
	}
}

// writeCompositeChildBaseline writes a `child` snapshot with a composite PK (a,b).
func writeCompositeChildBaseline(t *testing.T, parquetPath string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(parquetPath), 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer d.Close()
	q := fmt.Sprintf(
		`COPY (SELECT * FROM (VALUES (1,2,1,'c12')) AS t(a,b,pid,payload)) TO '%s' (FORMAT PARQUET)`,
		strings.ReplaceAll(parquetPath, "'", "''"))
	if _, err := d.Exec(q); err != nil {
		t.Fatalf("write composite baseline parquet: %v", err)
	}
}

// TestRecoverCascade_phase2DedupCompositePK pins the load-bearing dedup contract:
// a child present in BOTH the binlog (touched, still referencing the parent) AND
// the baseline must be emitted EXACTLY ONCE. If the provider's composite-PK
// encoding (CanonicalizePKMap + BuildPKValues) diverged by one byte from the
// indexer's pk_values, the dedup would miss and the recovery SQL would
// double-INSERT the PK (which FOREIGN_KEY_CHECKS=0 does not suppress).
func TestRecoverCascade_phase2DedupCompositePK(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	snapTs := "2026-06-01 00:00:00"
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "a", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "b", 2, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 3, "", "int", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "payload", 4, "", "varchar", "YES")

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`, dbName, dbName)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")
	// child (a=1,b=2) is TOUCHED in the binlog (still referencing parent 1)...
	testutil.InsertEvent(t, db, "b.000001", 10, 20, childTs, nil, dbName, "child", 1 /*INSERT*/, "1|2", nil, nil, []byte(`{"a":1,"b":2,"pid":1,"payload":"c12"}`))
	testutil.InsertEvent(t, db, "b.000001", 20, 30, parentTs, nil, dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	// ...and ALSO present in the baseline (same composite PK).
	baselineDir := t.TempDir()
	snapDir := filepath.Join(baselineDir, "2026-06-01T00-00-00Z")
	writeCompositeChildBaseline(t, filepath.Join(snapDir, dbName, "child.parquet"))
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("success marker: %v", err)
	}

	out := filepath.Join(t.TempDir(), "cascade.sql")
	cleanCascadeFlags(testutil.IntegrationDSN(dbName), dbName, out)
	rcPK = "1"
	rcBaselineDir = baselineDir
	defer func() { rcBaselineDir = "" }()

	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	sql := string(b)
	if c := strings.Count(sql, "`"+dbName+"`.`child`"); c != 1 {
		t.Errorf("composite child present in binlog AND baseline must dedup to ONE INSERT, got %d\n---\n%s", c, sql)
	}
}
