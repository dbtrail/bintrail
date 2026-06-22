//go:build integration

package cli

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

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
}

// TestRecoverCascade_incompleteExit proves that a detectable coverage gap (here:
// the index has archived partitions the live-only search can't see) flags the
// output incomplete and makes the command exit non-zero unless --allow-incomplete.
func TestRecoverCascade_incompleteExit(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName, dsn := seedCascadeIndex(t)

	// An archive_state row with an S3 location makes ResolveArchiveSources
	// return non-empty (no disk dependency), tripping the live-only caveat.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(bintrail_id, partition_name, s3_bucket, s3_key)
		VALUES ('bt', 'p_2026010100', 'bucket', 'bintrail_id=bt/p_2026010100/data.parquet')`)

	out := t.TempDir() + "/cascade.sql"
	rcIndexDSN, rcSchema, rcTable = dsn, dbName, "parent"
	rcPK, rcPKs, rcSince, rcUntil = "", nil, "", ""
	rcOutput, rcDryRun, rcFormat = out, false, "text"
	rcLookback, rcMaxDepth, rcLimit = "30d", 5, 1000

	// Without --allow-incomplete: SQL is still written, but the command errors.
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

	// With --allow-incomplete: same output, but exit 0.
	rcAllowIncomplete = true
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("--allow-incomplete should exit 0, got: %v", err)
	}
}
