//go:build integration

package indexer

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

func TestCreateBinlogEventsTable(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	if err := createBinlogEventsTable(db, 3, false); err != nil {
		t.Fatalf("createBinlogEventsTable failed: %v", err)
	}

	// Verify the table has 4 partitions (3 hourly + p_future).
	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'`,
		dbName).Scan(&count); err != nil {
		t.Fatalf("query partitions failed: %v", err)
	}
	if count != 4 {
		t.Errorf("expected 4 partitions, got %d", count)
	}
}

// TestEnsureSchemaAddsFlavorColumn pins the MariaDB-source migration: a
// pre-flavor install must gain stream_state.flavor as NOT NULL DEFAULT 'mysql'
// (so existing rows read back as mysql with no data migration), and the
// migration must be idempotent.
func TestEnsureSchemaAddsFlavorColumn(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Simulate a pre-flavor install by dropping the column EnsureSchema re-adds.
	testutil.MustExec(t, db, `ALTER TABLE stream_state DROP COLUMN flavor`)

	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	var colDefault, isNullable string
	if err := db.QueryRow(`SELECT COLUMN_DEFAULT, IS_NULLABLE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'stream_state'
		  AND COLUMN_NAME = 'flavor'`).Scan(&colDefault, &isNullable); err != nil {
		t.Fatalf("read flavor column: %v", err)
	}
	if colDefault != "mysql" {
		t.Errorf("flavor COLUMN_DEFAULT = %q, want \"mysql\"", colDefault)
	}
	if isNullable != "NO" {
		t.Errorf("flavor IS_NULLABLE = %q, want \"NO\"", isNullable)
	}

	// Idempotent: a second run must not error or re-add.
	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema (second run): %v", err)
	}
}

// TestEnsureSchemaAddsFKRuleColumns pins the cascade-recovery migration: a
// pre-cascade install must gain fk_constraints.delete_rule/update_rule as
// NOT NULL DEFAULT ” (existing rows backfill to "" = unknown), idempotently.
func TestEnsureSchemaAddsFKRuleColumns(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Simulate a pre-cascade install: drop the columns + seed an old-shape row.
	testutil.MustExec(t, db, `ALTER TABLE fk_constraints DROP COLUMN delete_rule, DROP COLUMN update_rule`)
	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name,
		 ordinal_position, referenced_schema_name, referenced_table_name, referenced_column_name)
		VALUES (1,'fk','app','child','pid',1,'app','parent','id')`)

	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	for _, col := range []string{"delete_rule", "update_rule"} {
		var def, nullable string
		if err := db.QueryRow(`SELECT COLUMN_DEFAULT, IS_NULLABLE FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'fk_constraints' AND COLUMN_NAME = ?`,
			col).Scan(&def, &nullable); err != nil {
			t.Fatalf("read %s column: %v", col, err)
		}
		if def != "" {
			t.Errorf("%s COLUMN_DEFAULT = %q, want \"\"", col, def)
		}
		if nullable != "NO" {
			t.Errorf("%s IS_NULLABLE = %q, want \"NO\"", col, nullable)
		}
	}

	// The pre-existing row must backfill to '' (unknown), not NULL.
	var del, upd string
	if err := db.QueryRow(`SELECT delete_rule, update_rule FROM fk_constraints WHERE constraint_name='fk'`).
		Scan(&del, &upd); err != nil {
		t.Fatalf("read backfilled row: %v", err)
	}
	if del != "" || upd != "" {
		t.Errorf("backfilled rules = (%q,%q), want empty", del, upd)
	}

	// Idempotent: a second run must not error or re-add.
	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema (second run): %v", err)
	}
}

// TestEnsureSchemaToleratesMissingFKConstraints guards the regression where the
// cascade-recovery migration would break EnsureSchema on very old indexes that
// predate the fk_constraints table (TakeSnapshot already tolerates its absence).
func TestEnsureSchemaToleratesMissingFKConstraints(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, `DROP TABLE fk_constraints`)
	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema must tolerate a missing fk_constraints table, got: %v", err)
	}
}

// TestEnsureSchemaWidensColumnType pins the #472 migration: #212 created
// schema_snapshots.column_type as VARCHAR(128), which a realistic ENUM
// declaration exceeds — under strict mode the 1406 aborts the whole
// snapshot transaction. EnsureSchema must widen pre-existing installs to
// TEXT while preserving stored values, and stay idempotent.
func TestEnsureSchemaWidensColumnType(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Downgrade to the #212-era shape an existing install would have.
	testutil.MustExec(t, db,
		`ALTER TABLE schema_snapshots MODIFY COLUMN column_type VARCHAR(128) NOT NULL DEFAULT ''`)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, column_type, is_nullable)
		VALUES (1, NOW(), 'app', 't', 'created_at', 1, '', 'datetime', 'datetime(6)', 'NO')`)

	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	var dataType string
	if err := db.QueryRow(`SELECT DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'schema_snapshots'
		  AND COLUMN_NAME = 'column_type'`).Scan(&dataType); err != nil {
		t.Fatalf("read column_type DATA_TYPE: %v", err)
	}
	if dataType != "text" {
		t.Fatalf("column_type DATA_TYPE = %q after EnsureSchema, want \"text\"", dataType)
	}

	// Pre-existing value preserved through the MODIFY.
	var preserved string
	if err := db.QueryRow(
		`SELECT column_type FROM schema_snapshots WHERE column_name = 'created_at'`,
	).Scan(&preserved); err != nil {
		t.Fatalf("read preserved value: %v", err)
	}
	if preserved != "datetime(6)" {
		t.Errorf("pre-migration value = %q, want \"datetime(6)\"", preserved)
	}

	// The widened column accepts a >128-char declaration — the exact
	// insert that aborted snapshots before.
	longEnum := "enum('pending_payment','payment_confirmed','awaiting_fulfillment','partially_shipped','shipped','out_for_delivery','delivered','return_requested','refund_processed','cancelled_by_customer')"
	if len(longEnum) <= 128 {
		t.Fatalf("fixture regression: longEnum is %d chars", len(longEnum))
	}
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, column_type, is_nullable)
		VALUES (1, NOW(), 'app', 't', 'status', 2, '', 'enum', ?, 'NO')`, longEnum)

	// Idempotent: a second run must not error or re-alter.
	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema (second run): %v", err)
	}
}
