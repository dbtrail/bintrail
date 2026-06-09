//go:build integration

package metadata

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/dbtrail/bintrail/internal/testutil"
)

func TestTakeSnapshot_nonInnoDB(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY,
		status VARCHAR(20)
	) ENGINE=MyISAM`)

	_, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err == nil {
		t.Fatal("expected validation error for non-InnoDB table, got nil")
	}
	if !strings.Contains(err.Error(), "not using InnoDB") {
		t.Errorf("expected 'not using InnoDB' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), sourceName+".orders") {
		t.Errorf("expected table name in error, got: %v", err)
	}
}

func TestTakeSnapshot_noPK(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE events (
		name VARCHAR(100),
		value INT
	) ENGINE=InnoDB`)

	_, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err == nil {
		t.Fatal("expected validation error for table without primary key, got nil")
	}
	if !strings.Contains(err.Error(), "without a primary key") {
		t.Errorf("expected 'without a primary key' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), sourceName+".events") {
		t.Errorf("expected table name in error, got: %v", err)
	}
}

func TestTakeSnapshot_bothViolations(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE myisam_tbl (id INT PRIMARY KEY) ENGINE=MyISAM`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE nopk_tbl (name VARCHAR(100)) ENGINE=InnoDB`)

	_, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "not using InnoDB") {
		t.Errorf("expected 'not using InnoDB' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "without a primary key") {
		t.Errorf("expected 'without a primary key' in error, got: %v", err)
	}
}

func TestTakeSnapshot_basic(t *testing.T) {
	// Create two databases: source (with a real table) and index (for snapshot storage).
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)

	// Create index tables.
	testutil.InitIndexTables(t, indexDB)

	// Create a source table.
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY,
		customer VARCHAR(100) NOT NULL,
		status VARCHAR(20) NOT NULL,
		amount DECIMAL(10,2) NOT NULL
	)`)

	stats, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	if stats.SnapshotID < 1 {
		t.Errorf("expected SnapshotID >= 1, got %d", stats.SnapshotID)
	}
	if stats.TableCount < 1 {
		t.Errorf("expected at least 1 table, got %d", stats.TableCount)
	}
	if stats.ColumnCount < 4 {
		t.Errorf("expected at least 4 columns (orders has 4), got %d", stats.ColumnCount)
	}

	// Verify rows exist in schema_snapshots.
	var count int
	indexDB.QueryRow("SELECT COUNT(*) FROM schema_snapshots WHERE snapshot_id = ?", stats.SnapshotID).Scan(&count)
	if count < 4 {
		t.Errorf("expected at least 4 snapshot rows, got %d", count)
	}
}

func TestTakeSnapshot_filteredSchemas(t *testing.T) {
	// Two source DBs but only snapshot one.
	sourceDB1, name1 := testutil.CreateTestDB(t)
	sourceDB2, name2 := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB1, "CREATE TABLE tbl1 (id INT PRIMARY KEY)")
	testutil.MustExec(t, sourceDB2, "CREATE TABLE tbl2 (id INT PRIMARY KEY)")

	// Only snapshot name1.
	stats, err := TakeSnapshot(sourceDB1, indexDB, []string{name1})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	// Verify name2's table is not in the snapshot.
	var count int
	indexDB.QueryRow("SELECT COUNT(*) FROM schema_snapshots WHERE snapshot_id = ? AND schema_name = ?",
		stats.SnapshotID, name2).Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 rows for filtered schema %q, got %d", name2, count)
	}
}

func TestNewResolver_latestSnapshot(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Insert two snapshots manually.
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "status", 3, "", "varchar", "NO")

	// NewResolver(db, 0) should load the latest (snapshot 2 with 3 columns).
	resolver, err := NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver(db, 0) failed: %v", err)
	}
	if resolver.SnapshotID() != 2 {
		t.Errorf("expected snapshot_id=2, got %d", resolver.SnapshotID())
	}

	tm, err := resolver.Resolve("mydb", "orders")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(tm.Columns) != 3 {
		t.Errorf("expected 3 columns in snapshot 2, got %d", len(tm.Columns))
	}
}

func TestNewResolver_specificSnapshot(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "status", 3, "", "varchar", "NO")

	// Load specific snapshot 1 (2 columns only).
	resolver, err := NewResolver(indexDB, 1)
	if err != nil {
		t.Fatalf("NewResolver(db, 1) failed: %v", err)
	}
	if resolver.SnapshotID() != 1 {
		t.Errorf("expected snapshot_id=1, got %d", resolver.SnapshotID())
	}

	tm, err := resolver.Resolve("mydb", "orders")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(tm.Columns) != 2 {
		t.Errorf("expected 2 columns in snapshot 1, got %d", len(tm.Columns))
	}
}

func TestNewResolver_emptyTable(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// No snapshots inserted — should error.
	_, err := NewResolver(indexDB, 0)
	if err == nil {
		t.Error("expected error for empty schema_snapshots, got nil")
	}
}

func TestResolver_pkColumns(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Composite PK: (order_id, item_id).
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-19 10:00:00", "mydb", "order_items", "order_id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-19 10:00:00", "mydb", "order_items", "item_id", 2, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-19 10:00:00", "mydb", "order_items", "quantity", 3, "", "int", "NO")

	resolver, err := NewResolver(indexDB, 1)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	tm, err := resolver.Resolve("mydb", "order_items")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	pkCols := tm.PKColumnMetas()
	if len(pkCols) != 2 {
		t.Fatalf("expected 2 PK columns, got %d", len(pkCols))
	}
	if pkCols[0].Name != "order_id" || pkCols[1].Name != "item_id" {
		t.Errorf("expected PK columns [order_id, item_id], got [%s, %s]", pkCols[0].Name, pkCols[1].Name)
	}
}

// TestTakeSnapshot_columnType is the #212 regression test for the schema-
// snapshot side of the precision-aware PK canonicalizer. TakeSnapshot must
// read `information_schema.COLUMNS.COLUMN_TYPE` and store the full type
// (e.g. "datetime(6)") in `schema_snapshots.column_type`, so the reconstruct
// canonicalizer can parse the declared fractional precision.
//
// Before this fix, schema_snapshots only had `data_type` (base type like
// "datetime"), and the canonicalizer had no way to tell DATETIME(0) from
// DATETIME(6). This test proves TakeSnapshot actually captures the precision.
func TestTakeSnapshot_columnType(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE events (
		id INT PRIMARY KEY,
		created_at DATETIME(6) NOT NULL,
		amount DECIMAL(12,4) NOT NULL,
		slug VARCHAR(64) NOT NULL
	)`)

	stats, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	// Pull column_type back out for each column and check it matches.
	cases := []struct {
		column   string
		wantType string
	}{
		{"id", "int"},
		{"created_at", "datetime(6)"},
		{"amount", "decimal(12,4)"},
		{"slug", "varchar(64)"},
	}
	for _, c := range cases {
		var got string
		err := indexDB.QueryRow(
			`SELECT column_type FROM schema_snapshots
			 WHERE snapshot_id = ? AND table_name = 'events' AND column_name = ?`,
			stats.SnapshotID, c.column,
		).Scan(&got)
		if err != nil {
			t.Errorf("query column_type for %s: %v", c.column, err)
			continue
		}
		if got != c.wantType {
			t.Errorf("column %s: got column_type=%q, want %q", c.column, got, c.wantType)
		}
	}
}

// ─── ValidateBinlogFormat ────────────────────────────────────────────────────────────

func TestValidateBinlogFormat_row(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	// Docker test container should have binlog_format=ROW.
	dsn := testutil.IntegrationDSN("")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	if err := ValidateBinlogFormat(db); err != nil {
		t.Fatalf("expected nil error for ROW binlog_format, got: %v", err)
	}
}

// ─── ValidateBinlogRowImage ────────────────────────────────────────────────────────────

func TestValidateBinlogRowImage_full(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	// Docker test container should have binlog_row_image=FULL (default).
	dsn := testutil.IntegrationDSN("")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	if err := ValidateBinlogRowImage(db); err != nil {
		t.Fatalf("expected nil error for FULL binlog_row_image, got: %v", err)
	}
}

// ─── ValidateNoFKCascades ────────────────────────────────────────────────────────────

func TestValidateNoFKCascades_none(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT,
		total DECIMAL(10,2) NOT NULL
	)`)

	if err := ValidateNoFKCascades(db, []string{dbName}); err != nil {
		t.Fatalf("expected nil error for schema with no cascades, got: %v", err)
	}
}

func TestValidateNoFKCascades_cascade(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT,
		total DECIMAL(10,2) NOT NULL
	)`)
	testutil.MustExec(t, db, `CREATE TABLE order_items (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		order_id INT NOT NULL,
		CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
	)`)

	if err := ValidateNoFKCascades(db, []string{dbName}); err == nil {
		t.Fatal("expected error for schema with FK cascade, got nil")
	}
}

func TestValidateNoFKCascades_updateCascade(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE categories (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)
	testutil.MustExec(t, db, `CREATE TABLE products (
		id          INT PRIMARY KEY AUTO_INCREMENT,
		category_id INT NOT NULL,
		CONSTRAINT fk_cat FOREIGN KEY (category_id) REFERENCES categories(id) ON UPDATE CASCADE
	)`)

	if err := ValidateNoFKCascades(db, []string{dbName}); err == nil {
		t.Fatal("expected error for schema with UPDATE CASCADE, got nil")
	}
}

func TestValidateNoFKCascades_otherSchemaIgnored(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	otherDB, otherName := testutil.CreateTestDB(t)

	// Create a cascade in otherDB.
	testutil.MustExec(t, otherDB, `CREATE TABLE parents (id INT PRIMARY KEY)`)
	testutil.MustExec(t, otherDB, `CREATE TABLE children (
		id INT PRIMARY KEY,
		parent_id INT NOT NULL,
		CONSTRAINT fk_p FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE CASCADE
	)`)

	// dbName has no cascades — checking only dbName should pass.
	_ = dbName
	if err := ValidateNoFKCascades(db, []string{dbName}); err != nil {
		t.Fatalf("expected nil when cascade is only in %q (not targeted), got: %v", otherName, err)
	}
}

// The unscoped pre-flight skips a bintrail index schema regardless of its name:
// it is recognised by its signature tables (binlog_events, schema_snapshots,
// stream_state), not by a name pattern. Here the index DB has a non-bintrail
// name (`audit_index`) yet carries the access_rules→profiles cascade — the scan
// must skip it (the #347 fix, now name-independent, closing the custom-name
// under-exclusion hole, #365). When the operator names it explicitly, it is
// still policed.
func TestValidateNoFKCascades_customNamedIndexSkippedWhenUnscoped(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)

	const idxSchema = "audit_index" // not bt_-prefixed, not the default index name
	testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `"+idxSchema+"`")
	testutil.MustExec(t, db, "CREATE DATABASE `"+idxSchema+"`")
	t.Cleanup(func() { _, _ = db.Exec("DROP DATABASE IF EXISTS `" + idxSchema + "`") })

	// The signature tables that mark the schema as a bintrail index. binlog_events
	// is RANGE-partitioned in production, so partition it here too — this confirms
	// the subquery's TABLE_TYPE = 'BASE TABLE' filter matches a partitioned table.
	testutil.MustExec(t, db, "CREATE TABLE `"+idxSchema+"`.binlog_events ("+
		"id INT, event_timestamp DATETIME NOT NULL, PRIMARY KEY (id, event_timestamp)) "+
		"PARTITION BY RANGE (TO_SECONDS(event_timestamp)) (PARTITION p_future VALUES LESS THAN MAXVALUE)")
	testutil.MustExec(t, db, "CREATE TABLE `"+idxSchema+"`.schema_snapshots (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "CREATE TABLE `"+idxSchema+"`.stream_state (id INT PRIMARY KEY)")
	// ...plus the access_rules→profiles ON DELETE CASCADE the pre-flight trips on.
	testutil.MustExec(t, db, "CREATE TABLE `"+idxSchema+"`.profiles (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "CREATE TABLE `"+idxSchema+"`.access_rules ("+
		"id INT PRIMARY KEY, profile_id INT NOT NULL, "+
		"CONSTRAINT fk_access_rules_profile FOREIGN KEY (profile_id) REFERENCES `"+idxSchema+"`.profiles(id) ON DELETE CASCADE)")

	// Unscoped: skipped because it is structurally a bintrail index, despite the
	// non-bintrail name. Assert audit_index specifically is absent from the scan
	// rather than that the whole server-wide scan is clean — that keeps the test
	// robust to unrelated cascades that may exist on a shared/dev MySQL server.
	if schemas := unscopedFKCascadeSchemas(t, db); schemas[idxSchema] {
		t.Fatalf("expected structurally-internal schema %q to be excluded from the unscoped scan, but it was flagged", idxSchema)
	}

	// Explicitly named: still policed.
	if err := ValidateNoFKCascades(db, []string{idxSchema}); err == nil {
		t.Fatalf("expected error when %q is explicitly targeted", idxSchema)
	}
}

// unscopedFKCascadeSchemas runs the unscoped FK-cascade query and returns the
// set of schemas it flags. Lets a test assert that a specific schema is (or is
// not) excluded without depending on the rest of the server being cascade-free.
func unscopedFKCascadeSchemas(t *testing.T, db *sql.DB) map[string]bool {
	t.Helper()
	q, args := buildFKCascadeQuery(nil)
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatalf("unscoped FK-cascade query: %v", err)
	}
	defer rows.Close()
	got := map[string]bool{}
	for rows.Next() {
		var schema, name, del, upd string
		if err := rows.Scan(&schema, &name, &del, &upd); err != nil {
			t.Fatalf("scan FK-cascade row: %v", err)
		}
		got[schema] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate FK-cascade rows: %v", err)
	}
	return got
}

// The inverse of the exclusion, and the load-bearing direction: an unscoped
// scan must still CATCH a real CASCADE FK in an ordinary user schema (no bt_
// prefix, not an index-DB name). This guards against a regression that
// broadens the exclusion until the unscoped branch matches nothing. testutil
// only makes bt_-prefixed DBs, so the non-internal schema is created by hand.
func TestValidateNoFKCascades_userSchemaCaughtWhenUnscoped(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)

	const userSchema = "fkcascade_user"
	testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `"+userSchema+"`")
	testutil.MustExec(t, db, "CREATE DATABASE `"+userSchema+"`")
	t.Cleanup(func() { _, _ = db.Exec("DROP DATABASE IF EXISTS `" + userSchema + "`") })

	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.parents (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.children ("+
		"id INT PRIMARY KEY, parent_id INT NOT NULL, "+
		"CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES `"+userSchema+"`.parents(id) ON DELETE CASCADE)")

	if err := ValidateNoFKCascades(db, nil); err == nil {
		t.Fatalf("expected unscoped scan to catch the CASCADE FK in non-internal schema %q, got nil", userSchema)
	}
}

// A schema with only SOME of the signature tables (here 2 of 3) is NOT a
// bintrail index — its real CASCADE must still be CAUGHT when unscoped. This
// pins the exactness of HAVING COUNT(DISTINCT TABLE_NAME) = 3: a regression to
// >= 1 (or a shorter IN-list) would silently skip this real user cascade,
// reopening the #347-class silent-skip bug. (The existing "caught" test above
// uses zero signature tables, so it would not detect such a loosening.)
func TestValidateNoFKCascades_partialSignatureCaughtWhenUnscoped(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)

	const userSchema = "fkcascade_partial"
	testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `"+userSchema+"`")
	testutil.MustExec(t, db, "CREATE DATABASE `"+userSchema+"`")
	t.Cleanup(func() { _, _ = db.Exec("DROP DATABASE IF EXISTS `" + userSchema + "`") })

	// Two of the three signature names, but not all three.
	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.binlog_events (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.stream_state (id INT PRIMARY KEY)")
	// ...plus a genuine cascade that must be caught.
	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.parents (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.children ("+
		"id INT PRIMARY KEY, parent_id INT NOT NULL, "+
		"CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES `"+userSchema+"`.parents(id) ON DELETE CASCADE)")

	if !unscopedFKCascadeSchemas(t, db)[userSchema] {
		t.Fatalf("expected partial-signature schema %q (2 of 3 signature tables) to be flagged by the unscoped scan, but it was not", userSchema)
	}
}

// ─── EnsureResolver ──────────────────────────────────────────────────────────────────

func TestEnsureResolver_autoSnapshot(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Create a table on the source.
	testutil.MustExec(t, sourceDB, `CREATE TABLE products (
		id   INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)

	resolver, err := EnsureResolver(indexDB, sourceDB, []string{sourceName})
	if err != nil {
		t.Fatalf("EnsureResolver failed: %v", err)
	}

	if resolver.SnapshotID() == 0 {
		t.Error("expected non-zero snapshot ID")
	}
	if resolver.TableCount() != 1 {
		t.Errorf("expected 1 table, got %d", resolver.TableCount())
	}
}

func TestEnsureResolver_noSnapshotNoSource(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	_, err := EnsureResolver(indexDB, nil, nil)
	if err == nil {
		t.Fatal("expected error when no snapshot and no sourceDB")
	}
}

func TestEnsureResolver_existingSnapshot(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100)
	)`)

	// Take snapshot manually first.
	testutil.InsertSnapshot(t, indexDB, 1, "2026-01-01 00:00:00",
		sourceName, "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-01-01 00:00:00",
		sourceName, "orders", "name", 2, "", "varchar", "YES")

	// Should load existing snapshot without needing sourceDB.
	resolver, err := EnsureResolver(indexDB, nil, nil)
	if err != nil {
		t.Fatalf("EnsureResolver failed: %v", err)
	}
	if resolver.SnapshotID() != 1 {
		t.Errorf("expected snapshot ID 1, got %d", resolver.SnapshotID())
	}
}
