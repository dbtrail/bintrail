//go:build integration

package query

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestFetch_queryHashFilter runs the statement-digest filter against a real
// MySQL index. The unit test pins the SQL string; this pins the behaviour that
// string is supposed to produce, including the property the whole filter exists
// for: one digest covers several EXECUTIONS of the same statement shape, and
// nothing else.
//
// The digests are computed by the server via STATEMENT_DIGEST(), the same
// function the indexer uses (#699) — hardcoding a hex constant here would test
// the test's arithmetic rather than MySQL's.
func TestFetch_queryHashFilter(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	// query_text / query_hash arrived after the initial schema; EnsureSchema is
	// what a real index gets on every command startup.
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	stmtA1 := "UPDATE mydb.orders SET status = 'shipped' WHERE id = 1"
	stmtA2 := "UPDATE mydb.orders SET status = 'shipped' WHERE id = 999" // same SHAPE
	stmtB := "DELETE FROM mydb.orders WHERE id = 7"

	digestA := statementDigest(t, db, stmtA1)
	if digestA != statementDigest(t, db, stmtA2) {
		t.Fatal("premise broken: two executions of one statement shape must share a digest")
	}
	digestB := statementDigest(t, db, stmtB)

	ts := "2026-02-19 14:00:00"
	insertWithStatement(t, db, ts, 100, "mydb", "orders", "1", stmtA1, digestA)
	insertWithStatement(t, db, ts, 200, "mydb", "orders", "999", stmtA2, digestA)
	// Same statement text, a DIFFERENT table: a multi-table statement stamps
	// every table it touched, and the filter must follow it there.
	insertWithStatement(t, db, ts, 300, "mydb", "order_lines", "1", stmtA1, digestA)
	insertWithStatement(t, db, ts, 400, "mydb", "orders", "7", stmtB, digestB)
	// Captured while binlog_rows_query_log_events was OFF: no text, no digest.
	testutil.InsertEvent(t, db, "binlog.000001", 500, 600, ts, nil, "mydb", "orders", 1, "42", nil, nil, []byte(`{"id":42}`))

	e := New(db)

	rows, err := e.Fetch(context.Background(), Options{QueryHash: digestA, Limit: 100})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3 (both executions of the shape, across both tables)", len(rows))
	}
	tables := map[string]int{}
	for _, r := range rows {
		if r.QueryHash == nil || *r.QueryHash != digestA {
			t.Fatalf("row %d came back with query_hash %v, want %q", r.EventID, r.QueryHash, digestA)
		}
		tables[r.TableName]++
	}
	if tables["orders"] != 2 || tables["order_lines"] != 1 {
		t.Errorf("table spread = %v, want 2 orders + 1 order_lines", tables)
	}

	// Case is the trap: MySQL's default collation compares CHAR(64)
	// case-insensitively, DuckDB does not, so the engines only agree on a
	// canonicalised digest. Passing the un-normalised form must not change the
	// answer here either.
	upper, err := e.Fetch(context.Background(), Options{QueryHash: strings.ToUpper(digestA), Limit: 100})
	if err != nil {
		t.Fatalf("Fetch (uppercase digest): %v", err)
	}
	if len(upper) != 3 {
		t.Errorf("rows for the uppercase digest = %d, want 3", len(upper))
	}

	// The other statement's events, and the events captured without statement
	// logging, must not leak in.
	other, err := e.Fetch(context.Background(), Options{QueryHash: digestB, Limit: 100})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(other) != 1 || other[0].PKValues != "7" {
		t.Fatalf("second digest returned %d rows, want exactly the DELETE", len(other))
	}

	// Under a policy the digest is blanked on every returned row, so filtering
	// on it is refused rather than answered.
	if _, err := e.Fetch(context.Background(), Options{QueryHash: digestA, ProfileActive: true, Limit: 100}); !errors.Is(err, ErrQueryHashUnderProfile) {
		t.Errorf("err = %v, want ErrQueryHashUnderProfile", err)
	}
}

func statementDigest(t *testing.T, db *sql.DB, stmt string) string {
	t.Helper()
	var d sql.NullString
	if err := db.QueryRow("SELECT STATEMENT_DIGEST(?)", stmt).Scan(&d); err != nil {
		t.Fatalf("STATEMENT_DIGEST(%q): %v", stmt, err)
	}
	if !d.Valid || len(d.String) != 64 {
		t.Fatalf("STATEMENT_DIGEST(%q) = %v, want a 64-char digest", stmt, d)
	}
	return d.String
}

func insertWithStatement(t *testing.T, db *sql.DB, ts string, pos uint64, schema, table, pk, stmt, digest string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp,
		 schema_name, table_name, event_type, pk_values, row_after, query_text, query_hash)
		VALUES ('binlog.000001', ?, ?, ?, ?, ?, 2, ?, ?, ?, ?)`,
		pos, pos+100, ts, schema, table, pk, `{"id":1}`, stmt, digest)
	if err != nil {
		t.Fatalf("insert event: %v", err)
	}
}
