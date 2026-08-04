//go:build integration

package cli

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestQueryHashSeam_CLI closes the gap every unit test in this feature leaves
// open: they all build query.Options by hand or assert a refusal, so NONE of
// them touches the one line that connects --query-hash to the engine
// (`QueryHash: queryHash` in runQuery's Options literal). Deleting that line
// keeps the entire suite green while `bintrail query --query-hash <digest>`
// returns EVERY event in the window — an unfiltered answer presented under the
// documented promise "everything that statement did". A silent over-return on a
// forensic question is worse than the empty-result failures the rest of this
// feature is written against: there is no missing output to be suspicious of.
//
// So this asserts through the real command entry point, on a real index, on the
// count: 2 of 4 seeded events.
func TestQueryHashSeam_CLI(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	target := "UPDATE app.orders SET status = 'shipped' WHERE id = 1"
	other := "DELETE FROM app.orders WHERE id = 9"
	targetDigest := cliStatementDigest(t, db, target)
	otherDigest := cliStatementDigest(t, db, other)

	ts := "2026-06-01 12:00:00"
	insertCLIStatementEvent(t, db, ts, 100, "1", target, targetDigest)
	insertCLIStatementEvent(t, db, ts, 200, "2", target, targetDigest)
	insertCLIStatementEvent(t, db, ts, 300, "9", other, otherDigest)
	// Captured while the source was not logging statements: no text, no digest.
	testutil.InsertEvent(t, db, "bin.000001", 400, 500, ts, nil, "app", "orders", 1, "42", nil, nil, []byte(`{"id":42}`))

	resetQueryGlobals(t)
	qIndexDSN, qFormat, qNoArchive = testutil.IntegrationDSN(dbName), "json", true
	qQueryHash = targetDigest

	var runErr error
	out := captureStdout(t, func() { runErr = runQuery(newQueryTestCmd(), nil) })
	if runErr != nil {
		t.Fatalf("runQuery: %v", runErr)
	}

	var rows []struct {
		PKValues  string  `json:"pk_values"`
		QueryHash *string `json:"query_hash"`
	}
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		t.Fatalf("unmarshal %q: %v", out, err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 — the filter is not reaching the engine (output: %s)", len(rows), out)
	}
	for _, r := range rows {
		if r.QueryHash == nil || *r.QueryHash != targetDigest {
			t.Errorf("row pk=%s carries query_hash %v, want %q", r.PKValues, r.QueryHash, targetDigest)
		}
	}
}

func cliStatementDigest(t *testing.T, db *sql.DB, stmt string) string {
	t.Helper()
	var d sql.NullString
	if err := db.QueryRow("SELECT STATEMENT_DIGEST(?)", stmt).Scan(&d); err != nil {
		t.Fatalf("STATEMENT_DIGEST: %v", err)
	}
	if !d.Valid || len(d.String) != 64 {
		t.Fatalf("STATEMENT_DIGEST(%q) = %v, want a 64-char digest", stmt, d)
	}
	return d.String
}

func insertCLIStatementEvent(t *testing.T, db *sql.DB, ts string, pos uint64, pk, stmt, digest string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp,
		 schema_name, table_name, event_type, pk_values, row_after, query_text, query_hash)
		VALUES ('bin.000001', ?, ?, ?, 'app', 'orders', 2, ?, ?, ?, ?)`,
		pos, pos+100, ts, pk, `{"id":1}`, stmt, digest)
	if err != nil {
		t.Fatalf("insert event: %v", err)
	}
}
