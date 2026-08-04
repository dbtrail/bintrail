//go:build integration

package mcptools

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestQueryHashSeam_MCP is the MCP half of the wiring gap: the unit tests here
// only reach refusal paths, all of which return BEFORE `opts.QueryHash =
// queryHash` runs. Deleting that assignment leaves them green while an agent
// asking for one statement's events receives the entire window and summarises
// it as that statement's work — the operator never sees the query, only the
// agent's conclusion, so there is nothing to be suspicious of.
func TestQueryHashSeam_MCP(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	target := "UPDATE app.orders SET status = 'shipped' WHERE id = 1"
	targetDigest := mcpStatementDigest(t, db, target)
	otherDigest := mcpStatementDigest(t, db, "DELETE FROM app.orders WHERE id = 9")

	ts := "2026-06-01 12:00:00"
	insertMCPStatementEvent(t, db, ts, 100, "1", target, targetDigest)
	insertMCPStatementEvent(t, db, ts, 200, "2", target, targetDigest)
	insertMCPStatementEvent(t, db, ts, 300, "9", "DELETE FROM app.orders WHERE id = 9", otherDigest)
	testutil.InsertEvent(t, db, "bin.000001", 400, 500, ts, nil, "app", "orders", 1, "42", nil, nil, []byte(`{"id":42}`))

	// A surface that does NOT withhold statement text — the standalone
	// bintrail-mcp posture. The console's is covered by the refusal unit test.
	cfg := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, NoArchive: true}, nil
		},
	}
	res, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{}, QueryArgs{
		QueryHash: targetDigest,
		Format:    "json",
	})
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if res.IsError {
		t.Fatalf("query refused: %s", resultText(res))
	}

	// The payload is JSON followed by an optional trailing notice; cut at the
	// closing bracket rather than assuming the notice is absent.
	body := resultText(res)
	end := strings.LastIndex(body, "]")
	if end < 0 {
		t.Fatalf("no JSON array in result: %s", body)
	}
	var rows []struct {
		PKValues  string  `json:"pk_values"`
		QueryHash *string `json:"query_hash"`
	}
	if err := json.Unmarshal([]byte(body[:end+1]), &rows); err != nil {
		t.Fatalf("unmarshal %q: %v", body[:end+1], err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 — the digest filter is not reaching the engine (result: %s)", len(rows), body)
	}
	for _, r := range rows {
		if r.QueryHash == nil || *r.QueryHash != targetDigest {
			t.Errorf("row pk=%s carries query_hash %v, want %q", r.PKValues, r.QueryHash, targetDigest)
		}
	}
}

func mcpStatementDigest(t *testing.T, db *sql.DB, stmt string) string {
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

func insertMCPStatementEvent(t *testing.T, db *sql.DB, ts string, pos uint64, pk, stmt, digest string) {
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
