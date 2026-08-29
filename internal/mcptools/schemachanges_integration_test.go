//go:build integration

package mcptools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// schemaChangeOut is the subset of the tool's JSON rows this test reads.
type schemaChangeOut struct {
	ID        int64  `json:"id"`
	BinlogPos int64  `json:"binlog_pos"`
	DDLType   string `json:"ddl_type"`
}

// callSchemaChanges runs the real handler and decodes the JSON array that
// precedes the "N schema change(s)" trailer.
func callSchemaChanges(t *testing.T, cfg Config, args SchemaChangesArgs) []schemaChangeOut {
	t.Helper()
	res, _, err := MakeSchemaChangesTool(cfg)(context.Background(), &mcp.CallToolRequest{}, args)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool refused: %s", resultText(res))
	}
	text := resultText(res)
	body, _, _ := strings.Cut(text, "\n\n")
	var rows []schemaChangeOut
	if err := json.Unmarshal([]byte(body), &rows); err != nil {
		t.Fatalf("decode tool output: %v\n%s", err, text)
	}
	return rows
}

func positions(rows []schemaChangeOut) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r.BinlogPos
	}
	return out
}

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestIntegrationSchemaChangesTool_sameSecondOrder pins #1441: DDLs that
// share one detected_at second must come back newest-first by binlog
// coordinate, and a limit that cuts inside the group must keep the same rows
// on every call. The rows are written through indexer.InsertSchemaChange, the
// production writer, oldest first, so the storage order is the oldest-first
// order the bare `ORDER BY detected_at DESC` was observed to walk.
func TestIntegrationSchemaChangesTool_sameSecondOrder(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	burst := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)
	earlier := burst.Add(-time.Second)

	// One DDL in the second before the burst, then a migration's four
	// statements inside one second, in the order the binlog carries them.
	seed := []event.Event{
		{Timestamp: earlier, BinlogFile: "binlog.000007", EndPos: 50,
			Schema: "shop", Table: "orders", DDLType: event.DDLAlterTable,
			DDLQuery: "ALTER TABLE orders ADD COLUMN note TEXT"},
		{Timestamp: burst, BinlogFile: "binlog.000007", EndPos: 100,
			Schema: "shop", Table: "orders_new", DDLType: event.DDLCreateTable,
			DDLQuery: "CREATE TABLE orders_new (id INT PRIMARY KEY)"},
		{Timestamp: burst, BinlogFile: "binlog.000007", EndPos: 200,
			Schema: "shop", Table: "orders", DDLType: event.DDLDropTable,
			DDLQuery: "DROP TABLE orders"},
		{Timestamp: burst, BinlogFile: "binlog.000007", EndPos: 300,
			Schema: "shop", Table: "orders_new", DDLType: event.DDLRenameTable,
			DDLQuery: "RENAME TABLE orders_new TO orders"},
		{Timestamp: burst, BinlogFile: "binlog.000007", EndPos: 400,
			Schema: "shop", Table: "orders", DDLType: event.DDLAlterTable,
			DDLQuery: "ALTER TABLE orders ADD INDEX (note(32))"},
	}
	for _, ev := range seed {
		if err := indexer.InsertSchemaChange(db, ev, nil); err != nil {
			t.Fatalf("InsertSchemaChange pos %d: %v", ev.EndPos, err)
		}
	}

	cfg := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, NoArchive: true}, nil
		},
	}

	wantAll := []int64{400, 300, 200, 100, 50}

	// The filter variants steer the optimizer down different plans (index
	// walk vs. filesort); the order must not depend on which one it picks.
	variants := map[string]SchemaChangesArgs{
		"unfiltered": {},
		"schema":     {Schema: "shop"},
		"since":      {Since: earlier.Format("2006-01-02 15:04:05")},
	}
	for name, args := range variants {
		got := positions(callSchemaChanges(t, cfg, args))
		if !equalInt64s(got, wantAll) {
			t.Errorf("%s: newest-first by binlog coordinate broken: got positions %v, want %v", name, got, wantAll)
		}
	}

	// A limit that cuts inside the same-second group must keep the newest
	// rows, and the same rows on a second call.
	first := callSchemaChanges(t, cfg, SchemaChangesArgs{Limit: 2})
	second := callSchemaChanges(t, cfg, SchemaChangesArgs{Limit: 2})
	if got := positions(first); !equalInt64s(got, wantAll[:2]) {
		t.Errorf("limit 2 must keep the two newest same-second rows: got positions %v, want %v", got, wantAll[:2])
	}
	if !equalInt64s(positions(first), positions(second)) {
		t.Errorf("limit 2 is not repeatable: first call %v, second call %v", positions(first), positions(second))
	}
	for i := range first {
		if first[i].ID != second[i].ID {
			t.Errorf("limit 2 returned different rows across calls at index %d: id %d vs %d", i, first[i].ID, second[i].ID)
		}
	}
}
