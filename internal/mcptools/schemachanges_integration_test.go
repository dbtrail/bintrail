//go:build integration

package mcptools

import (
	"context"
	"encoding/json"
	"slices"
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

func ids(rows []schemaChangeOut) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

func positions(rows []schemaChangeOut) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r.BinlogPos
	}
	return out
}

// TestIntegrationSchemaChangesTool_sameSecondOrder pins #1441: DDLs that
// share one detected_at second must come back newest-first by binlog
// coordinate (binlog_file, then binlog_pos), with id as the deterministic
// tail. The rows are written through indexer.InsertSchemaChange, the
// production writer, and deliberately NOT in coordinate order: insertion
// order (= id order) must disagree with binlog order, or a sort on id alone
// would pass for the wrong reason. The fixture also carries one row in a
// LATER file with a LOWER position (so binlog_file must outrank binlog_pos)
// and one duplicate-coordinate pair inserted last, the multi-source shape
// the tool's caveat describes, which only the id tail can order.
func TestIntegrationSchemaChangesTool_sameSecondOrder(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	burst := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)
	earlier := burst.Add(-time.Second)

	// Insertion order → id 1..6. Coordinates on purpose out of order.
	seed := []event.Event{
		// id 1: the second before the burst.
		{Timestamp: earlier, BinlogFile: "binlog.000007", EndPos: 50,
			Schema: "shop", Table: "orders", DDLType: event.DDLAlterTable,
			DDLQuery: "ALTER TABLE orders ADD COLUMN note TEXT"},
		// id 2: burst, file 7, the highest position in that file.
		{Timestamp: burst, BinlogFile: "binlog.000007", EndPos: 300,
			Schema: "shop", Table: "orders_new", DDLType: event.DDLRenameTable,
			DDLQuery: "RENAME TABLE orders_new TO orders"},
		// id 3: burst, file 7, the lowest position.
		{Timestamp: burst, BinlogFile: "binlog.000007", EndPos: 100,
			Schema: "shop", Table: "orders_new", DDLType: event.DDLCreateTable,
			DDLQuery: "CREATE TABLE orders_new (id INT PRIMARY KEY)"},
		// id 4: burst, LATER file, LOW position: newer than every file-7 row.
		{Timestamp: burst, BinlogFile: "binlog.000008", EndPos: 20,
			Schema: "shop", Table: "orders", DDLType: event.DDLAlterTable,
			DDLQuery: "ALTER TABLE orders ADD INDEX (note(32))"},
		// id 5: burst, file 7, the middle position.
		{Timestamp: burst, BinlogFile: "binlog.000007", EndPos: 200,
			Schema: "shop", Table: "orders", DDLType: event.DDLDropTable,
			DDLQuery: "DROP TABLE orders"},
		// id 6: the same coordinate as id 4 (a second source's file 8):
		// only the id tail separates the two.
		{Timestamp: burst, BinlogFile: "binlog.000008", EndPos: 20,
			Schema: "crm", Table: "leads", DDLType: event.DDLTruncateTable,
			DDLQuery: "TRUNCATE TABLE leads"},
	}
	for _, ev := range seed {
		if err := indexer.InsertSchemaChange(db, ev, nil); err != nil {
			t.Fatalf("InsertSchemaChange %s@%d: %v", ev.BinlogFile, ev.EndPos, err)
		}
	}

	cfg := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, NoArchive: true}, nil
		},
	}

	// Burst first (detected_at DESC), file 8 before file 7 (binlog_file
	// DESC), the file-8 duplicate pair by id DESC, file 7 by binlog_pos DESC,
	// then the earlier second.
	wantIDs := []int64{6, 4, 2, 5, 3, 1}
	wantPos := []int64{20, 20, 300, 200, 100, 50}

	// The filter variants steer the optimizer down different plans, and the
	// order must not depend on which one it picks. On the unfixed query the
	// unfiltered and schema-filtered shapes filesorted and came back oldest
	// first; the since-filtered shape walked the detected_at index backward
	// and was right by accident, so it is here to pin that plan too, not as
	// a duplicate of the others.
	variants := map[string]SchemaChangesArgs{
		"unfiltered": {},
		"schema":     {Schema: "shop"},
		"since":      {Since: earlier.Format("2006-01-02 15:04:05")},
	}
	for name, args := range variants {
		rows := callSchemaChanges(t, cfg, args)
		want, wantP := wantIDs, wantPos
		if name == "schema" {
			// The crm row (id 6) is filtered out; the rest keep their order.
			want, wantP = []int64{4, 2, 5, 3, 1}, []int64{20, 300, 200, 100, 50}
		}
		if got := ids(rows); !slices.Equal(got, want) {
			t.Errorf("%s: newest-first by binlog coordinate broken: got ids %v, want %v", name, got, want)
		}
		if got := positions(rows); !slices.Equal(got, wantP) {
			t.Errorf("%s: got positions %v, want %v", name, got, wantP)
		}
	}

	// A limit of 1 cuts between the two duplicate-coordinate rows, where
	// detected_at, binlog_file and binlog_pos all tie: only the id tail
	// decides which one is kept, so this is the id DESC guard. The second
	// call checks the cut is repeatable; it does not by itself prove a
	// tiebreak (an arbitrary but stable plan would also repeat), the exact
	// id assertion is what does.
	first := callSchemaChanges(t, cfg, SchemaChangesArgs{Limit: 1})
	second := callSchemaChanges(t, cfg, SchemaChangesArgs{Limit: 1})
	if got := ids(first); !slices.Equal(got, wantIDs[:1]) {
		t.Errorf("limit 1 must keep the higher id of the duplicate-coordinate pair: got ids %v, want %v", got, wantIDs[:1])
	}
	if !slices.Equal(ids(first), ids(second)) {
		t.Errorf("limit 1 is not repeatable: first call %v, second call %v", ids(first), ids(second))
	}
}
