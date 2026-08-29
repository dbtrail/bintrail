//go:build integration

package mcptools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestQueryAndRecoverTools_pkRange (#1440) is the wiring proof for pk_min/
// pk_max: the unit tests pin BuildQueryOptions, but only a real call shows
// the range reaches the SQL with the cast the snapshot chose, and that the
// shape refusal fires BEFORE any fetch on both tools.
func TestQueryAndRecoverTools_pkRange(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	testutil.MustExec(t, db, `CREATE TABLE orders (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB`)
	testutil.MustExec(t, db, `CREATE TABLE order_lines (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (a, b)) ENGINE=InnoDB`)
	if _, err := metadata.TakeSnapshot(db, db, []string{dbName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	pos := uint64(100)
	for i, k := range []string{"9", "10", "100", "18446744073709551610"} {
		testutil.InsertEvent(t, db, "bin.000001", pos, pos+50, "2026-06-01 12:00:0"+string(rune('0'+i)), nil,
			dbName, "orders", 2, k, []byte(`["v"]`), []byte(`{"id":`+k+`,"v":1}`), []byte(`{"id":`+k+`,"v":2}`))
		pos += 100
	}

	cfg := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, NoArchive: true}, nil
		},
	}
	ctx := context.Background()

	// query: min alone exposes string order ("9" >= "10" is true as text).
	res, _, err := MakeQueryTool(cfg)(ctx, &mcp.CallToolRequest{}, QueryArgs{Schema: dbName, Table: "orders", PKMin: "10"})
	if err != nil {
		t.Fatalf("query handler: %v", err)
	}
	if res.IsError {
		t.Fatalf("query refused: %s", resultText(res))
	}
	var rows []struct {
		PKValues string `json:"pk_values"`
	}
	if err := json.Unmarshal([]byte(resultText(res)), &rows); err != nil {
		t.Fatalf("query result is not JSON: %v\n%s", err, resultText(res))
	}
	var keys []string
	for _, r := range rows {
		keys = append(keys, r.PKValues)
	}
	if got := strings.Join(keys, ","); got != "10,100,18446744073709551610" {
		t.Errorf("pk_min 10 returned %s; 9 must be out and 100 in", got)
	}

	// query: pk_max must NARROW the result, alone and with pk_min.
	queryKeys := func(args QueryArgs) string {
		t.Helper()
		res, _, err := MakeQueryTool(cfg)(ctx, &mcp.CallToolRequest{}, args)
		if err != nil {
			t.Fatalf("query handler: %v", err)
		}
		if res.IsError {
			t.Fatalf("query refused: %s", resultText(res))
		}
		var rows []struct {
			PKValues string `json:"pk_values"`
		}
		if err := json.Unmarshal([]byte(resultText(res)), &rows); err != nil {
			t.Fatalf("query result is not JSON: %v\n%s", err, resultText(res))
		}
		var keys []string
		for _, r := range rows {
			keys = append(keys, r.PKValues)
		}
		return strings.Join(keys, ",")
	}
	if got := queryKeys(QueryArgs{Schema: dbName, Table: "orders", PKMin: "10", PKMax: "100"}); got != "10,100" {
		t.Errorf("pk_min 10 pk_max 100 returned %q, want 10,100", got)
	}
	if got := queryKeys(QueryArgs{Schema: dbName, Table: "orders", PKMax: "9"}); got != "9" {
		t.Errorf("pk_max 9 returned %q, want 9", got)
	}

	// query: the 64-bit unsigned key sits above 2^63, which a signed cast
	// cannot hold; the snapshot's "bigint unsigned" must pick UNSIGNED.
	res, _, _ = MakeQueryTool(cfg)(ctx, &mcp.CallToolRequest{}, QueryArgs{Schema: dbName, Table: "orders", PKMin: "9223372036854775808"})
	if res.IsError || !strings.Contains(resultText(res), "18446744073709551610") || strings.Contains(resultText(res), `"100"`) {
		t.Errorf("pk_min 2^63 on an unsigned key: %s", resultText(res))
	}

	// query: shape refusals name the table's actual key.
	res, _, _ = MakeQueryTool(cfg)(ctx, &mcp.CallToolRequest{}, QueryArgs{Schema: dbName, Table: "order_lines", PKMin: "1"})
	if !res.IsError || !strings.Contains(resultText(res), "pk_min/pk_max: range filters need a single integer primary key; this table's is (a, b)") {
		t.Errorf("composite key must refuse: %s", resultText(res))
	}
	res, _, _ = MakeQueryTool(cfg)(ctx, &mcp.CallToolRequest{}, QueryArgs{Schema: dbName, Table: "orders", PKMax: "-1"})
	if !res.IsError || !strings.Contains(resultText(res), "is negative, but the primary key column is unsigned (id bigint unsigned)") {
		t.Errorf("negative bound on an unsigned key must refuse: %s", resultText(res))
	}

	// A surface with a preloaded, STALE resolver (the console's bundle
	// resolver, opened before this table was snapshotted) must still
	// resolve: the range always reads the latest snapshot.
	stale := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, NoArchive: true,
				Resolver: metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{}), ResolverLoaded: true}, nil
		},
	}
	res, _, _ = MakeQueryTool(stale)(ctx, &mcp.CallToolRequest{}, QueryArgs{Schema: dbName, Table: "orders", PKMin: "100"})
	if res.IsError || !strings.Contains(resultText(res), `"100"`) {
		t.Errorf("a stale preloaded resolver must not decide the range: %s", resultText(res))
	}

	// A denied table is refused without its key shape being described.
	denied := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, NoArchive: true,
				DenyTables: []query.SchemaTable{{Schema: dbName, Table: "order_lines"}}, ProfileActive: true}, nil
		},
	}
	res, _, _ = MakeQueryTool(denied)(ctx, &mcp.CallToolRequest{}, QueryArgs{Schema: dbName, Table: "order_lines", PKMin: "1"})
	if !res.IsError || !strings.Contains(resultText(res), "does not allow reading") || strings.Contains(resultText(res), "(a, b)") {
		t.Errorf("denied table must refuse without describing its key: %s", resultText(res))
	}

	// recover: the range scopes the reversal; keys outside it are untouched.
	rres, _, err := MakeRecoverTool(cfg)(ctx, &mcp.CallToolRequest{}, RecoverArgs{Schema: dbName, Table: "orders", PKMin: "10", PKMax: "100"})
	if err != nil {
		t.Fatalf("recover handler: %v", err)
	}
	if rres.IsError {
		t.Fatalf("recover refused: %s", resultText(rres))
	}
	script := resultText(rres)
	for _, want := range []string{"pk=10 ", "pk=100 "} {
		if !strings.Contains(script, want) {
			t.Errorf("script lacks %q:\n%s", want, script)
		}
	}
	for _, leak := range []string{"pk=9 ", "pk=18446744073709551610 "} {
		if strings.Contains(script, leak) {
			t.Errorf("script reverses %q, outside the range:\n%s", leak, script)
		}
	}
	rres, _, _ = MakeRecoverTool(cfg)(ctx, &mcp.CallToolRequest{}, RecoverArgs{Schema: dbName, Table: "order_lines", PKMin: "1"})
	if !rres.IsError || !strings.Contains(resultText(rres), "this table's is (a, b)") {
		t.Errorf("recover on a composite key must refuse before fetching: %s", resultText(rres))
	}
}
