//go:build integration

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// ─── Shared helpers ──────────────────────────────────────────────────────────

// connectMCP creates an in-memory MCP client session connected to a fresh server.
// The server is started first (SDK requirement), then the client connects.
func connectMCP(t *testing.T) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()

	t1, t2 := mcp.NewInMemoryTransports()
	if _, err := newServer().Connect(ctx, t1, nil); err != nil {
		t.Fatalf("server Connect: %v", err)
	}

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v1.0.0"}, nil)
	session, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

// callToolText extracts the text from the first content item of a tool result.
func callToolText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	if len(result.Content) == 0 {
		return ""
	}
	tc, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("expected *mcp.TextContent, got %T", result.Content[0])
	}
	return tc.Text
}

// setupTestDB creates a fresh test database with index tables and returns the
// db handle, database name, and a DSN string suitable for tool arguments.
func setupTestDB(t *testing.T) (*sql.DB, string, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	return db, dbName, testutil.SnapshotDSN(dbName)
}

// ─── ListTools ───────────────────────────────────────────────────────────────

func TestListTools(t *testing.T) {
	session := connectMCP(t)
	ctx := context.Background()

	result, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}

	names := make(map[string]bool)
	for _, tool := range result.Tools {
		names[tool.Name] = true
	}

	// reconstruct (#953) is opted into by the standalone posture on top of the
	// four always-on tools — its baseline source is resolved per call, so it is
	// advertised whether or not one is configured.
	for _, want := range []string{"query", "recover", "status", "list_schema_changes", "reconstruct"} {
		if !names[want] {
			t.Errorf("tool %q not found; got: %v", want, names)
		}
	}
	if len(result.Tools) != 5 {
		t.Errorf("expected 5 tools, got %d", len(result.Tools))
	}
}

// ─── Query tool ──────────────────────────────────────────────────────────────

func TestQueryTool_jsonDefault(t *testing.T) {
	db, _, dsn := setupTestDB(t)
	ctx := context.Background()

	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, "2026-02-19 10:00:00", nil,
		"mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1,"status":"new"}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, "2026-02-19 10:01:00", nil,
		"mydb", "orders", 3, "2", nil, []byte(`{"id":2,"status":"old"}`), nil)

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "query",
		Arguments: map[string]any{
			"index_dsn": dsn,
			"schema":    "mydb",
			"table":     "orders",
			"format":    "json",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool error: %s", callToolText(t, result))
	}

	var rows []any
	if err := json.Unmarshal([]byte(callToolText(t, result)), &rows); err != nil {
		t.Fatalf("parse JSON: %v\ntext: %s", err, callToolText(t, result))
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}
}

func TestQueryTool_tableFormat(t *testing.T) {
	db, _, dsn := setupTestDB(t)
	ctx := context.Background()

	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, "2026-02-19 10:00:00", nil,
		"mydb", "orders", 1, "42", nil, nil, []byte(`{"id":42}`))

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "query",
		Arguments: map[string]any{
			"index_dsn": dsn,
			"schema":    "mydb",
			"table":     "orders",
			"format":    "table",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool error: %s", callToolText(t, result))
	}

	text := callToolText(t, result)
	if !strings.Contains(text, "INSERT") {
		t.Errorf("expected INSERT in output, got: %s", text)
	}
	if !strings.Contains(text, "1 row(s)") {
		t.Errorf("expected '1 row(s)' in output, got: %s", text)
	}
}

func TestQueryTool_pkFilter(t *testing.T) {
	db, _, dsn := setupTestDB(t)
	ctx := context.Background()

	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, "2026-02-19 10:00:00", nil,
		"mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, "2026-02-19 10:01:00", nil,
		"mydb", "orders", 1, "2", nil, nil, []byte(`{"id":2}`))

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "query",
		Arguments: map[string]any{
			"index_dsn": dsn,
			"schema":    "mydb",
			"table":     "orders",
			"pk":        "1",
			"format":    "json",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool error: %s", callToolText(t, result))
	}

	var rows []any
	if err := json.Unmarshal([]byte(callToolText(t, result)), &rows); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row for pk=1, got %d", len(rows))
	}
}

func TestQueryTool_eventTypeFilter(t *testing.T) {
	db, _, dsn := setupTestDB(t)
	ctx := context.Background()

	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, "2026-02-19 10:00:00", nil,
		"mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, "2026-02-19 10:01:00", nil,
		"mydb", "orders", 3, "2", nil, []byte(`{"id":2}`), nil)

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "query",
		Arguments: map[string]any{
			"index_dsn":  dsn,
			"schema":     "mydb",
			"table":      "orders",
			"event_type": "DELETE",
			"format":     "json",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool error: %s", callToolText(t, result))
	}

	var rows []map[string]any
	if err := json.Unmarshal([]byte(callToolText(t, result)), &rows); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 DELETE row, got %d", len(rows))
	}
	if rows[0]["event_type"] != "DELETE" {
		t.Errorf("expected event_type DELETE, got %v", rows[0]["event_type"])
	}
}

func TestQueryTool_noResults(t *testing.T) {
	_, _, dsn := setupTestDB(t)
	ctx := context.Background()

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "query",
		Arguments: map[string]any{
			"index_dsn": dsn,
			"format":    "json",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", callToolText(t, result))
	}
}

func TestQueryTool_invalidFormat(t *testing.T) {
	_, _, dsn := setupTestDB(t)
	ctx := context.Background()

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "query",
		Arguments: map[string]any{
			"index_dsn": dsn,
			"format":    "xml",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !result.IsError {
		t.Fatalf("expected IsError for invalid format, got: %s", callToolText(t, result))
	}
}

func TestQueryTool_missingDSN(t *testing.T) {
	t.Setenv("BINTRAIL_INDEX_DSN", "")
	ctx := context.Background()

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected IsError when no DSN provided")
	}
	if !strings.Contains(callToolText(t, result), "BINTRAIL_INDEX_DSN") {
		t.Errorf("expected DSN mention in error, got: %s", callToolText(t, result))
	}
}

func TestQueryTool_envDSN(t *testing.T) {
	db, _, dsn := setupTestDB(t)
	t.Setenv("BINTRAIL_INDEX_DSN", dsn)
	ctx := context.Background()

	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, "2026-02-19 10:00:00", nil,
		"mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "query",
		Arguments: map[string]any{
			"schema": "mydb",
			"table":  "orders",
			"format": "json",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", callToolText(t, result))
	}

	var rows []any
	if err := json.Unmarshal([]byte(callToolText(t, result)), &rows); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row via env DSN, got %d", len(rows))
	}
}

func TestQueryTool_pkWithoutSchema(t *testing.T) {
	_, _, dsn := setupTestDB(t)
	ctx := context.Background()

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "query",
		Arguments: map[string]any{
			"index_dsn": dsn,
			"pk":        "123",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected IsError when pk set without schema/table")
	}
}

func TestQueryTool_invalidEventType(t *testing.T) {
	_, _, dsn := setupTestDB(t)
	ctx := context.Background()

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "query",
		Arguments: map[string]any{
			"index_dsn":  dsn,
			"event_type": "UPSERT",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected IsError for invalid event_type")
	}
}

// ─── Recover tool ─────────────────────────────────────────────────────────────

func TestRecoverTool_deleteToInsert(t *testing.T) {
	db, _, dsn := setupTestDB(t)
	ctx := context.Background()

	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, "2026-02-19 10:00:00", nil,
		"mydb", "users", 3, "42", nil, []byte(`{"id":42,"name":"Alice"}`), nil)

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "recover",
		Arguments: map[string]any{
			"index_dsn":  dsn,
			"schema":     "mydb",
			"table":      "users",
			"event_type": "DELETE",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool error: %s", callToolText(t, result))
	}

	text := callToolText(t, result)
	if !strings.Contains(text, "INSERT INTO") {
		t.Errorf("expected INSERT INTO in recovery SQL, got: %s", text)
	}
	if !strings.Contains(text, "Alice") {
		t.Errorf("expected 'Alice' in recovery SQL, got: %s", text)
	}
}

func TestRecoverTool_noEvents(t *testing.T) {
	_, _, dsn := setupTestDB(t)
	ctx := context.Background()

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "recover",
		Arguments: map[string]any{
			"index_dsn": dsn,
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected error for empty result: %s", callToolText(t, result))
	}
}

func TestRecoverTool_statementCount(t *testing.T) {
	db, _, dsn := setupTestDB(t)
	ctx := context.Background()

	for i := range 3 {
		pk := string(rune('1' + i))
		testutil.InsertEvent(t, db, "mysql-bin.000001", uint64(100+i*100), uint64(200+i*100),
			"2026-02-19 10:00:00", nil,
			"mydb", "orders", 3, pk, nil, []byte(`{"id":1}`), nil)
	}

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "recover",
		Arguments: map[string]any{
			"index_dsn": dsn,
			"schema":    "mydb",
			"table":     "orders",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool error: %s", callToolText(t, result))
	}

	text := callToolText(t, result)
	if !strings.Contains(text, "3 reversal statement(s)") {
		t.Errorf("expected '3 reversal statement(s)' in output, got: %s", text)
	}
}

// TestRecoverTool_limitKeepsNewestEvents pins #982 (the MCP recover tool's
// mirror of #785/#927): when --limit truncates the matched window, the
// reversal script must undo the most RECENT events, not the oldest prefix
// that the missing Order="DESC" used to leave behind. Seeds 4 chained
// UPDATEs on one row (v0→v1→v2→v3→v4) at one-minute intervals, so a
// truncating limit=2 has an unambiguous newest-suffix (v2→v3, v3→v4) versus
// oldest-prefix (v0→v1, v1→v2); v0/v1 appear ONLY in the oldest two events.
func TestRecoverTool_limitKeepsNewestEvents(t *testing.T) {
	db, _, dsn := setupTestDB(t)
	ctx := context.Background()

	base := "2026-02-19 10:00:00"
	baseTime, err := time.Parse("2006-01-02 15:04:05", base)
	if err != nil {
		t.Fatalf("parse base time: %v", err)
	}
	for i := 1; i <= 4; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Minute).Format("2006-01-02 15:04:05")
		before := fmt.Sprintf(`{"id":1,"v":"v%d"}`, i-1)
		after := fmt.Sprintf(`{"id":1,"v":"v%d"}`, i)
		testutil.InsertEvent(t, db, "mysql-bin.000001", uint64(i*100), uint64(i*100+50), ts, nil,
			"mydb", "orders", 2 /* UPDATE */, "1",
			[]byte(`["v"]`), []byte(before), []byte(after))
	}

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "recover",
		Arguments: map[string]any{
			"index_dsn": dsn,
			"schema":    "mydb",
			"table":     "orders",
			"limit":     2,
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool error: %s", callToolText(t, result))
	}

	text := callToolText(t, result)
	// Newest two events are v2→v3 and v3→v4; their reversals reference v2..v4.
	if !strings.Contains(text, `'v3'`) || !strings.Contains(text, `'v2'`) {
		t.Errorf("expected reversals of the two NEWEST updates (v2→v3, v3→v4), got:\n%s", text)
	}
	// v0/v1 exist only in the oldest two events — any occurrence means the
	// truncation kept the oldest prefix (the #982/#785 bug).
	if strings.Contains(text, `'v0'`) || strings.Contains(text, `'v1'`) {
		t.Errorf("script reversed the OLDEST events; --limit must keep the newest suffix of the window:\n%s", text)
	}
}

func TestRecoverTool_invalidSince(t *testing.T) {
	_, _, dsn := setupTestDB(t)
	ctx := context.Background()

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "recover",
		Arguments: map[string]any{
			"index_dsn": dsn,
			"since":     "not-a-date",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected IsError for invalid since")
	}
}

// ─── Status tool ──────────────────────────────────────────────────────────────

func TestStatusTool_emptyIndex(t *testing.T) {
	_, _, dsn := setupTestDB(t)
	ctx := context.Background()

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "status",
		Arguments: map[string]any{
			"index_dsn": dsn,
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool error: %s", callToolText(t, result))
	}

	text := callToolText(t, result)
	if !strings.Contains(text, "no files indexed yet") {
		t.Errorf("expected 'no files indexed yet', got: %s", text)
	}
	if !strings.Contains(text, "p_future") {
		t.Errorf("expected 'p_future' in output, got: %s", text)
	}
}

func TestStatusTool_withFiles(t *testing.T) {
	db, _, dsn := setupTestDB(t)
	ctx := context.Background()

	testutil.MustExec(t, db, `INSERT INTO index_state
		(binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at)
		VALUES ('mysql-bin.000042', 8192, 8192, 5, 'completed', '2026-02-19 09:00:00', '2026-02-19 09:01:00')`)

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "status",
		Arguments: map[string]any{
			"index_dsn": dsn,
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool error: %s", callToolText(t, result))
	}

	text := callToolText(t, result)
	if !strings.Contains(text, "completed") {
		t.Errorf("expected 'completed' in output, got: %s", text)
	}
	if !strings.Contains(text, "mysql-bin.000042") {
		t.Errorf("expected binlog filename in output, got: %s", text)
	}
	if !strings.Contains(text, "=== Summary ===") {
		t.Errorf("expected '=== Summary ===' in output, got: %s", text)
	}
}

func TestStatusTool_missingDSN(t *testing.T) {
	t.Setenv("BINTRAIL_INDEX_DSN", "")
	ctx := context.Background()

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "status",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected IsError when no DSN provided")
	}
}

func TestStatusTool_dsnWithoutDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	session := connectMCP(t)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "status",
		Arguments: map[string]any{
			"index_dsn": "root:testroot@tcp(127.0.0.1:13306)/",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected IsError for DSN without database name")
	}
	if !strings.Contains(callToolText(t, result), "database name") {
		t.Errorf("expected 'database name' mention in error, got: %s", callToolText(t, result))
	}
}
