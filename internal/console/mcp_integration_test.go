//go:build integration

package console

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// bearerTransport injects the console token into every MCP HTTP request.
type bearerTransport struct{ token string }

func (b bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(req)
}

// mcpConnect opens a real Streamable HTTP MCP session against endpoint.
func mcpConnect(t *testing.T, endpoint, token string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "it", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:   endpoint,
		HTTPClient: &http.Client{Transport: bearerTransport{token}},
	}, nil)
	if err != nil {
		t.Fatalf("MCP client Connect: %v", err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func mcpToolText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if len(res.Content) == 0 {
		return ""
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("expected *mcp.TextContent, got %T", res.Content[0])
	}
	return tc.Text
}

// seedMCPConsole seeds an index with an INSERT+UPDATE on app.users pk=1,
// stamps captured statement text on every event (the redaction subject), and
// returns a token-authenticated console over it.
func seedMCPConsole(t *testing.T) (*Server, *sql.DB, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "users", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"name":"alice"}`))
	testutil.InsertEvent(t, db, "bin.000001", 40, 80, "2026-06-01 12:05:00", nil,
		"app", "users", 2 /*UPDATE*/, "1",
		[]byte(`["name"]`), []byte(`{"id":1,"name":"alice"}`), []byte(`{"id":1,"name":"alicia"}`))

	// Captured statement text — present in the index, redacted by the console
	// read boundary (the events API's eventDTO omits it, #699).
	if _, err := db.Exec(`UPDATE binlog_events SET query_text = 'UPDATE users SET name = ''SECRET_LITERAL''', query_hash = 'cafe0199'`); err != nil {
		t.Fatalf("stamp query_text: %v", err)
	}

	srv, err := New(Config{
		DB:        db,
		DBName:    dbName,
		Listen:    "127.0.0.1:8090",
		Token:     intToken,
		NoArchive: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, db, dbName
}

func TestIntegrationMCPEndpoint(t *testing.T) {
	srv, _, _ := seedMCPConsole(t)
	ts := httptest.NewServer(srv.Handler())
	// Cleanup, not defer: the streamable client holds a standalone SSE GET open
	// for the session lifetime, and httptest Close blocks until every
	// connection drains. Registering ts.Close BEFORE the session connects makes
	// the LIFO cleanup order close the session (registered later, inside
	// mcpConnect) first.
	t.Cleanup(ts.Close)
	ctx := context.Background()

	session := mcpConnect(t, ts.URL+"/mcp", intToken)

	// Tool listing: exactly the read-only tools, including reconstruct (#953 —
	// registered whenever the console serves MCP; the per-server baseline gate
	// is enforced on the CALL, mirroring /api/reconstruct's 404).
	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range tools.Tools {
		names[tool.Name] = true
	}
	for _, want := range []string{"query", "recover", "status", "list_schema_changes", "reconstruct"} {
		if !names[want] {
			t.Errorf("tool %q not listed; got %v", want, names)
		}
	}
	if len(tools.Tools) != 5 {
		t.Errorf("expected 5 tools, got %d", len(tools.Tools))
	}

	// This console was seeded with NoArchive (no baseline), so the per-server
	// gate must refuse — and the parameters that would let a client point the
	// console at arbitrary storage must be refused too.
	for name, args := range map[string]map[string]any{
		"ungated": {"schema": "app", "table": "users", "pk": "1"},
		"baseline_dir param": {"schema": "app", "table": "users", "pk": "1",
			"baseline_dir": t.TempDir()},
	} {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "reconstruct", Arguments: args})
		if err != nil {
			t.Fatalf("CallTool reconstruct (%s): %v", name, err)
		}
		if !res.IsError {
			t.Errorf("reconstruct (%s): expected a tool error, got %s", name, mcpToolText(t, res))
		}
	}

	// Query round-trip: both events come back, statement text does not.
	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{"schema": "app", "table": "users"},
	})
	if err != nil {
		t.Fatalf("CallTool query: %v", err)
	}
	text := mcpToolText(t, res)
	if res.IsError {
		t.Fatalf("query IsError: %s", text)
	}
	if !strings.Contains(text, "alicia") {
		t.Errorf("query result missing row data: %s", text)
	}
	if strings.Contains(text, "SECRET_LITERAL") || strings.Contains(text, "cafe0199") {
		t.Errorf("query result leaks statement text the console API redacts: %s", text)
	}

	// index_dsn is rejected: the console routes connections itself.
	res, err = session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{"index_dsn": "u:p@tcp(127.0.0.1:13306)/other"},
	})
	if err != nil {
		t.Fatalf("CallTool query(index_dsn): %v", err)
	}
	if !res.IsError || !strings.Contains(mcpToolText(t, res), "index_dsn is not accepted") {
		t.Errorf("index_dsn must be rejected, got: %s", mcpToolText(t, res))
	}

	// The console events cap (1000) applies as the query ceiling.
	res, err = session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{"schema": "app", "table": "users", "limit": 5000},
	})
	if err != nil {
		t.Fatalf("CallTool query(limit): %v", err)
	}
	if !strings.Contains(mcpToolText(t, res), "ceiling of 1000") {
		t.Errorf("oversized limit must be capped at the console events cap: %s", mcpToolText(t, res))
	}

	// Recover round-trip: reversal SQL for the UPDATE restores "alice".
	res, err = session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "recover",
		Arguments: map[string]any{"schema": "app", "table": "users", "pk": "1"},
	})
	if err != nil {
		t.Fatalf("CallTool recover: %v", err)
	}
	text = mcpToolText(t, res)
	if res.IsError {
		t.Fatalf("recover IsError: %s", text)
	}
	if !strings.Contains(text, "alice") {
		t.Errorf("recover script missing reversal data: %s", text)
	}

	// Status round-trip.
	res, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "status", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("CallTool status: %v", err)
	}
	if res.IsError {
		t.Fatalf("status IsError: %s", mcpToolText(t, res))
	}
	if mcpToolText(t, res) == "" {
		t.Error("status returned empty text")
	}
}

func TestIntegrationMCPWrongTokenRefused(t *testing.T) {
	srv, _, _ := seedMCPConsole(t)
	ts := httptest.NewServer(srv.Handler())
	// Cleanup, not defer: the streamable client holds a standalone SSE GET open
	// for the session lifetime, and httptest Close blocks until every
	// connection drains. Registering ts.Close BEFORE the session connects makes
	// the LIFO cleanup order close the session (registered later, inside
	// mcpConnect) first.
	t.Cleanup(ts.Close)

	client := mcp.NewClient(&mcp.Implementation{Name: "it", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:   ts.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearerTransport{"not-the-token"}},
	}, nil)
	if err == nil {
		session.Close()
		t.Fatal("MCP connect with a wrong token must fail")
	}
}

func TestIntegrationMCPRoutesByServerName(t *testing.T) {
	srv, _, dbName := seedMCPConsole(t)
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(ServerEntry{Name: "reg-target", DSN: testutil.SnapshotDSN(dbName), NoArchive: true}); err != nil {
		t.Fatal(err)
	}
	// Rebuild the console over the registry (same index behind both names).
	srv.cm.reg = reg

	ts := httptest.NewServer(srv.Handler())
	// Cleanup, not defer: the streamable client holds a standalone SSE GET open
	// for the session lifetime, and httptest Close blocks until every
	// connection drains. Registering ts.Close BEFORE the session connects makes
	// the LIFO cleanup order close the session (registered later, inside
	// mcpConnect) first.
	t.Cleanup(ts.Close)
	ctx := context.Background()

	session := mcpConnect(t, ts.URL+"/mcp/reg-target", intToken)
	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{"schema": "app", "table": "users"},
	})
	if err != nil {
		t.Fatalf("CallTool via /mcp/reg-target: %v", err)
	}
	if res.IsError {
		t.Fatalf("query via named server IsError: %s", mcpToolText(t, res))
	}
	if !strings.Contains(mcpToolText(t, res), "alicia") {
		t.Errorf("named-server query missing row data: %s", mcpToolText(t, res))
	}
}
