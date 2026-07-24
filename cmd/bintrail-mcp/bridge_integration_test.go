//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// startRemote serves an MCP server over Streamable HTTP the way
// `bintrail-mcp --http` does. When requireToken is non-empty, every request
// must carry `Authorization: Bearer <requireToken>` or it is rejected with
// 401 — the shape of a token-authenticated console `/mcp` endpoint. The
// returned server instance is shared across sessions so tests can mutate its
// tool set.
func startRemote(t *testing.T, requireToken string) (*httptest.Server, *mcp.Server) {
	t.Helper()
	remote := newServer()
	handler := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return remote },
		nil,
	)
	mux := http.NewServeMux()
	mux.Handle("/mcp", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requireToken != "" && r.Header.Get("Authorization") != "Bearer "+requireToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts, remote
}

// connectBridgeClient starts the bridge's proxy server over in-memory
// transports and returns a client session speaking to it — the in-process
// equivalent of Claude Desktop driving the bridge over stdio.
func connectBridgeClient(t *testing.T, b *bridge) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	t1, t2 := mcp.NewInMemoryTransports()
	if _, err := b.server.Connect(ctx, t1, nil); err != nil {
		t.Fatalf("bridge server Connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-desktop", Version: "v1.0.0"}, nil)
	session, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

// TestIntegrationBridgePassthrough verifies the bridge mirrors the remote
// tool set verbatim and round-trips a tool call, with the token accepted by
// an authenticated remote.
func TestIntegrationBridgePassthrough(t *testing.T) {
	ctx := context.Background()
	ts, _ := startRemote(t, "s3cret")

	b, err := newBridge(ctx, ts.URL+"/mcp", "s3cret")
	if err != nil {
		t.Fatalf("newBridge: %v", err)
	}
	t.Cleanup(func() { b.Close() })

	session := connectBridgeClient(t, b)

	// Tool listing must match the remote server's set, schemas included.
	res, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
		if tool.Description == "" {
			t.Errorf("tool %s: empty description (not mirrored)", tool.Name)
		}
		raw, _ := json.Marshal(tool.InputSchema)
		var schema map[string]any
		if err := json.Unmarshal(raw, &schema); err != nil || schema["type"] != "object" {
			t.Errorf("tool %s: input schema not mirrored as an object: %s", tool.Name, raw)
		}
	}
	sort.Strings(names)
	want := []string{"list_schema_changes", "query", "recover", "status"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("mirrored tools = %v, want %v", names, want)
	}

	// The query tool's schema must carry the remote's properties verbatim.
	for _, tool := range res.Tools {
		if tool.Name != "query" {
			continue
		}
		raw, _ := json.Marshal(tool.InputSchema)
		if !strings.Contains(string(raw), "changed_column") {
			t.Errorf("query input schema lost remote properties: %s", raw)
		}
	}

	// Round-trip a call through bridge → HTTP → remote handler. An IsError
	// result (unreachable DSN) still proves the full forwarding path: the
	// text is produced by the remote server's tool handler.
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "status",
		Arguments: map[string]any{"index_dsn": "baduser:badpass@tcp(127.0.0.1:1)/nope"},
	})
	if err != nil {
		t.Fatalf("CallTool status: %v", err)
	}
	if !result.IsError {
		t.Fatalf("expected IsError result for unreachable DSN, got %+v", result)
	}
	if text := callToolText(t, result); text == "" {
		t.Fatal("empty error text from forwarded tool call")
	}
}

// TestIntegrationBridgeToolCallRealDB round-trips a successful query call
// against a real index database through the bridge.
func TestIntegrationBridgeToolCallRealDB(t *testing.T) {
	_, _, dsn := setupTestDB(t)

	ctx := context.Background()
	ts, _ := startRemote(t, "")

	b, err := newBridge(ctx, ts.URL+"/mcp", "")
	if err != nil {
		t.Fatalf("newBridge: %v", err)
	}
	t.Cleanup(func() { b.Close() })

	session := connectBridgeClient(t, b)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{"index_dsn": dsn, "limit": 5},
	})
	if err != nil {
		t.Fatalf("CallTool query: %v", err)
	}
	if result.IsError {
		t.Fatalf("query via bridge failed: %s", callToolText(t, result))
	}
}

// TestIntegrationBridgeToolListResync verifies the dynamic passthrough: a
// tool added on the remote after the bridge is up appears on the bridge via
// the tools/list_changed notification, without re-declaring anything locally.
func TestIntegrationBridgeToolListResync(t *testing.T) {
	ctx := context.Background()
	ts, remote := startRemote(t, "")

	b, err := newBridge(ctx, ts.URL+"/mcp", "")
	if err != nil {
		t.Fatalf("newBridge: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	session := connectBridgeClient(t, b)

	remote.AddTool(&mcp.Tool{
		Name:        "late_tool",
		Description: "added after the bridge connected",
		InputSchema: map[string]any{"type": "object"},
	}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "late"}}}, nil
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		res, err := session.ListTools(ctx, nil)
		if err != nil {
			t.Fatalf("ListTools: %v", err)
		}
		for _, tool := range res.Tools {
			if tool.Name == "late_tool" {
				return // resynced
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("late_tool never appeared on the bridge after remote AddTool")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestIntegrationBridgeWrongToken verifies a rejected token fails fast with a
// clear, actionable error instead of hanging.
func TestIntegrationBridgeWrongToken(t *testing.T) {
	ctx := context.Background()
	ts, _ := startRemote(t, "s3cret")

	start := time.Now()
	_, err := newBridge(ctx, ts.URL+"/mcp", "wrong")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("newBridge succeeded with a wrong token")
	}
	if !strings.Contains(err.Error(), "cannot connect to") {
		t.Errorf("error lacks endpoint context: %v", err)
	}
	if !strings.Contains(err.Error(), "check --token") {
		t.Errorf("401 error lacks the token hint: %v", err)
	}
	if elapsed > bridgeConnectTimeout+5*time.Second {
		t.Errorf("wrong-token failure took %v; expected fast failure", elapsed)
	}
}

// TestIntegrationBridgeRefusedConnection verifies an unreachable endpoint
// fails within the connect timeout instead of hanging.
func TestIntegrationBridgeRefusedConnection(t *testing.T) {
	// Reserve a port and close it so the connection is refused.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	ctx := context.Background()
	start := time.Now()
	_, err = newBridge(ctx, "http://"+addr+"/mcp", "")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("newBridge succeeded against a closed port")
	}
	if !strings.Contains(err.Error(), "cannot connect to") {
		t.Errorf("error lacks endpoint context: %v", err)
	}
	if elapsed > bridgeConnectTimeout+5*time.Second {
		t.Errorf("refused-connection failure took %v; expected failure within the connect timeout", elapsed)
	}
}
