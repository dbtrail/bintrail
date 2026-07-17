// Bridge mode (--connect): a stdio ↔ Streamable-HTTP proxy.
//
// The process runs as a local stdio MCP server — what Claude Desktop and
// similar clients launch — and forwards every tool call to a remote bintrail
// Streamable-HTTP MCP endpoint (a `bintrail-mcp --http` server or a console
// `/mcp` endpoint). The tool set is a dynamic passthrough: tools are listed
// from the remote at startup and re-exposed verbatim (names, descriptions,
// schemas, annotations), and re-synced when the remote emits a
// tools/list_changed notification — the bridge never carries its own tool
// declarations, so it cannot drift from the server it fronts.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// bridgeConnectTimeout bounds the initial connect + tool listing against the
// remote endpoint. An unreachable URL or a hung server must produce a fast,
// loud exit — Claude Desktop surfaces stderr, not a silent hang. The
// StreamableClientTransport detaches its connection lifetime from this
// context (verified against go-sdk v1.3.1), so the timeout does not kill an
// established session.
const bridgeConnectTimeout = 15 * time.Second

// validateBridgeFlags enforces the flag contract around --connect: bridge
// mode is mutually exclusive with --http (the process is a client of a remote
// HTTP server, not an HTTP server itself) and with --tenant-dsns (DSN-based
// operation happens on the remote end), and --token is meaningless without
// --connect.
func validateBridgeFlags(connectURL, httpAddr, tenantDSNsFile, token string) error {
	if connectURL == "" {
		if token != "" {
			return fmt.Errorf("--token requires --connect")
		}
		return nil
	}
	if httpAddr != "" {
		return fmt.Errorf("--connect and --http are mutually exclusive: the bridge is a stdio front-end for a remote HTTP endpoint")
	}
	if tenantDSNsFile != "" {
		return fmt.Errorf("--connect and --tenant-dsns are mutually exclusive: in bridge mode DSNs are resolved by the remote server")
	}
	if !strings.HasPrefix(connectURL, "http://") && !strings.HasPrefix(connectURL, "https://") {
		return fmt.Errorf("--connect URL must start with http:// or https:// (got %q)", connectURL)
	}
	return nil
}

// bearerTransport injects an Authorization: Bearer header on requests to the
// configured endpoint host. Scoping by host matters: header stripping on
// cross-host redirects happens in http.Client above the Transport layer, so a
// transport that stamped the token unconditionally would re-attach it to a
// redirect aimed at a different host.
type bearerTransport struct {
	token string
	host  string
	base  http.RoundTripper
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host != t.host {
		return t.base.RoundTrip(req)
	}
	// Per RoundTripper contract the request must not be mutated; clone first.
	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(r)
}

// bridge proxies tool listings and calls between a local MCP server (stdio
// side) and a remote client session (HTTP side).
type bridge struct {
	endpoint string
	session  *mcp.ClientSession
	server   *mcp.Server

	mu    sync.Mutex
	tools map[string]bool // tool names currently registered on the local server
}

// newBridge connects to the remote endpoint, mirrors its tool set onto a
// local proxy server, and returns the bridge. It is factored out of runBridge
// so tests can drive the proxy server over in-memory transports.
func newBridge(ctx context.Context, endpoint, token string) (*bridge, error) {
	httpClient := http.DefaultClient
	if token != "" {
		u, err := url.Parse(endpoint)
		if err != nil {
			return nil, fmt.Errorf("invalid --connect URL %q: %w", endpoint, err)
		}
		httpClient = &http.Client{Transport: &bearerTransport{token: token, host: u.Host, base: http.DefaultTransport}}
	}
	transport := &mcp.StreamableClientTransport{
		Endpoint:   endpoint,
		HTTPClient: httpClient,
	}

	b := &bridge{endpoint: endpoint, tools: map[string]bool{}}

	client := mcp.NewClient(&mcp.Implementation{Name: "bintrail-mcp-bridge", Version: mcpVersion}, &mcp.ClientOptions{
		// Re-sync the mirrored tool set when the remote's changes, so a
		// long-lived Desktop session tracks server-side updates.
		ToolListChangedHandler: func(ctx context.Context, req *mcp.ToolListChangedRequest) {
			if err := b.resync(ctx); err != nil {
				slog.Warn("bridge: tool re-sync after list_changed failed", "endpoint", b.endpoint, "error", err)
			}
		},
	})

	connectCtx, cancel := context.WithTimeout(ctx, bridgeConnectTimeout)
	defer cancel()

	session, err := client.Connect(connectCtx, transport, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to %s: %w%s", endpoint, err, authHint(err))
	}
	b.session = session

	// Mirror the remote server's instructions so the stdio client sees the
	// same guidance it would get connecting directly.
	instructions := ""
	if ir := session.InitializeResult(); ir != nil {
		instructions = ir.Instructions
	}
	b.server = mcp.NewServer(&mcp.Implementation{Name: "bintrail", Version: mcpVersion}, &mcp.ServerOptions{
		Instructions: instructions,
	})

	if err := b.resync(connectCtx); err != nil {
		session.Close()
		return nil, fmt.Errorf("cannot list tools on %s: %w%s", endpoint, err, authHint(err))
	}
	return b, nil
}

// Close terminates the remote session.
func (b *bridge) Close() error { return b.session.Close() }

// toolCount returns the number of currently mirrored tools.
func (b *bridge) toolCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.tools)
}

// resync lists the remote tools and reconciles the local proxy server's tool
// set: every remote tool is (re-)registered verbatim with a forwarding
// handler, and local tools that disappeared from the remote are removed.
func (b *bridge) resync(ctx context.Context) error {
	var remote []*mcp.Tool
	for tool, err := range b.session.Tools(ctx, nil) {
		if err != nil {
			return err
		}
		remote = append(remote, tool)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	seen := map[string]bool{}
	for _, tool := range remote {
		// AddTool panics on a non-object input schema — and on a present,
		// non-object output schema; a malformed remote tool must not take the
		// whole bridge down.
		if !isObjectSchema(tool.InputSchema) {
			slog.Warn("bridge: skipping remote tool with non-object input schema", "tool", tool.Name)
			continue
		}
		if tool.OutputSchema != nil && !isObjectSchema(tool.OutputSchema) {
			slog.Warn("bridge: skipping remote tool with non-object output schema", "tool", tool.Name)
			continue
		}
		b.server.AddTool(tool, b.forward(tool.Name))
		seen[tool.Name] = true
	}
	var stale []string
	for name := range b.tools {
		if !seen[name] {
			stale = append(stale, name)
		}
	}
	if len(stale) > 0 {
		b.server.RemoveTools(stale...)
	}
	b.tools = seen
	return nil
}

// forward returns a ToolHandler that relays the raw call to the remote tool
// and returns the remote result unchanged (including IsError results).
func (b *bridge) forward(name string) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		params := &mcp.CallToolParams{Name: name}
		if len(req.Params.Arguments) > 0 {
			params.Arguments = json.RawMessage(req.Params.Arguments)
		}
		return b.session.CallTool(ctx, params)
	}
}

// isObjectSchema reports whether a listed tool's input schema is a JSON
// object schema — the only shape Server.AddTool accepts.
func isObjectSchema(schema any) bool {
	raw, err := json.Marshal(schema)
	if err != nil {
		return false
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return false
	}
	return m["type"] == "object"
}

// authHint appends a token hint when the remote rejected the request with an
// auth-shaped status, so the one-line stderr message is actionable.
func authHint(err error) string {
	msg := err.Error()
	if strings.Contains(msg, "401") || strings.Contains(msg, "403") ||
		strings.Contains(msg, "Unauthorized") || strings.Contains(msg, "Forbidden") {
		return " (authentication rejected — check --token)"
	}
	return ""
}

// runBridge runs bridge mode: connect to the remote endpoint, then serve the
// mirrored tool set over stdio until the client disconnects.
func runBridge(ctx context.Context, endpoint, token string) error {
	b, err := newBridge(ctx, endpoint, token)
	if err != nil {
		return err
	}
	defer b.Close()

	slog.Info("bridge connected", "endpoint", endpoint, "tools", b.toolCount())
	return b.server.Run(ctx, &mcp.StdioTransport{})
}
