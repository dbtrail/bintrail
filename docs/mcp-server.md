# How the MCP Server Works

This page explains `bintrail-mcp` — the Model Context Protocol server that exposes bintrail's `query`, `recover`, and `status` operations as tools for AI assistants like Claude.

---

## What MCP Is

MCP (Model Context Protocol) is an open standard for connecting AI assistants to external data sources and tools. When an MCP server is registered with an AI assistant, the assistant can call the server's tools during a conversation — and the server's responses become part of the conversation context.

For bintrail, this means you can ask Claude things like:

> "What got deleted from the orders table in the last 10 minutes?"

Claude calls the `query` tool with the right parameters, gets the results back as text, and tells you what happened — without you having to write a single CLI command.

---

## Three Read-Only Tools

The MCP server exposes three tools, all of which are read-only:

| Tool | CLI equivalent | Description |
|------|---------------|-------------|
| `query` | `bintrail query` | Search indexed binlog events with filters |
| `recover` | `bintrail recover --dry-run` | Generate reversal SQL (never executes it) |
| `status` | `bintrail status` | Show indexed files, partitions, and summary |

All three tools are annotated with `ReadOnlyHint: true` and `IdempotentHint: true`. These hints tell the MCP client that it's safe to call them multiple times and that they don't modify any state.

---

## Code Reuse: Same Internals as the CLI

The MCP server is a thin wrapper around the same internal packages used by the CLI:

```
cmd/bintrail-mcp/main.go
       │
       ├── queryTool  → internal/query.Engine.Run (same as query command)
       ├── recoverTool → internal/recovery.Generator.GenerateSQL (same as recover command)
       └── statusTool → internal/status.LoadIndexState + LoadPartitionStats + WriteStatus
```

`buildQueryOptions` in `cmd/bintrail-mcp/main.go` is the shared filter builder used by both `queryTool` and `recoverTool`. It calls the same `cliutil.ParseEventType` and `cliutil.ParseTime` helpers that the CLI uses:

```go
// cmd/bintrail-mcp/main.go
func buildQueryOptions(schema, table, pk, eventType, gtid, since, until, changedCol string,
    limit, defaultLimit int) (query.Options, error) {
    // validate pk requires schema+table
    // validate changed_column requires schema+table
    et, err := cliutil.ParseEventType(eventType)
    sinceT, err := cliutil.ParseTime(since)
    untilT, err := cliutil.ParseTime(until)
    return query.Options{...}, nil
}
```

This means the same validation rules, the same time parsing formats (MySQL datetime `"2006-01-02 15:04:05"`, RFC 3339, date-only `"2006-01-02"`), and the same query logic apply whether you call the CLI or the AI tool.

---

## DSN Resolution

The server needs a MySQL DSN to connect to the index database. There are two ways to provide it:

1. **`BINTRAIL_INDEX_DSN` environment variable** — set once when starting the server; applies to all tool calls.
2. **`index_dsn` tool parameter** — passed on each individual tool call; overrides the env var.

`resolveDSN` implements this priority:

```go
// cmd/bintrail-mcp/main.go
func resolveDSN(override string) (string, error) {
    if override != "" {
        return override, nil
    }
    dsn := os.Getenv("BINTRAIL_INDEX_DSN")
    if dsn == "" {
        return "", fmt.Errorf("no index DSN: set BINTRAIL_INDEX_DSN env var or pass index_dsn parameter")
    }
    return dsn, nil
}
```

In practice, set the env var when starting the server so callers don't need to pass `index_dsn` on every call.

---

## Error Handling: Application vs Protocol Level

MCP distinguishes two kinds of errors:

- **Protocol errors** (the tool couldn't be called at all — wrong parameters, malformed request) → returned as JSON-RPC error responses.
- **Application errors** (the tool was called correctly but the operation failed) → returned as a successful tool call result where `IsError: true`.

The MCP server uses application-level errors for all tool failures:

```go
// cmd/bintrail-mcp/main.go
func errorResult(err error) *mcp.CallToolResult {
    return &mcp.CallToolResult{
        Content: []mcp.Content{
            &mcp.TextContent{Text: err.Error()},
        },
        IsError: true,
    }
}
```

This means Claude sees the error message as text in the tool result, can explain it to you, and can suggest fixes — instead of seeing an opaque JSON-RPC error that it might not understand.

---

## `newServer()`: Extracted for Testability

The server setup is in `newServer()` rather than `main()`:

```go
// cmd/bintrail-mcp/main.go
func newServer() *mcp.Server {
    server := mcp.NewServer(&mcp.Implementation{...}, &mcp.ServerOptions{...})
    mcp.AddTool(server, &mcp.Tool{Name: "query", ...}, queryTool)
    mcp.AddTool(server, &mcp.Tool{Name: "recover", ...}, recoverTool)
    mcp.AddTool(server, &mcp.Tool{Name: "status", ...}, statusTool)
    return server
}
```

`main()` just calls `newServer()` and wires it to a transport. This means unit tests can call `newServer()` directly and pass it an in-memory transport, without starting a subprocess or dealing with stdio framing:

```go
// cmd/bintrail-mcp/integration_test.go
t1, t2 := mcp.NewInMemoryTransports()
go newServer().Connect(ctx, t1, nil)
// connect test client to t2 and call tools...
```

---

## Two Transport Modes

### stdio (default — for Claude Code)

When started without flags, `bintrail-mcp` communicates over `stdin`/`stdout` using newline-delimited JSON-RPC. This is the MCP stdio transport.

Claude Code on the same machine auto-starts the server via `.mcp.json` at the project root:

```json
{
  "mcpServers": {
    "bintrail": {
      "command": "go",
      "args": ["run", "./cmd/bintrail-mcp"]
    }
  }
}
```

When you open the bintrail directory in Claude Code, the server starts automatically. Set `BINTRAIL_INDEX_DSN` in your shell or in the env section of `.mcp.json` and the tools are immediately available.

```sh
export BINTRAIL_INDEX_DSN='user:pass@tcp(127.0.0.1:3306)/binlog_index'
# Now ask Claude: "What got deleted in the orders table today?"
```

### HTTP (for remote access — Claude Desktop)

```sh
BINTRAIL_INDEX_DSN='user:pass@tcp(127.0.0.1:3306)/binlog_index' \
  bintrail-mcp --http :8080
```

HTTP mode starts a persistent HTTP server using the MCP Streamable HTTP spec (2025-03-26). It serves at `/mcp`. Each incoming HTTP connection gets a fresh `newServer()` instance; the SDK manages session state via the `Mcp-Session-Id` response header.

```go
// cmd/bintrail-mcp/main.go
handler := mcp.NewStreamableHTTPHandler(
    func(_ *http.Request) *mcp.Server { return newServer() },
    nil,
)
mux.Handle("/mcp", handler)
```

This is useful when Claude Desktop runs on your laptop but the bintrail server runs on a remote machine.

---

## `proxy.py`: Bridging Claude Desktop to Remote HTTP

Claude Desktop only supports MCP servers via stdio (a subprocess it starts locally). To connect Claude Desktop to a remote `bintrail-mcp --http` server, `cmd/bintrail-mcp/proxy.py` acts as a bridge:

```
Claude Desktop  →  proxy.py (stdio, runs locally)  →  bintrail-mcp --http :8080  →  MySQL
```

`proxy.py` is a single Python file with zero dependencies (stdlib only), compatible with Python 3.7+. It:

1. Reads newline-delimited JSON-RPC messages from `stdin`.
2. POSTs each message to the `BINTRAIL_SERVER` URL.
3. Reads the SSE response and writes JSON-RPC responses back to `stdout`.
4. Tracks the `Mcp-Session-Id` response header across requests (thread-safe, with a lock).

**Python version compatibility**: `proxy.py` uses comment-style type annotations (`# type: str`) instead of `str | None` syntax. The union type syntax requires Python 3.10+, but macOS ships Python 3.9 or older.

**Notifications (no `id` field)**: MCP notifications don't expect a response. `proxy.py` suppresses error responses for notifications because Claude Desktop rejects JSON-RPC error responses that have `id: null`.

**Setup**:

1. Copy `proxy.py` to the remote machine.
2. Add to Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

   ```json
   {
     "mcpServers": {
       "bintrail": {
         "command": "python3",
         "args": ["/Users/you/proxy.py"],
         "env": { "BINTRAIL_SERVER": "http://192.168.1.10:8080/mcp" }
       }
     }
   }
   ```

3. Test connectivity before configuring Claude Desktop:

   ```sh
   BINTRAIL_SERVER=http://192.168.1.10:8080/mcp python3 ~/proxy.py <<'EOF'
   {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}
   {"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
   EOF
   ```

   Two JSON responses = working. No response or connection error = check the server address and firewall.

---

## Stale Session Gotcha

When `bintrail-mcp --http` restarts, all existing sessions are invalidated. But `proxy.py` (started as a subprocess by Claude Desktop) holds the old `Mcp-Session-Id` in memory. Subsequent tool calls fail with validation errors.

**Fix**: Restart Claude Desktop. This kills and restarts the proxy process, clearing the stale session ID. No restart of the HTTP server is needed — just Claude Desktop.

---

## `jsonschema` Tag Format

The tool argument structs use struct tags to describe parameters to the MCP framework. The correct format for `jsonschema-go v0.3+` is a plain description string:

```go
IndexDSN string `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var."`
```

The old `key=value` format (`jsonschema:"description=..."`) is rejected at runtime with a panic inside `mcp.AddTool()`. The panic happens at construction time (when the server starts), not when the tool is called — so a wrong tag format would cause the server to fail immediately on startup.
