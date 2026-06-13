# The MCP Server

`bintrail-mcp` is a Model Context Protocol server that exposes dbtrail's `query`, `recover`, `status`, and `list_schema_changes` as tools for AI assistants like Claude — so you can investigate database changes in natural language instead of running CLI commands.

---

## What MCP Is

MCP (Model Context Protocol) is an open standard for connecting AI assistants to external data sources and tools. When an MCP server is registered with an AI assistant, the assistant can call the server's tools during a conversation — and the server's responses become part of the conversation context.

For dbtrail, this means you can ask Claude things like:

> "What got deleted from the orders table in the last 10 minutes?"

Claude calls the `query` tool with the right parameters, gets the results back as text, and tells you what happened — without you having to write a single CLI command.

---

## Four Read-Only Tools

The MCP server exposes four tools, all of which are read-only:

| Tool | CLI equivalent | Description |
|------|---------------|-------------|
| `query` | `bintrail query` | Search indexed binlog events with filters |
| `recover` | `bintrail recover --dry-run` | Generate reversal SQL (never executes it) |
| `status` | `bintrail status` | Show indexed files, partitions, and summary |
| `list_schema_changes` | — (reads the `schema_changes` table, see [DDL tracking](./ddl-tracking.md)) | List DDL changes recorded during indexing or streaming, with the full statement and binlog coordinates |

All four tools are annotated with `ReadOnlyHint: true` and `IdempotentHint: true`. These hints tell the MCP client that it's safe to call them multiple times and that they don't modify any state.

`list_schema_changes` accepts `schema`, `table`, `ddl_type`, `since`, `until`, and `limit` (default 100). `ddl_type` takes `CREATE`, `ALTER`, `DROP`, `RENAME`, or `TRUNCATE` and is prefix-matched against the stored values (`ALTER` matches `ALTER TABLE`). Results come back as JSON, newest first (a plain "No schema changes found." when nothing matches).

---

## Tool parameters and behavior

The `query` and `recover` tools share the CLI's filters and validation. Beyond the basics (`schema`, `table`, `pk`, `event_type`, `gtid`, `since`, `until`), both accept:

- `changed_column` — events that touched a given column
- `column_eq` — repeatable `column=value` equality filters
- `flag` — table/column flag filter
- `profile` — RBAC table-deny + column-redaction
- `no_archive` — disable Parquet archive auto-routing (see below)

`query` additionally takes `format` (`json`, `table`, or `csv`). Time values (`since` / `until`) accept MySQL datetime (`2006-01-02 15:04:05`), RFC 3339, or date-only (`2006-01-02`) — the same formats as the CLI.

**Archive auto-discovery.** `query` and `recover` automatically discover Parquet archive sources from `archive_state` in the index database (or from the `BINTRAIL_ARCHIVE_S3` + `BINTRAIL_ID` env vars). When archives are found, results from MySQL and Parquet are merged, deduplicated, and sorted before output or SQL generation. Pass `no_archive` to disable this.

**Index DSN.** The server connects to the index via the `BINTRAIL_INDEX_DSN` environment variable (set once at startup) or the per-call `index_dsn` parameter, which overrides the env var. Set the env var at startup so callers don't repeat it on every call.

---

## Three Ways to Connect Claude

| Method | Works from | Auth | Setup |
|---|---|---|---|
| **stdio** (recommended for Claude Code) | Claude Code (local) | None (trusts local user) | `.mcp.json` at project root |
| **Claude Connector** | claude.ai, Claude Desktop, Claude mobile | OAuth 2.1 (automatic) | Self-host a gateway (or use dbtrail's hosted one), add its URL in Claude Settings |
| **proxy.py** (legacy) | Claude Desktop only | None (trusts local user) | Edit `claude_desktop_config.json` |

For Claude Code on the same machine as your index, **stdio** is the zero-infrastructure path — start there. The **Claude Connector** reaches the index from claude.ai / Claude Desktop / mobile over the network, and needs an MCP Gateway in front (which you self-host, or which dbtrail operates for managed-service customers).

### stdio (recommended — for Claude Code)

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

When you open the dbtrail directory in Claude Code, the server starts automatically. Set `BINTRAIL_INDEX_DSN` in your shell or in the env section of `.mcp.json` and the tools are immediately available.

```sh
export BINTRAIL_INDEX_DSN='user:pass@tcp(127.0.0.1:3306)/binlog_index'
# Now ask Claude: "What got deleted in the orders table today?"
```

### Claude Connector (for claude.ai, Desktop, and mobile)

This reaches your index over the network through an **MCP Gateway** — an OAuth front door whose source is in `cmd/mcp-gateway`. It is an **advanced, optional** path; most users want **stdio** (above), which needs no gateway at all. There are two ways to get one:

- **Self-host it (open source).** Build `cmd/mcp-gateway` and run it behind HTTPS on your own infrastructure and public domain — fully doable with this repo alone.
- **Use dbtrail's hosted gateway (managed-service customers).** dbtrail operates one at `https://mcp.dbtrail.com/mcp` and provisions your tenant. This requires a dbtrail account — the open-source repo does **not** give you access to it; it is the managed offering.

Once a gateway is reachable at a public URL:

1. Open **claude.ai** → **Settings** → **Integrations**
2. Click **Add custom integration**
3. Enter the gateway URL — your self-hosted domain (e.g. `https://mcp.example.com/mcp`) or, if you are a managed-service customer, `https://mcp.dbtrail.com/mcp`
4. Claude auto-discovers the OAuth endpoints, opens the login page
5. Enter your **tenant ID** and click **Authorize**
6. Done — the `query`, `recover`, `status`, and `list_schema_changes` tools are now available

This works from the Claude web app, Claude Desktop, and Claude mobile. Token refresh happens automatically — sessions survive indefinitely without re-authenticating.

### HTTP (for remote access — Claude Desktop)

```sh
BINTRAIL_INDEX_DSN='user:pass@tcp(127.0.0.1:3306)/binlog_index' \
  bintrail-mcp --http :8080
```

HTTP mode starts a persistent HTTP server using the MCP Streamable HTTP spec (2025-03-26). It serves at `/mcp`; the SDK manages session state via the `Mcp-Session-Id` response header. This is useful when Claude Desktop runs on your laptop but the bintrail server runs on a remote machine.

---

## `proxy.py`: Legacy Bridge for Claude Desktop

> **Note:** For new setups, use the [Claude Connector](#claude-connector-for-claudeai-desktop-and-mobile) method instead — it's simpler, works from more clients, and supports OAuth. `proxy.py` is still available as a fallback for environments that can't use the gateway.

`proxy.py` bridges Claude Desktop (which speaks stdio) to a remote `bintrail-mcp --http` server:

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
