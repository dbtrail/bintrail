# Using Claude with dbtrail (MCP)

dbtrail ships an MCP server, `bintrail-mcp`, that lets **Claude** — in Claude
Code, Claude Desktop, or claude.ai — search your change history and draft
recoveries in plain English. Once connected, you can ask:

> "What got deleted from the `orders` table in the last 10 minutes?"
>
> "Generate the SQL to bring back customer 42."
>
> "What schema changes happened this week?"

It exposes four **read-only** tools — `query`, `recover` (generates reversal SQL,
never runs it), `status`, and `list_schema_changes` — and never writes to your
database.

> **First time?** If you run the web console, the shortest path is the
> [5-minute Connect-AI guide](connect-ai.md) — console URL + token + a
> one-click bundle, no JSON and no DSN. This page is the full reference.

---

## Before you start

You need two things on the machine where Claude runs:

1. **The `bintrail-mcp` binary.** It ships in the release archives, the
   `.deb`/`.rpm` (`bintrail` package), and the Docker image — or install it
   directly:
   ```sh
   go install github.com/dbtrail/dbtrail/cmd/bintrail-mcp@latest
   ```
2. **Your index DSN** — the same `BINTRAIL_INDEX_DSN` your dbtrail stack uses,
   e.g. `user:pass@tcp(127.0.0.1:3306)/binlog_index`.

Using the [one-click bundle](#claude-desktop-one-click) or
[bridge mode](#bridge-mode---connect)? Skip both — you only need the remote
endpoint's URL and token; the DSN stays on the server side.

---

## Connect Claude

Pick the setup that matches where you run Claude. **Claude Code is the simplest —
start there.**

### Claude Code (local)

Add an `.mcp.json` at your project root (or `~/.claude.json` to use it
everywhere):

```json
{
  "mcpServers": {
    "bintrail": {
      "command": "bintrail-mcp",
      "env": { "BINTRAIL_INDEX_DSN": "user:pass@tcp(127.0.0.1:3306)/binlog_index" }
    }
  }
}
```

Restart Claude Code. The `query`, `recover`, `status`, and `list_schema_changes`
tools appear — now ask: *"What changed in the orders table in the last hour?"*

> Working inside the dbtrail **source repo**? Use
> `"command": "go", "args": ["run", "./cmd/bintrail-mcp"]` instead — no separate
> install needed.

### Claude Desktop (one-click)

Every release publishes **MCP Bundle** artifacts — `dbtrail-<os>-<arch>.mcpb`
files on the [releases page](https://github.com/dbtrail/dbtrail/releases) —
that Claude Desktop installs without any JSON editing:

1. Download the `.mcpb` matching the machine where Claude Desktop runs.
2. Double-click it (or Claude Desktop → **Settings → Extensions**, drop the
   file in).
3. Fill in the two-field form:
   - **Console / MCP endpoint URL** — your bintrail MCP endpoint, e.g.
     `http://localhost:8090/mcp` (a `bintrail-mcp --http` server; keep the
     `/mcp` path)
   - **Access token** — sent as an `Authorization: Bearer` header; stored by
     Claude Desktop as a sensitive value

Under the hood the bundle runs the bundled `bintrail-mcp` in bridge mode
(`--connect`, next section), so it works against a LAN/VPN/tunneled deployment
— the endpoint never needs to be publicly exposed. No DSN appears anywhere:
the remote endpoint owns the database connection.

Release bundles currently cover the release build matrix (Linux amd64/arm64).
On macOS or Windows, build a host-native bundle from source with `make mcpb`
(output in `dist/mcpb/`), or configure bridge mode by hand as shown below.

### Bridge mode (`--connect`)

`bintrail-mcp --connect <url>` is what the bundle runs, and you can use it
directly with any stdio MCP client: the process serves MCP over stdio locally
and proxies every request to a remote bintrail Streamable-HTTP MCP endpoint,
forwarding the remote's tools verbatim.

```json
{
  "mcpServers": {
    "bintrail": {
      "command": "bintrail-mcp",
      "args": ["--connect", "http://192.168.1.10:8080/mcp", "--token", "YOUR-TOKEN"]
    }
  }
}
```

`--token` is optional (a plain `bintrail-mcp --http` server today has no
auth); when set, it is sent as `Authorization: Bearer`. `--connect` cannot be
combined with `--http` or `--tenant-dsns`, and no DSN is needed — the remote
end resolves it. If the endpoint is unreachable or the token is rejected, the
bridge exits non-zero with a one-line error (visible in Claude Desktop's MCP
logs) instead of hanging.

### Claude Desktop (local index)

Same idea, in Claude Desktop's config file
(`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "bintrail": {
      "command": "bintrail-mcp",
      "env": { "BINTRAIL_INDEX_DSN": "user:pass@tcp(127.0.0.1:3306)/binlog_index" }
    }
  }
}
```

Restart Claude Desktop.

### Index on another machine (Claude Desktop)

If the index runs on a different host, start the server **there** over HTTP:

```sh
BINTRAIL_INDEX_DSN='user:pass@tcp(127.0.0.1:3306)/binlog_index' \
  bintrail-mcp --http :8080
```

Then connect from your laptop with the one-click bundle or
[bridge mode](#bridge-mode---connect) above (`--connect
http://192.168.1.10:8080/mcp`). If you'd rather not install a binary locally,
the Python `proxy.py` does the same job — see
[proxy.py (remote bridge)](#proxypy-remote-bridge) below.

### claude.ai and Claude mobile

Reaching your index from the web app or your phone needs an OAuth **gateway** in
front — an **advanced, optional** path. Most people use Claude Code or Claude
Desktop above instead. Two ways to get a gateway:

- **Self-host it (open source).** Build `cmd/mcp-gateway` and run it behind HTTPS
  on your own infrastructure and public domain.
- **dbtrail's hosted gateway (managed-service customers).** dbtrail operates one
  at `https://mcp.dbtrail.com/mcp` and provisions your tenant — this requires a
  dbtrail account; the open-source repo does not give you access to it.

Once a gateway is reachable at a public URL: in **claude.ai → Settings →
Integrations → Add custom integration**, enter the gateway URL (your domain, or
`https://mcp.dbtrail.com/mcp` for managed customers), complete the OAuth login
with your **tenant ID**, and the four tools appear. Works from web, Desktop, and
mobile; tokens refresh automatically.

---

## The four tools

| Tool | CLI equivalent | What it does |
|---|---|---|
| `query` | `bintrail query` | Search indexed row changes with filters |
| `recover` | `bintrail recover --dry-run` | Generate reversal SQL (never executes it) |
| `status` | `bintrail status` | Indexed files, partitions, and summary |
| `list_schema_changes` | reads `schema_changes` (see [DDL tracking](./ddl-tracking.md)) | DDL changes recorded while indexing/streaming, with the full statement and binlog coordinates |

All four are read-only — annotated `ReadOnlyHint: true` and `IdempotentHint: true`,
so the client knows they're safe to call repeatedly and never modify state.

The same four tools are also served by the web console at `/mcp` (Streamable
HTTP, console-token auth, per-server routing by URL path) — if you already run
`bintrail-console`, you may not need this binary at all; see
[console.md](console.md#mcp-endpoint).
`list_schema_changes` accepts `schema`, `table`, `ddl_type`
(`CREATE`/`ALTER`/`DROP`/`RENAME`/`TRUNCATE`, prefix-matched so `ALTER` matches
`ALTER TABLE`), `since`, `until`, and `limit` (default 100); results come back
newest-first.

Prompts that work well:

> "Someone deleted rows from `users` this afternoon — show me what and when."
>
> "Generate the SQL to undo the bad UPDATE on order 1234."
>
> "What was customer 42's row before the last change?"

---

## Tool parameters and behavior (reference)

The `query` and `recover` tools share the CLI's filters and validation. Beyond
the basics (`schema`, `table`, `pk`, `event_type`, `gtid`, `since`, `until`),
both accept:

- `changed_column` — events that touched a given column
- `column_eq` — repeatable `column=value` equality filters
- `flag` — table/column flag filter
- `profile` — RBAC table-deny + column-redaction
- `no_archive` — disable Parquet archive auto-routing (see below)

`query` additionally takes `format` (`json`, `table`, or `csv`). Time values
(`since` / `until`) accept MySQL datetime (`2006-01-02 15:04:05`), RFC 3339, or
date-only (`2006-01-02`) — the same formats as the CLI.

**Query row ceiling ([#654](https://github.com/dbtrail/dbtrail/issues/654)).** The
`query` tool caps an explicit `limit` at a hard ceiling (default **1,000,000
rows**; env `BINTRAIL_MCP_QUERY_MAX_LIMIT`) so an oversized request can't exhaust
the long-lived server's memory; when it caps, the returned text says so. A `limit`
of `0` or omitted falls back to the tool default (100), not unbounded — the
unbounded path is the `bintrail query` CLI, not the agent-facing tool. The
`recover` tool is **not** capped this way: it refuses oversized output on a memory
budget instead (see [Query and Recovery](query-and-recovery.md#recovery)).

**Archive auto-discovery.** `query` and `recover` automatically discover Parquet
archive sources from `archive_state` in the index database (or from the
`BINTRAIL_ARCHIVE_S3` + `BINTRAIL_ID` env vars). When archives are found, results
from MySQL and Parquet are merged, deduplicated, and sorted before output or SQL
generation. Pass `no_archive` to disable this.

**Index DSN.** The server connects to the index via the `BINTRAIL_INDEX_DSN`
environment variable (set once at startup) or the per-call `index_dsn` parameter,
which overrides the env var. Set the env var at startup so callers don't repeat
it on every call.

---

## proxy.py (remote bridge)

When the index runs on a different machine than Claude Desktop, `proxy.py`
bridges Claude Desktop (which speaks stdio) to a remote `bintrail-mcp --http`
server:

```
Claude Desktop  →  proxy.py (stdio, runs locally)  →  bintrail-mcp --http :8080  →  MySQL
```

It's a single Python file with zero dependencies (stdlib only, Python 3.7+).

1. Copy `proxy.py` (from `cmd/bintrail-mcp/proxy.py`) to your laptop.
2. Add it to `~/Library/Application Support/Claude/claude_desktop_config.json`:

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

3. Test connectivity before restarting Claude Desktop:

   ```sh
   BINTRAIL_SERVER=http://192.168.1.10:8080/mcp python3 ~/proxy.py <<'EOF'
   {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}
   {"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
   EOF
   ```

   Two JSON responses = working. No response or a connection error = check the
   server address and firewall.

**Stale session after a restart:** when `bintrail-mcp --http` restarts, existing
sessions are invalidated, but `proxy.py` still holds the old `Mcp-Session-Id` and
tool calls start failing. Fix: restart Claude Desktop (that restarts the proxy
and clears the session) — no need to restart the HTTP server.
