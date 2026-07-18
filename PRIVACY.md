# Privacy Policy — dbtrail Claude Desktop extension (`dbtrail.mcpb`)

*Last updated: 2026-07-18*

This policy covers the **dbtrail desktop extension** — the `.mcpb` bundle that
connects Claude Desktop to a self-hosted bintrail/dbtrail deployment — and the
`bintrail-mcp` bridge binary it runs. It also describes, for completeness, how
data moves when you use it.

## The short version

The extension is a local bridge to **your own infrastructure**. dbtrail (the
project and its maintainers) operates no servers in this flow, and **collects
nothing**: no telemetry, no analytics, no crash reports, no account, no
phone-home of any kind.

## What the extension does with data

- **Where it connects.** The bundled binary makes network connections to
  exactly one place: the console/MCP endpoint URL **you** configure at install
  time (your own bintrail console, on your machine, LAN, VPN, or server). It
  never connects to dbtrail, Anthropic, or any other third party on its own.
- **Your access token** is entered once in Claude Desktop's configuration form
  and stored by Claude Desktop as a sensitive value (in the operating system's
  credential store). The extension sends it only to the endpoint you
  configured, as an `Authorization: Bearer` header. It is never written to
  disk by the extension and never appears in its logs.
- **Query results** (row change history, recovery SQL, index status, schema
  changes) flow from your deployment, through the local bridge, into your
  Claude conversation. The extension does not store, cache, or copy them —
  each response exists only in the conversation that requested it.

## Data collection, usage, and storage

**We collect nothing.** There is no data collection practice to describe
beyond that: the extension has no telemetry and transmits nothing to the
dbtrail project. All indexed database history lives in the deployment you
operate, under your control and your retention rules (see the
[rotation documentation](docs/rotation-and-status.md)).

## Third-party sharing

None by the extension. One flow you should be aware of, because it is inherent
to using any AI assistant: tool results that enter your Claude conversation
are processed by **Anthropic** as conversation content, under
[Anthropic's own privacy policy](https://www.anthropic.com/legal/privacy).
If your change history contains sensitive row data, dbtrail's
[RBAC profiles](docs/query-and-recovery.md) and the console's redaction rules
let you limit what the MCP surface can return before it ever reaches a
conversation.

## Data retention

The extension retains no data. Uninstalling it from Claude Desktop removes the
bundle and its stored configuration; your deployment and its index are
untouched.

## Contact

Questions about this policy or the extension's data handling:
[open an issue](https://github.com/dbtrail/dbtrail/issues) on the public
repository.
