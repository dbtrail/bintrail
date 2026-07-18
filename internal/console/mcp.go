package console

import (
	"context"
	"crypto/subtle"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/mcptools"
)

// The console serves the four read-only MCP tools (query, recover, status,
// list_schema_changes) over Streamable HTTP (#1039):
//
//	/mcp                → the default server (same selection rules as the
//	                      rest of the console, including HideBoot semantics)
//	/mcp/{id-or-name}   → that registry server ("default" = the boot entry)
//
// MCP clients cannot reliably send custom headers, so the server choice lives
// in the URL path — mirroring how the flashback port routes by connection
// username — instead of the X-Bintrail-Server header the browser API uses.
//
// Auth: a console token as a Bearer credential, compared in constant time —
// either the static --token / BINTRAIL_CONSOLE_TOKEN or the UI-managed MCP
// token (#1052). The managed token authenticates HERE ONLY: its advertised
// scope is the read-only MCP tools, so tokenMiddleware does not accept it and
// it cannot drive the browser API. Like the flashback port, the endpoint
// requires a token to be CONFIGURED — login sessions are a browser credential
// and the bcrypt password store cannot authenticate a headless MCP client —
// so under password-only or no auth the endpoint refuses with an actionable
// error.
// The host-header allowlist and security headers apply like every route
// (the handler is mounted on the guarded root mux).
//
// Read boundary: tool calls resolve the per-server connManager bundle at call
// time, so the bundle's posture applies exactly as on the API — per-server
// no-archive (which already folds in the process RBAC profile), the
// process-global deny/redact rules, and the console result caps (events
// 100/1000, recover 1000/10000). query_text/query_hash are withheld from
// query results to match the events API's eventDTO, and the index_dsn /
// profile tool parameters are rejected: an authenticated MCP client must not
// point the console at an arbitrary DSN or vary the process's RBAC posture.
func (s *Server) mcpHandler() http.Handler {
	streamable := mcp.NewStreamableHTTPHandler(
		func(r *http.Request) *mcp.Server {
			// The selector was validated before delegation; a race with a
			// registry delete surfaces as ErrUnknownServer on the tool call.
			id, _ := s.mcpServerID(r.PathValue("server"))
			return s.newMCPServer(id)
		},
		nil,
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.token == "" && !s.managedTok.configured() {
			// Mirror the flashback-port precedent: no token configured means
			// MCP clients have no credential that could authenticate them —
			// refuse with the remediation, never serve open.
			writeJSONError(w, http.StatusForbidden,
				"the MCP endpoint requires a console token: generate one in Settings → Connect AI, "+
					"or start with --token / BINTRAIL_CONSOLE_TOKEN "+
					"(password login is a browser credential and cannot authenticate MCP clients)")
			return
		}
		got := bearerToken(r)
		staticOK := got != "" && s.token != "" && subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) == 1
		if !staticOK && !s.managedTok.matches(got) {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized: missing or invalid token")
			return
		}
		if _, ok := s.mcpServerID(r.PathValue("server")); !ok {
			writeJSONError(w, http.StatusNotFound,
				"unknown server: use /mcp for the default server, or /mcp/{id-or-name} for a server from the console registry")
			return
		}
		streamable.ServeHTTP(w, r)
	})
}

// mcpServerID maps the /mcp/{server} path selector to the canonical
// connManager id, reporting whether it names a selectable server. The empty
// selector (the bare /mcp endpoint) resolves like a header-less API request —
// the console default, falling back to a hidden boot bundle — and is valid
// whenever any server exists at all. Non-empty selectors accept a registry
// id, a registry display name, or "default" for the boot entry, exactly like
// the flashback port's username routing.
func (s *Server) mcpServerID(selector string) (string, bool) {
	if selector == "" {
		if s.cm.defaultID() != "" {
			return "", true
		}
		// Source-less watch with an empty registry: no default id, but
		// Resolve("") still lands on the hidden boot bundle.
		if b, _ := s.cm.bootInfo(); b != nil {
			return "", true
		}
		return "", false
	}
	return s.flashbackTarget(selector)
}

// newMCPServer builds the per-session MCP server bound to one console server
// selection. The bundle is resolved per tool call (lazily opening registry
// connections, exactly like the API), so a server edited or added mid-session
// behaves the same as on every other console surface.
func (s *Server) newMCPServer(id string) *mcp.Server {
	return mcptools.NewServer(mcptools.Config{
		Version: "console",
		Instructions: "Bintrail console MCP endpoint for querying indexed binlog events, " +
			"generating recovery SQL, and viewing index status. " +
			"The target server is chosen by URL path: /mcp for the console's default server, " +
			"/mcp/{id-or-name} for a named server from the console registry.",
		Resolve: func(ctx context.Context, _ string) (*mcptools.Target, error) {
			b, err := s.cm.Resolve(ctx, id)
			if err != nil {
				return nil, err
			}
			return &mcptools.Target{
				DB:     b.db,
				DBName: b.dbName,
				// The connection is owned by the connManager; never closed
				// per call, never schema-migrated (registry servers are
				// deliberately not EnsureSchema'd — the console's read-only
				// contract confines DDL to the command-line DSN).
				CloseDB:             false,
				EnsureSchema:        false,
				NoArchive:           b.noArchive,
				EnvArchiveDiscovery: false,
				DenyTables:          s.denyTables,
				RedactColumns:       s.redactCols,
				ProfileActive:       s.profileActive,
				Resolver:            b.resolver,
				ResolverLoaded:      true,
				RedactStatementText: true,
			}, nil
		},
		AllowDSNParam:     false,
		AllowProfileParam: false,
		QueryMaxLimit:     func() int { return eventsMaxLimit },
		RecoverMaxLimit:   recoverMaxLimit,
		AuditSurface:      "console-mcp",
	})
}
