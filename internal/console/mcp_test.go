package console

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

// mcpInitializeBody is a minimal, valid JSON-RPC initialize request — the one
// MCP exchange that never touches the index, so mux-level tests (auth,
// routing) can assert the endpoint accepted a session without a database.
const mcpInitializeBody = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`

// doMCP posts an initialize request to path with the given bearer token
// ("" = no Authorization header) and returns the recorder.
func doMCP(t *testing.T, s *Server, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090"+path, strings.NewReader(mcpInitializeBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)
	return rec
}

func TestMCP_requiresConfiguredToken(t *testing.T) {
	db, _, closer := newSQLMock(t)
	defer closer()
	// Password-only / no-auth posture: no static token configured.
	s := &Server{token: "", cm: newConnManager(nil, false)}
	s.cm.boot = &bundle{db: db, engine: query.New(db), noArchive: true}
	s.mux = s.buildHandler()

	rec := doMCP(t, s, "/mcp", "whatever")
	if rec.Code != 403 {
		t.Fatalf("token-less console /mcp = %d, want 403; body: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "BINTRAIL_CONSOLE_TOKEN") {
		t.Errorf("refusal must name the remediation, got: %s", rec.Body.String())
	}
}

func TestMCP_authMissingToken(t *testing.T) {
	db, _, closer := newSQLMock(t)
	defer closer()
	s := newBootServer(db)

	if rec := doMCP(t, s, "/mcp", ""); rec.Code != 401 {
		t.Fatalf("/mcp without credential = %d, want 401", rec.Code)
	}
}

func TestMCP_authWrongToken(t *testing.T) {
	db, _, closer := newSQLMock(t)
	defer closer()
	s := newBootServer(db)

	if rec := doMCP(t, s, "/mcp", "not-the-token"); rec.Code != 401 {
		t.Fatalf("/mcp with wrong token = %d, want 401", rec.Code)
	}
}

func TestMCP_authRightTokenInitializes(t *testing.T) {
	db, _, closer := newSQLMock(t)
	defer closer()
	s := newBootServer(db) // token "t"

	rec := doMCP(t, s, "/mcp", "t")
	if rec.Code != 200 {
		t.Fatalf("/mcp initialize = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "serverInfo") {
		t.Errorf("initialize response missing serverInfo: %s", rec.Body.String())
	}
}

func TestMCP_routingUnknownServer404(t *testing.T) {
	db, _, closer := newSQLMock(t)
	defer closer()
	s := newBootServer(db)

	for _, path := range []string{"/mcp/no-such-server", "/mcp/no-such-server/"} {
		rec := doMCP(t, s, path, "t")
		if rec.Code != 404 {
			// Errorf, not Fatalf: the slashed case is the one that catches a
			// renamed wildcard, and a failure on the bare path must not hide it.
			t.Errorf("%s = %d, want 404; body: %s", path, rec.Code, rec.Body.String())
			continue
		}
		if !strings.Contains(rec.Body.String(), "unknown server") {
			t.Errorf("%s 404 body should say unknown server, got: %s", path, rec.Body.String())
		}
	}
}

func TestMCP_routingByIDNameAndDefault(t *testing.T) {
	db, _, closer := newSQLMock(t)
	defer closer()
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(127.0.0.1:3306)/idx"})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{token: "t", cm: newConnManager(reg, false)}
	s.cm.boot = &bundle{db: db, engine: query.New(db), noArchive: true}
	s.mux = s.buildHandler()

	// Initialize never opens the server's connection, so a routing accept
	// (200) proves selector resolution without a live MySQL.
	// The slashed forms ride the {$} routes: "/mcp/" must resolve like bare
	// /mcp (empty PathValue) and "/mcp/<sel>/" like "/mcp/<sel>". This loop
	// alone cannot pin the wildcard NAME on the trailing-slash pattern (a
	// renamed wildcard yields an empty selector, which this fixture's boot
	// bundle happily serves as 200) — the slashed case in
	// TestMCP_routingUnknownServer404 is what kills that mutation, by
	// resolving to the healthy default instead of 404 (verified red).
	for _, path := range []string{"/mcp", "/mcp/" + entry.ID, "/mcp/prod", "/mcp/default", "/mcp/", "/mcp/" + entry.ID + "/", "/mcp/prod/", "/mcp/default/"} {
		if rec := doMCP(t, s, path, "t"); rec.Code != 200 {
			t.Errorf("%s initialize = %d, want 200; body: %s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestMCP_defaultWithNoServers404(t *testing.T) {
	// No boot entry, empty registry: nothing to route to.
	s := &Server{token: "t", cm: newConnManager(nil, false)}
	s.mux = s.buildHandler()

	if rec := doMCP(t, s, "/mcp", "t"); rec.Code != 404 {
		t.Fatalf("empty console /mcp = %d, want 404; body: %s", rec.Code, rec.Body.String())
	}
}

func TestMCP_hiddenBootStillBacksDefault(t *testing.T) {
	// Source-less watch posture: boot hidden, empty registry. The bare /mcp
	// endpoint must still resolve to the hidden boot bundle, mirroring
	// header-less API requests.
	db, _, closer := newSQLMock(t)
	defer closer()
	s := &Server{token: "t", cm: newConnManager(nil, false)}
	s.cm.hideBoot = true
	s.cm.boot = &bundle{db: db, engine: query.New(db), noArchive: true}
	s.mux = s.buildHandler()

	if rec := doMCP(t, s, "/mcp", "t"); rec.Code != 200 {
		t.Fatalf("hidden-boot /mcp initialize = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	// But the hidden boot must NOT be addressable as "default" — it is not a
	// selectable server anywhere else in the console either.
	if rec := doMCP(t, s, "/mcp/default", "t"); rec.Code != 404 {
		t.Fatalf("hidden-boot /mcp/default = %d, want 404", rec.Code)
	}
}

// TestMCP_capabilityGatedOnToken: /api/capabilities advertises mcp iff a
// token is configured (static here; the managed-token half of the condition
// is pinned by TestManagedToken_CapabilitiesManagedOnly), plus the running
// build version the Connect AI card derives release links from.
func TestMCP_capabilityGatedOnToken(t *testing.T) {
	db, _, closer := newSQLMock(t)
	defer closer()

	caps := func(s *Server) capabilitiesResponse {
		t.Helper()
		rec := httptest.NewRecorder()
		s.handleCapabilities(rec, httptest.NewRequest("GET", "/api/capabilities", nil))
		if rec.Code != 200 {
			t.Fatalf("capabilities = %d, want 200; body: %s", rec.Code, rec.Body.String())
		}
		var resp capabilitiesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// Token configured: mcp advertised, version passed through.
	s := newBootServer(db) // token "t"
	s.version = "1.2.3"
	got := caps(s)
	if !got.MCP {
		t.Error("mcp = false with a configured token, want true")
	}
	if got.Version != "1.2.3" {
		t.Errorf("version = %q, want %q", got.Version, "1.2.3")
	}

	// Password-only / no-auth posture (no static token): mcp must be false —
	// mcpHandler refuses every request there (TestMCP_requiresConfiguredToken),
	// so advertising it would point the card at a dead endpoint.
	s2 := &Server{token: "", cm: newConnManager(nil, false)}
	s2.cm.boot = &bundle{db: db, engine: query.New(db), noArchive: true}
	s2.mux = s2.buildHandler()
	got2 := caps(s2)
	if got2.MCP {
		t.Error("mcp = true with no token configured, want false")
	}
	if got2.Version != "" {
		t.Errorf("version = %q, want empty for an unversioned build", got2.Version)
	}
}

func TestMCP_hostGuardApplies(t *testing.T) {
	db, _, closer := newSQLMock(t)
	defer closer()
	s := newBootServer(db)

	req := httptest.NewRequest("POST", "http://attacker.example/mcp", strings.NewReader(mcpInitializeBody))
	req.Host = "attacker.example"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)
	if rec.Code != 403 {
		t.Fatalf("domain-name Host on /mcp = %d, want 403 (DNS-rebinding guard)", rec.Code)
	}
}
