package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
)

// grantBearerTransport injects a bearer token into every MCP HTTP request.
// (The integration build has its own bearerTransport; both files compile into
// the package under -tags integration, hence the distinct name.)
type grantBearerTransport struct{ token string }

func (b grantBearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(req)
}

// mcpGrantConnect opens a real Streamable HTTP MCP session against endpoint.
func mcpGrantConnect(t *testing.T, endpoint, token string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "authz-test", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:   endpoint,
		HTTPClient: &http.Client{Transport: grantBearerTransport{token}},
	}, nil)
	if err != nil {
		t.Fatalf("MCP client Connect: %v", err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func mcpGrantToolText(t *testing.T, res *mcp.CallToolResult) string {
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

// TestMCPToolPermsCoverCoreTools pins the tool→permission table against the
// tools the console actually registers: a core tool missing from mcpToolPerms
// would be misclassified into the extension bucket (PermExtViewRead), and a
// stale extra entry would gate a tool that no longer exists. Every mapped
// permission must also be one the core defines, mirroring the apiRoutePerms
// typo guard.
func TestMCPToolPermsCoverCoreTools(t *testing.T) {
	s := newManagedServer(t, "tok")
	ts := httptest.NewServer(s.mux)
	t.Cleanup(ts.Close)

	session := mcpGrantConnect(t, ts.URL+"/mcp", "tok")
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools.Tools) != len(mcpToolPerms) {
		t.Errorf("mcpToolPerms has %d entries, console registers %d tools — the two must track each other", len(mcpToolPerms), len(tools.Tools))
	}
	for _, tool := range tools.Tools {
		if _, ok := mcpToolPerms[tool.Name]; !ok {
			t.Errorf("core tool %q missing from mcpToolPerms — it would be misclassified as an extension tool", tool.Name)
		}
	}
	known := map[ext.Permission]bool{}
	for _, p := range ext.AllPermissions() {
		known[p] = true
	}
	for tool, perm := range mcpToolPerms {
		if !known[perm] {
			t.Errorf("mcpToolPerms[%q] = %q, which is not a permission the core defines", tool, perm)
		}
	}
}

// TestMCPManagedTokenGrantEnforcement drives #1124 through the real HTTP
// stack without a database: a settings:read-only session mints through the
// real handler (the mint gate is unchanged), and the minted token is refused
// every core tool BEFORE any index access — which is why no DB is needed for
// the denial path. A grant the minter does hold lets the call through the
// permission gate (it then fails on the fake DB, but never with the
// permission error).
func TestMCPManagedTokenGrantEnforcement(t *testing.T) {
	s := newManagedServer(t, "")
	ts := httptest.NewServer(s.mux)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	mint := func(bearer string) string {
		t.Helper()
		rec := doJSON(t, s, "POST", "/api/mcp-token", bearer)
		if rec.Code != 200 {
			t.Fatalf("mint = %d: %s", rec.Code, rec.Body.String())
		}
		var minted struct {
			Token string `json:"token"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &minted); err != nil || minted.Token == "" {
			t.Fatalf("mint response: %v (%s)", err, rec.Body.String())
		}
		return minted.Token
	}

	viewer, _, err := s.sessions.IssueWithPolicy("settings-viewer",
		&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSettingsRead}})
	if err != nil {
		t.Fatal(err)
	}
	scopedTok := mint(viewer)

	session := mcpGrantConnect(t, ts.URL+"/mcp", scopedTok)
	for tool, perm := range mcpToolPerms {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: map[string]any{}})
		if err != nil {
			t.Fatalf("CallTool %s: %v", tool, err)
		}
		text := mcpGrantToolText(t, res)
		if !res.IsError || !strings.Contains(text, string(perm)) || !strings.Contains(text, "forbidden") {
			t.Errorf("%s with settings:read-only token: want forbidden naming %s, got IsError=%v %q", tool, perm, res.IsError, text)
		}
	}

	// A minter that holds query:execute mints a token that passes the query
	// grant gate (the call then fails on the fake DB — asserted to NOT be the
	// permission denial) while recover stays forbidden.
	analyst, _, err := s.sessions.IssueWithPolicy("analyst",
		&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSettingsRead, ext.PermQueryExecute}})
	if err != nil {
		t.Fatal(err)
	}
	analystTok := mint(analyst)
	session2 := mcpGrantConnect(t, ts.URL+"/mcp", analystTok)
	res, err := session2.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("CallTool query (analyst): %v", err)
	}
	if strings.Contains(mcpGrantToolText(t, res), "forbidden") {
		t.Errorf("query with a query:execute token must pass the grant gate, got %q", mcpGrantToolText(t, res))
	}
	res, err = session2.CallTool(ctx, &mcp.CallToolParams{Name: "recover", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("CallTool recover (analyst): %v", err)
	}
	if !res.IsError || !strings.Contains(mcpGrantToolText(t, res), "recover:execute") {
		t.Errorf("recover with a query-only token: want forbidden recover:execute, got %q", mcpGrantToolText(t, res))
	}
}

// TestManagedTokenGrants_FileRoundTrip pins the storage semantics of #1124 at
// the file layer: a scoped mint records the grant list; a full-access mint
// records NO permissions field (byte-identical to a pre-grants legacy file,
// which therefore keeps the full read surface); a present-but-empty list
// grants nothing (never collapses to "absent"); and matches() pairs the
// digest with the grants from the same refresh.
func TestManagedTokenGrants_FileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-token.yaml")
	var m managedMCPToken

	// Scoped mint: grants recorded, matches returns the capped policy.
	tok, f, err := GenerateMCPToken(path, []ext.Permission{ext.PermSettingsRead, ext.PermQueryExecute})
	if err != nil {
		t.Fatal(err)
	}
	m.initFromDisk(path, f)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "permissions") || !strings.Contains(string(raw), string(ext.PermQueryExecute)) {
		t.Fatalf("scoped mint must record its grants, got: %s", raw)
	}
	ok, pol := m.matches(tok)
	if !ok || pol == nil {
		t.Fatalf("scoped token: matches = (%v, %v), want a match with a policy", ok, pol)
	}
	if !pol.Allows(ext.PermQueryExecute) || pol.Allows(ext.PermRecoverExecute) {
		t.Errorf("scoped policy: query=%v recover=%v, want true/false", pol.Allows(ext.PermQueryExecute), pol.Allows(ext.PermRecoverExecute))
	}

	// Full-access mint (nil grants): no permissions field on disk — the
	// legacy shape — and a nil (full-access) policy. The per-check re-read
	// picks up the rotation without a reload call.
	tok2, _, err := GenerateMCPToken(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "permissions") {
		t.Fatalf("full-access mint must not record a permissions field, got: %s", raw)
	}
	ok, pol = m.matches(tok2)
	if !ok || pol != nil {
		t.Errorf("full-access token: matches = (%v, %v), want (true, nil)", ok, pol)
	}
	if ok, _ := m.matches(tok); ok {
		t.Error("rotated-away scoped token still matches")
	}

	// Present-but-empty grants nothing — and survives the YAML round trip as
	// EMPTY, never as absent (which would mean full access).
	tok3, _, err := GenerateMCPToken(path, []ext.Permission{})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadMCPTokenFile(path)
	if err != nil || loaded == nil || loaded.Permissions == nil || len(*loaded.Permissions) != 0 {
		t.Fatalf("empty grants must load as present-and-empty, got %+v (err %v)", loaded, err)
	}
	ok, pol = m.matches(tok3)
	if !ok || pol == nil {
		t.Fatalf("empty-grant token: matches = (%v, %v), want a match with a policy", ok, pol)
	}
	for _, p := range ext.AllPermissions() {
		if pol.Allows(p) {
			t.Errorf("empty-grant policy allows %q, want nothing", p)
		}
	}
}
