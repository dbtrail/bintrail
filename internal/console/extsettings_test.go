package console

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// These tests mutate the process-global ext extension registries, so none may
// run in parallel; every install is undone with ext.ResetForTest. buildHandler
// reads the registries at construction time (route mount), so providers are
// always installed BEFORE the server is built.

// stubSettingsProvider is a minimal ext.ConsoleSettingsProvider. dataHit records
// whether its DATA handler actually ran, so a test can prove a denial happened
// BEFORE the provider's handler (not merely that the response carried a 403),
// and seen captures the session context the console handed it.
type stubSettingsProvider struct {
	id      string
	dataHit *bool
	seen    *ext.ConsoleSettingsContext
}

func (p *stubSettingsProvider) ID() string     { return p.id }
func (p *stubSettingsProvider) Label() string  { return "Example Panel" }
func (p *stubSettingsProvider) Script() string { return "/ext-settings/" + p.id + "/panel.js" }

func (p *stubSettingsProvider) StaticHandler(string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/javascript")
		_, _ = io.WriteString(w, "export function render(){}")
	})
}

func (p *stubSettingsProvider) DataHandler(_ string, session ext.ConsoleSettingsContextFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.dataHit != nil {
			*p.dataHit = true
		}
		sc := session(r)
		if p.seen != nil {
			*p.seen = sc
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
}

// newSettingsServer installs providers and builds a real server with a static
// token and a session store (so a test can mint a scoped session).
func newSettingsServer(t *testing.T, settings []ext.ConsoleSettingsProvider, views []ext.ConsoleViewProvider) *Server {
	t.Helper()
	t.Cleanup(ext.ResetForTest)
	for _, p := range settings {
		ext.RegisterConsoleSettings(p)
	}
	for _, p := range views {
		ext.RegisterConsoleView(p)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "static-tok", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv
}

func reqPath(t *testing.T, srv *Server, method, path, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// scopedSession mints a session carrying exactly perms (and profile, which may
// be "" for none).
func scopedSession(t *testing.T, srv *Server, profile string, perms ...ext.Permission) string {
	t.Helper()
	tok, _, err := srv.sessions.IssueWithPolicy("ops@example.com",
		&ext.AccessPolicy{Permissions: perms, Profile: profile})
	if err != nil {
		t.Fatalf("IssueWithPolicy: %v", err)
	}
	return tok
}

func capsFor(t *testing.T, srv *Server, bearer string) map[string]any {
	t.Helper()
	rec := reqPath(t, srv, "GET", "/api/capabilities", bearer)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/capabilities = %d body=%s", rec.Code, rec.Body.String())
	}
	var caps map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &caps); err != nil {
		t.Fatalf("unmarshal capabilities: %v", err)
	}
	return caps
}

func settingsIDs(t *testing.T, caps map[string]any) []string {
	t.Helper()
	raw, ok := caps["extension_settings"]
	if !ok {
		return nil
	}
	list, ok := raw.([]any)
	if !ok {
		t.Fatalf("extension_settings = %v, want a list", raw)
	}
	ids := make([]string, 0, len(list))
	for _, e := range list {
		m, _ := e.(map[string]any)
		id, _ := m["id"].(string)
		ids = append(ids, id)
	}
	return ids
}

// The OSS build installs nothing: neither subtree exists, so a probe 404s rather
// than reaching a surface the stock binary never ships.
func TestSettingsPanelRoutesAbsentWithoutProvider(t *testing.T) {
	srv := newSettingsServer(t, nil, nil)
	if rec := reqPath(t, srv, "GET", "/api/ext-settings/users/list", "static-tok"); rec.Code != http.StatusNotFound {
		t.Errorf("data route with no provider = %d, want 404 (route absent)", rec.Code)
	}
	if rec := reqPath(t, srv, "GET", "/ext-settings/users/panel.js", ""); rec.Code != http.StatusNotFound {
		t.Errorf("static asset with no provider = %d, want 404 (route absent)", rec.Code)
	}
	if ids := settingsIDs(t, capsFor(t, srv, "static-tok")); len(ids) != 0 {
		t.Errorf("capabilities advertises %v with no provider installed", ids)
	}
}

// Static assets ship unauthenticated (code always ships, only data is gated);
// data routes require the bearer.
func TestSettingsPanelStaticUnauthenticatedDataAuthenticated(t *testing.T) {
	hit := false
	srv := newSettingsServer(t, []ext.ConsoleSettingsProvider{&stubSettingsProvider{id: "users", dataHit: &hit}}, nil)

	if rec := reqPath(t, srv, "GET", "/ext-settings/users/panel.js", ""); rec.Code != http.StatusOK {
		t.Errorf("static asset without a bearer = %d, want 200", rec.Code)
	}
	if rec := reqPath(t, srv, "GET", "/api/ext-settings/users/list", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("data route without a bearer = %d, want 401", rec.Code)
	}
	if hit {
		t.Fatal("provider data handler ran on an UNAUTHENTICATED request — tokenMiddleware did not gate it")
	}
	if rec := reqPath(t, srv, "GET", "/api/ext-settings/users/list", "static-tok"); rec.Code != http.StatusOK {
		t.Errorf("data route with a bearer = %d body=%s, want 200", rec.Code, rec.Body.String())
	}
	if !hit {
		t.Error("provider data handler did not run on an authenticated request")
	}
	// The host guard (DNS-rebinding defense) still covers both new subtrees.
	for _, path := range []string{"/ext-settings/users/panel.js", "/api/ext-settings/users/list"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://attacker.example"+path, nil)
		req.Header.Set("Authorization", "Bearer static-tok")
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s with a domain Host = %d, want 403", path, rec.Code)
		}
	}
}

// Permission gating, read side: a session holding settings:read reaches the
// panel; one holding an unrelated permission is refused BEFORE the handler runs.
func TestSettingsPanelGatedBySettingsRead(t *testing.T) {
	hit := false
	srv := newSettingsServer(t, []ext.ConsoleSettingsProvider{&stubSettingsProvider{id: "users", dataHit: &hit}}, nil)

	reader := scopedSession(t, srv, "", ext.PermSettingsRead)
	if rec := reqPath(t, srv, "GET", "/api/ext-settings/users/list", reader); rec.Code != http.StatusOK {
		t.Errorf("GET with settings:read = %d body=%s, want 200", rec.Code, rec.Body.String())
	}
	if !hit {
		t.Error("provider handler did not run for a session holding settings:read")
	}

	hit = false
	stranger := scopedSession(t, srv, "", ext.PermStatusRead)
	rec := reqPath(t, srv, "GET", "/api/ext-settings/users/list", stranger)
	if rec.Code != http.StatusForbidden {
		t.Errorf("GET without settings:read = %d, want 403", rec.Code)
	}
	if hit {
		t.Error("provider handler ran for a session lacking settings:read — authz must refuse BEFORE it")
	}
	if ids := settingsIDs(t, capsFor(t, srv, stranger)); len(ids) != 0 {
		t.Errorf("capabilities advertises %v to a session lacking settings:read (the nav item would 403)", ids)
	}
}

// Permission gating, write side: a read-only administrator must not be able to
// MUTATE through a panel. Method-classified, so a POST needs settings:write even
// though the same session may GET the panel.
func TestSettingsPanelMutationRequiresSettingsWrite(t *testing.T) {
	hit := false
	srv := newSettingsServer(t, []ext.ConsoleSettingsProvider{&stubSettingsProvider{id: "users", dataHit: &hit}}, nil)
	reader := scopedSession(t, srv, "", ext.PermSettingsRead)

	for _, method := range []string{"POST", "PUT", "DELETE", "PATCH"} {
		hit = false
		rec := reqPath(t, srv, method, "/api/ext-settings/users/roles", reader)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s with only settings:read = %d, want 403", method, rec.Code)
		}
		if hit {
			t.Errorf("%s reached the provider handler with only settings:read", method)
		}
	}

	writer := scopedSession(t, srv, "", ext.PermSettingsRead, ext.PermSettingsWrite)
	hit = false
	if rec := reqPath(t, srv, "POST", "/api/ext-settings/users/roles", writer); rec.Code != http.StatusOK {
		t.Errorf("POST with settings:write = %d body=%s, want 200", rec.Code, rec.Body.String())
	}
	if !hit {
		t.Error("provider handler did not run for a session holding settings:write")
	}
}

// The point of the issue: a settings panel is gated by PERMISSION, not by the
// data-profile guard that withholds an extension VIEW. An administrator whose
// session carries a data profile must still be able to administer.
func TestSettingsPanelReachableUnderAnActiveDataProfile(t *testing.T) {
	hit := false
	srv := newSettingsServer(t, []ext.ConsoleSettingsProvider{&stubSettingsProvider{id: "users", dataHit: &hit}}, nil)
	profiled := scopedSession(t, srv, "sensitive", ext.PermSettingsRead, ext.PermSettingsWrite)

	if rec := reqPath(t, srv, "GET", "/api/ext-settings/users/list", profiled); rec.Code != http.StatusOK {
		t.Errorf("GET under a data profile = %d body=%s, want 200 — a panel serves no row data, so the profile must not withhold it", rec.Code, rec.Body.String())
	}
	if !hit {
		t.Fatal("provider handler did not run for a profile-carrying session")
	}
	if ids := settingsIDs(t, capsFor(t, srv, profiled)); !slices.Contains(ids, "users") {
		t.Errorf("capabilities omits the panel for a profile-carrying session (ids=%v); the nav item must stay visible", ids)
	}
}

// The provider must learn WHO is calling and WHAT they hold, so it can enforce
// its own invariants (refusing a self-demotion is the case this exists for).
func TestSettingsPanelReceivesIdentityAndPermissions(t *testing.T) {
	var seen ext.ConsoleSettingsContext
	srv := newSettingsServer(t, []ext.ConsoleSettingsProvider{&stubSettingsProvider{id: "users", seen: &seen}}, nil)

	reader := scopedSession(t, srv, "", ext.PermSettingsRead)
	if rec := reqPath(t, srv, "GET", "/api/ext-settings/users/list", reader); rec.Code != http.StatusOK {
		t.Fatalf("GET = %d body=%s", rec.Code, rec.Body.String())
	}
	if seen.Identity != "ops@example.com" {
		t.Errorf("Identity = %q, want the session's verified login identity", seen.Identity)
	}
	if seen.FullAccess {
		t.Error("FullAccess = true for a policy-carrying session")
	}
	if !seen.Allows(ext.PermSettingsRead) || seen.Allows(ext.PermSettingsWrite) {
		t.Errorf("Permissions = %v, want exactly the session's grants", seen.Permissions)
	}

	// The static token is policy-less: full access, no identity. A panel keying
	// an invariant on identity must see "" rather than a wrong name.
	seen = ext.ConsoleSettingsContext{}
	if rec := reqPath(t, srv, "GET", "/api/ext-settings/users/list", "static-tok"); rec.Code != http.StatusOK {
		t.Fatalf("GET with the static token = %d", rec.Code)
	}
	if !seen.FullAccess || seen.Identity != "" {
		t.Errorf("static-token context = %+v, want FullAccess with an empty identity", seen)
	}
	if !seen.Allows(ext.PermSettingsWrite) {
		t.Error("a full-access session was denied settings:write")
	}
}

// A provider must not be able to rewrite another request's grants through the
// slice it was handed.
func TestSettingsContextPermissionsAreACopy(t *testing.T) {
	srv := newSettingsServer(t, nil, nil)
	pol := &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSettingsRead}}
	r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/ext-settings/users/list", nil)
	// Attach the policy the way tokenMiddleware does before the provider handler
	// runs; the policy object is the one the session store keeps.
	r = r.WithContext(context.WithValue(r.Context(), policyCtxKey{}, pol))

	sc := srv.consoleSettingsContext(r)
	sc.Permissions[0] = ext.PermServersDelete
	if pol.Permissions[0] != ext.PermSettingsRead {
		t.Error("mutating the context's Permissions rewrote the session's own policy — the slice must be copied")
	}
}

// Two panels coexist — the registry is additive, so both mount and both are
// advertised. This is what the single-slot seam could not do.
func TestSettingsPanelsCoexist(t *testing.T) {
	srv := newSettingsServer(t, []ext.ConsoleSettingsProvider{
		&stubSettingsProvider{id: "users"},
		&stubSettingsProvider{id: "keys"},
	}, nil)

	for _, id := range []string{"users", "keys"} {
		if rec := reqPath(t, srv, "GET", "/api/ext-settings/"+id+"/list", "static-tok"); rec.Code != http.StatusOK {
			t.Errorf("data route for %q = %d, want 200 (both panels must mount)", id, rec.Code)
		}
	}
	ids := settingsIDs(t, capsFor(t, srv, "static-tok"))
	if !slices.Contains(ids, "users") || !slices.Contains(ids, "keys") {
		t.Errorf("capabilities advertises %v, want both panels", ids)
	}
}

// Same for views, whose seam grew the registry: two providers coexist instead of
// the second silently replacing the first.
func TestExtensionViewsCoexist(t *testing.T) {
	srv := newSettingsServer(t, nil, []ext.ConsoleViewProvider{
		&stubViewProvider{id: "forensics"},
		&stubViewProvider{id: "audit"},
	})

	rec := reqPath(t, srv, "GET", "/api/capabilities", "static-tok")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/capabilities = %d", rec.Code)
	}
	var caps struct {
		Views []struct {
			ID string `json:"id"`
		} `json:"extension_views"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &caps); err != nil {
		t.Fatal(err)
	}
	if len(caps.Views) != 2 || caps.Views[0].ID != "forensics" || caps.Views[1].ID != "audit" {
		t.Errorf("extension_views = %v, want both providers in install order", caps.Views)
	}
	for _, id := range []string{"forensics", "audit"} {
		if rec := reqPath(t, srv, "GET", "/ext/"+id+"/view.js", ""); rec.Code != http.StatusOK {
			t.Errorf("static route for view %q = %d, want 200 (both views must mount)", id, rec.Code)
		}
	}
}

// An ID that is not ^[a-z0-9-]+$ flows into a URL path and a DOM route, so it is
// skipped: not mounted, not advertised — two different code paths, both asserted.
func TestSettingsPanelInvalidIDSkipped(t *testing.T) {
	srv := newSettingsServer(t, []ext.ConsoleSettingsProvider{
		&stubSettingsProvider{id: "Users"}, // uppercase
		&stubSettingsProvider{id: "keys"},
	}, nil)

	if rec := reqPath(t, srv, "GET", "/api/ext-settings/Users/list", "static-tok"); rec.Code != http.StatusNotFound {
		t.Errorf("data route for an invalid-id panel = %d, want 404 (not mounted)", rec.Code)
	}
	if rec := reqPath(t, srv, "GET", "/ext-settings/Users/panel.js", ""); rec.Code != http.StatusNotFound {
		t.Errorf("static route for an invalid-id panel = %d, want 404 (not mounted)", rec.Code)
	}
	ids := settingsIDs(t, capsFor(t, srv, "static-tok"))
	if slices.Contains(ids, "Users") {
		t.Errorf("capabilities advertises an invalid-id panel: %v", ids)
	}
	if !slices.Contains(ids, "keys") {
		t.Errorf("a valid sibling was dropped along with the invalid panel: %v", ids)
	}
}

// A duplicate ID must be skipped, not mounted twice: http.ServeMux.Handle PANICS
// on a repeated pattern, which would take the daemon down at construction.
func TestSettingsPanelDuplicateIDSkipped(t *testing.T) {
	first, second := false, false
	srv := newSettingsServer(t, []ext.ConsoleSettingsProvider{
		&stubSettingsProvider{id: "users", dataHit: &first},
		&stubSettingsProvider{id: "users", dataHit: &second},
	}, nil)

	if rec := reqPath(t, srv, "GET", "/api/ext-settings/users/list", "static-tok"); rec.Code != http.StatusOK {
		t.Fatalf("data route = %d, want 200", rec.Code)
	}
	if !first || second {
		t.Errorf("mounted handler: first=%v second=%v — the FIRST provider installed must win", first, second)
	}
	if ids := settingsIDs(t, capsFor(t, srv, "static-tok")); len(ids) != 1 {
		t.Errorf("capabilities advertises %v, want the duplicate collapsed to one entry", ids)
	}
}

func TestExtensionViewDuplicateIDSkipped(t *testing.T) {
	srv := newSettingsServer(t, nil, []ext.ConsoleViewProvider{
		&stubViewProvider{id: "forensics"},
		&stubViewProvider{id: "forensics"},
	})
	rec := reqPath(t, srv, "GET", "/ext/forensics/view.js", "")
	if rec.Code != http.StatusOK {
		t.Errorf("static route for a duplicated view id = %d, want 200 (first wins, second skipped)", rec.Code)
	}
}

// A panel and a view may share an ID: their mounts differ in the FIRST path
// segment, so neither shadows the other.
func TestSettingsPanelAndViewMayShareAnID(t *testing.T) {
	srv := newSettingsServer(t,
		[]ext.ConsoleSettingsProvider{&stubSettingsProvider{id: "users"}},
		[]ext.ConsoleViewProvider{&stubViewProvider{id: "users"}})

	if rec := reqPath(t, srv, "GET", "/api/ext-settings/users/list", "static-tok"); rec.Code != http.StatusOK {
		t.Errorf("panel data route = %d, want 200", rec.Code)
	}
	if rec := reqPath(t, srv, "GET", "/ext/users/view.js", ""); rec.Code != http.StatusOK {
		t.Errorf("view static route = %d, want 200", rec.Code)
	}
}

// The settings subtree must be classified by METHOD, and the view subtree must
// keep its own permission — a shared prefix branch would send every panel write
// through extview:read.
func TestPermForRouteExtSettingsPrefix(t *testing.T) {
	cases := []struct {
		method, path string
		want         ext.Permission
	}{
		{"GET", "/api/ext-settings/users/list", ext.PermSettingsRead},
		{"HEAD", "/api/ext-settings/users/list", ext.PermSettingsRead},
		{"POST", "/api/ext-settings/users/roles", ext.PermSettingsWrite},
		{"DELETE", "/api/ext-settings/users/roles/7", ext.PermSettingsWrite},
		{"PATCH", "/api/ext-settings/users", ext.PermSettingsWrite},
		// An unknown verb falls to the WRITE permission, not the read one: an
		// unclassified method must never be the cheap way to mutate.
		{"PROPFIND", "/api/ext-settings/users", ext.PermSettingsWrite},
		{"GET", "/api/ext/forensics/anything", ext.PermExtViewRead},
	}
	for _, c := range cases {
		got, classified := permForRoute(c.method, c.path)
		if !classified {
			t.Errorf("%s %s: not classified", c.method, c.path)
			continue
		}
		if got != c.want {
			t.Errorf("permForRoute(%s, %s) = %q, want %q", c.method, c.path, got, c.want)
		}
	}
}
