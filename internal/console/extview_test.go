package console

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
)

// These tests mutate the process-global ext.ConsoleView seam, so none may run
// in parallel; every install is restored with t.Cleanup. buildHandler reads the
// seam at construction time (route mount), so the provider is always installed
// BEFORE the server is built.

// stubViewProvider is a minimal ext.ConsoleViewProvider. dataHit records whether
// its DATA handler actually ran, so a test can prove the RBAC guard refuses
// BEFORE the provider handler (not merely that the response is 403).
type stubViewProvider struct {
	id      string
	dataHit *bool
}

func (p *stubViewProvider) ID() string    { return p.id }
func (p *stubViewProvider) Label() string { return "Example View" }
func (p *stubViewProvider) Script() string {
	return "/ext/" + p.id + "/view.js"
}

func (p *stubViewProvider) StaticHandler(string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "export function render(){}")
	})
}

func (p *stubViewProvider) DataHandler(_ string, resolve ext.ConsoleQueryContextFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.dataHit != nil {
			*p.dataHit = true
		}
		// Exercise the resolve wiring: the boot bundle resolves without error,
		// and the returned context must carry a fetch closure.
		if cqc, err := resolve(r); err == nil && cqc.Fetch == nil {
			http.Error(w, "resolve returned a nil Fetch", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
}

// newViewServer installs p (may be nil) and builds a token-authenticated server
// whose boot bundle wraps a sqlmock DB. deny drives the RBAC rules: a non-empty
// slice makes rbacActive() true. profileActiveOverride, when supplied, forces
// s.profileActive independently of the rule count — the only way to express the
// NAMED-but-zero-rule profile (#838 `--profile <typo>`) where profileActive is
// true but rbacActive() is false; otherwise profileActive derives from deny.
func newViewServer(t *testing.T, p ext.ConsoleViewProvider, deny []query.SchemaTable, profileActiveOverride ...bool) *Server {
	t.Helper()
	ext.SetConsoleView(p)
	t.Cleanup(func() { ext.SetConsoleView(nil) })

	db, _, closer := newSQLMock(t)
	t.Cleanup(closer)

	profileActive := len(deny) > 0
	if len(profileActiveOverride) > 0 {
		profileActive = profileActiveOverride[0]
	}
	s := &Server{token: "t", denyTables: deny, profileActive: profileActive, cm: newConnManager(nil, profileActive)}
	s.cm.boot = &bundle{db: db, engine: query.New(db), noArchive: true}
	s.mux = s.buildHandler()
	return s
}

func capsJSON(t *testing.T, s *Server) map[string]any {
	t.Helper()
	rec := getPath(t, s, "127.0.0.1:8090", "/api/capabilities", "t")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/capabilities = %d body=%s", rec.Code, rec.Body.String())
	}
	var caps map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &caps); err != nil {
		t.Fatalf("unmarshal capabilities: %v", err)
	}
	return caps
}

// (a) capabilities advertises the view with a provider installed, and omits the
// key entirely without one.
func TestExtensionViewCapabilitiesAdvertised(t *testing.T) {
	s := newViewServer(t, &stubViewProvider{id: "example"}, nil)
	caps := capsJSON(t, s)
	raw, ok := caps["extension_views"]
	if !ok {
		t.Fatalf("capabilities has no extension_views with a provider installed: %v", caps)
	}
	list, ok := raw.([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("extension_views = %v, want a single entry", raw)
	}
	v, _ := list[0].(map[string]any)
	if v["id"] != "example" || v["label"] != "Example View" || v["script"] != "/ext/example/view.js" {
		t.Errorf("extension_views[0] = %v, want {id:example,label:Example View,script:/ext/example/view.js}", v)
	}
}

func TestExtensionViewCapabilitiesOmittedWithoutProvider(t *testing.T) {
	s := newViewServer(t, nil, nil)
	caps := capsJSON(t, s)
	if _, ok := caps["extension_views"]; ok {
		t.Errorf("capabilities carries extension_views with no provider installed: %v", caps)
	}
}

// (b) the data route is reachable WITH a bearer token and 401 WITHOUT.
func TestExtensionViewDataHandlerRequiresAuth(t *testing.T) {
	hit := false
	s := newViewServer(t, &stubViewProvider{id: "example", dataHit: &hit}, nil)

	if rec := getPath(t, s, "127.0.0.1:8090", "/api/ext/example/ping", "t"); rec.Code != http.StatusOK {
		t.Errorf("data route with a bearer = %d body=%s, want 200", rec.Code, rec.Body.String())
	}
	if !hit {
		t.Error("provider data handler did not run on an authenticated request")
	}

	hit = false
	if rec := getPath(t, s, "127.0.0.1:8090", "/api/ext/example/ping", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("data route without a bearer = %d, want 401", rec.Code)
	}
	if hit {
		t.Error("provider data handler ran on an UNAUTHENTICATED request — tokenMiddleware did not gate it")
	}
}

// (c) under an active RBAC profile the data route is refused 403 BEFORE the
// provider handler runs.
func TestExtensionViewDataHandlerRefusedUnderRBAC(t *testing.T) {
	hit := false
	deny := []query.SchemaTable{{Schema: "app", Table: "secrets"}}
	s := newViewServer(t, &stubViewProvider{id: "example", dataHit: &hit}, deny)

	rec := getPath(t, s, "127.0.0.1:8090", "/api/ext/example/ping", "t")
	if rec.Code != http.StatusForbidden {
		t.Errorf("data route under an RBAC profile = %d body=%s, want 403", rec.Code, rec.Body.String())
	}
	if hit {
		t.Error("provider data handler ran under an RBAC profile — rbacViewGuard must refuse BEFORE it")
	}
	// And capabilities must not advertise the view under a profile.
	caps := capsJSON(t, s)
	if _, ok := caps["extension_views"]; ok {
		t.Errorf("capabilities advertises extension_views under an active RBAC profile: %v", caps)
	}
}

// (c2) a NAMED but ZERO-RULE profile (profileActive true, no deny/redact rules —
// the #838 `--profile <typo>` / empty-profile state) must ALSO refuse the data
// route and omit the view from capabilities. rbacActive() is false here, so a
// guard keyed on rule count would serve the surface and let a provider read
// query_text/query_hash straight off the raw cqc.DB — precisely what a named
// profile withholds on /api/events. The guard therefore keys on profileActive.
func TestExtensionViewDataHandlerRefusedUnderZeroRuleProfile(t *testing.T) {
	hit := false
	// deny is nil (rbacActive()==false) but the profile NAME is active.
	s := newViewServer(t, &stubViewProvider{id: "example", dataHit: &hit}, nil, true)
	if s.rbacActive() {
		t.Fatal("test precondition broken: rbacActive() must be false under a zero-rule profile")
	}

	rec := getPath(t, s, "127.0.0.1:8090", "/api/ext/example/ping", "t")
	if rec.Code != http.StatusForbidden {
		t.Errorf("data route under a zero-rule named profile = %d body=%s, want 403", rec.Code, rec.Body.String())
	}
	if hit {
		t.Error("provider data handler ran under a zero-rule named profile — rbacViewGuard must refuse on profileActive, not rbacActive()")
	}
	// And capabilities must not advertise the view under a named profile either.
	caps := capsJSON(t, s)
	if _, ok := caps["extension_views"]; ok {
		t.Errorf("capabilities advertises extension_views under a zero-rule named profile: %v", caps)
	}
}

// (d) the static route is reachable UNAUTHENTICATED (code always ships).
func TestExtensionViewStaticHandlerUnauthenticated(t *testing.T) {
	s := newViewServer(t, &stubViewProvider{id: "example"}, nil)
	rec := getPath(t, s, "127.0.0.1:8090", "/ext/example/view.js", "")
	if rec.Code != http.StatusOK {
		t.Errorf("static asset without a bearer = %d body=%s, want 200", rec.Code, rec.Body.String())
	}
}

// (e) with no provider both subtrees are absent from the mux (404), not a
// silently-served surface. The static probe uses the real asset path (with an
// extension) so the SPA-fallback does not turn it into a 200 index.html.
func TestExtensionViewRoutesAbsentWithoutProvider(t *testing.T) {
	s := newViewServer(t, nil, nil)
	if rec := getPath(t, s, "127.0.0.1:8090", "/api/ext/example/ping", "t"); rec.Code != http.StatusNotFound {
		t.Errorf("data route with no provider = %d, want 404 (route absent)", rec.Code)
	}
	if rec := getPath(t, s, "127.0.0.1:8090", "/ext/example/view.js", ""); rec.Code != http.StatusNotFound {
		t.Errorf("static asset with no provider = %d, want 404 (route absent)", rec.Code)
	}
}

// (f) a provider whose ID fails validation is skipped (not mounted) and does not
// panic at construction. "Example" is invalid (uppercase) per ^[a-z0-9-]+$.
func TestExtensionViewInvalidIDSkipped(t *testing.T) {
	s := newViewServer(t, &stubViewProvider{id: "Example"}, nil)
	// Not advertised.
	caps := capsJSON(t, s)
	if _, ok := caps["extension_views"]; ok {
		t.Errorf("capabilities advertises a view with an invalid id: %v", caps)
	}
	// Not mounted.
	if rec := getPath(t, s, "127.0.0.1:8090", "/api/ext/Example/ping", "t"); rec.Code != http.StatusNotFound {
		t.Errorf("data route for an invalid-id provider = %d, want 404 (not mounted)", rec.Code)
	}
	if rec := getPath(t, s, "127.0.0.1:8090", "/ext/Example/view.js", ""); rec.Code != http.StatusNotFound {
		t.Errorf("static route for an invalid-id provider = %d, want 404 (not mounted)", rec.Code)
	}
}

// (h) consoleQueryContext degrades gracefully when the selected server's index
// cannot be opened but a SOURCE is configured: it returns a usable context (nil
// DB, populated SourceDSN, a Fetch that surfaces the index error) rather than
// failing outright — so a provider's source-only endpoints keep working during
// an index outage (source up, index down — the incident a source-only view exists for).
func TestConsoleQueryContextSourceOnlyWhenIndexDown(t *testing.T) {
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatalf("LoadRegistry: %v", err)
	}
	// Index DSN parses but refuses fast (port 1); the source DSN is what a
	// source-only endpoint needs and must survive the failed index open.
	entry, err := reg.Add(ServerEntry{
		Name:      "prod",
		DSN:       "u:p@tcp(127.0.0.1:1)/idx_missing",
		SourceDSN: "r:p@tcp(src.example:3306)/",
	})
	if err != nil {
		t.Fatalf("reg.Add: %v", err)
	}
	s := &Server{token: "t", cm: newConnManager(reg, false)}

	r := httptest.NewRequest(http.MethodGet, "/api/ext/example/source-info", nil)
	r.Header.Set(serverHeader, entry.ID)

	qc, err := s.consoleQueryContext(r)
	if err != nil {
		t.Fatalf("consoleQueryContext returned an error for a configured source with a down index: %v", err)
	}
	if qc.DB != nil {
		t.Error("DB must be nil when the index could not be opened")
	}
	if qc.SourceDSN != entry.SourceDSN {
		t.Errorf("SourceDSN = %q, want %q (must survive the failed index open)", qc.SourceDSN, entry.SourceDSN)
	}
	if qc.Fetch == nil {
		t.Fatal("Fetch must be non-nil even when the index is down (an index-backed view calls it unconditionally)")
	}
	if _, _, ferr := qc.Fetch(r.Context(), query.Options{}); ferr == nil {
		t.Error("Fetch must return the index error when DB is nil, not a false empty result")
	}
}

// (h2) with NO source configured, a failed index open is a genuine error — there
// is nothing to serve, so the resolve func surfaces the (DSN-scrubbed) error.
func TestConsoleQueryContextErrorsWhenIndexDownAndNoSource(t *testing.T) {
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatalf("LoadRegistry: %v", err)
	}
	entry, err := reg.Add(ServerEntry{Name: "idx-only", DSN: "u:p@tcp(127.0.0.1:1)/idx_missing"})
	if err != nil {
		t.Fatalf("reg.Add: %v", err)
	}
	s := &Server{token: "t", cm: newConnManager(reg, false)}

	r := httptest.NewRequest(http.MethodGet, "/api/ext/example/index-info", nil)
	r.Header.Set(serverHeader, entry.ID)

	if _, err := s.consoleQueryContext(r); err == nil {
		t.Fatal("consoleQueryContext must error when the index is down and no source is configured")
	}
}

// (g) the DNS-rebinding host guard still covers the new routes: a domain Host is
// refused before either handler runs.
func TestExtensionViewRoutesBehindHostGuard(t *testing.T) {
	hit := false
	s := newViewServer(t, &stubViewProvider{id: "example", dataHit: &hit}, nil)

	if rec := getPath(t, s, "attacker.example", "/ext/example/view.js", ""); rec.Code != http.StatusForbidden {
		t.Errorf("static route with a domain Host = %d, want 403", rec.Code)
	}
	if rec := getPath(t, s, "attacker.example", "/api/ext/example/ping", "t"); rec.Code != http.StatusForbidden {
		t.Errorf("data route with a domain Host = %d, want 403", rec.Code)
	}
	if hit {
		t.Error("provider data handler ran despite a rejected Host")
	}
}
