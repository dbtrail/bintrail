package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// fakeSink captures audit events for assertions. Safe for concurrent use
// per the AuditSink contract.
type fakeSink struct {
	mu     sync.Mutex
	events []ext.AuditEvent
}

func (f *fakeSink) Record(_ context.Context, ev ext.AuditEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
}

func (f *fakeSink) all() []ext.AuditEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ext.AuditEvent{}, f.events...)
}

func installFakeSink(t *testing.T) *fakeSink {
	t.Helper()
	s := &fakeSink{}
	ext.SetAuditSink(s)
	t.Cleanup(func() { ext.SetAuditSink(nil) })
	return s
}

// TestAuthzDenialIsAudited pins that a permission denial emits one audit
// event carrying the session's verified identity, the route, and the
// missing permission — and that ALLOWED requests emit nothing here.
func TestAuthzDenialIsAudited(t *testing.T) {
	sink := installFakeSink(t)
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "static-tok", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	viewer := &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermStatusRead, ext.PermServersRead}}
	tok, _, err := srv.sessions.IssueWithPolicy("ana@example.com", viewer)
	if err != nil {
		t.Fatal(err)
	}

	// Allowed request → no denial event.
	getPath(t, srv, "127.0.0.1:8090", "/api/capabilities", tok)
	if n := len(sink.all()); n != 0 {
		t.Fatalf("allowed request emitted %d audit events, want 0", n)
	}

	// Denied request → exactly one event with identity + route + permission.
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events", tok); rec.Code != http.StatusForbidden {
		t.Fatalf("scoped GET /api/events = %d, want 403", rec.Code)
	}
	evs := sink.all()
	if len(evs) != 1 {
		t.Fatalf("denial emitted %d events, want 1: %+v", len(evs), evs)
	}
	ev := evs[0]
	if ev.Surface != "console" || ev.Action != "authz.denied" {
		t.Errorf("event surface/action = %s/%s", ev.Surface, ev.Action)
	}
	if ev.Actor != "ana@example.com" {
		t.Errorf("event actor = %q, want the session's verified identity", ev.Actor)
	}
	if ev.Detail["path"] != "/api/events" || ev.Detail["method"] != "GET" || ev.Detail["missing_permission"] != string(ext.PermQueryExecute) {
		t.Errorf("event detail = %v", ev.Detail)
	}

	// The static token (policy-less) is never denied and never audited here.
	getPath(t, srv, "127.0.0.1:8090", "/api/events", "static-tok")
	for _, e := range sink.all()[1:] {
		if e.Action == "authz.denied" {
			t.Errorf("policy-less request produced a denial event: %+v", e)
		}
	}
}

// TestProfileGateDenialIsAudited pins the profile-gate audit arms: the
// nonexistent-profile 403 and an unredactable-surface 403 both emit
// profile.denied with the identity attached.
func TestProfileGateDenialIsAudited(t *testing.T) {
	sink := installFakeSink(t)
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "static-tok", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	tok, _, err := srv.sessions.IssueWithPolicy("sam@example.com",
		&ext.AccessPolicy{Permissions: ext.AllPermissions(), Profile: "sensitive"})
	if err != nil {
		t.Fatal(err)
	}

	// Unredactable surface → profile.denied. recover-cascade's gate fires
	// BEFORE server resolution, so it needs no configured index.
	if rec := postJSON(t, srv, "/api/recover-cascade", tok, `{"schema":"app","table":"t","pk":"1"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("profiled POST /api/recover-cascade = %d, want 403", rec.Code)
	}
	evs := sink.all()
	if len(evs) != 1 || evs[0].Action != "profile.denied" || evs[0].Actor != "sam@example.com" {
		t.Fatalf("cascade denial events = %+v, want one profile.denied by sam", evs)
	}
	if evs[0].Detail["surface_gate"] != "recover-cascade" {
		t.Errorf("detail = %v", evs[0].Detail)
	}
}

// TestRoleShapedMatrix drives four representative permission-set shapes
// (nested subsets, weakest to strongest) across the route families and
// pins allow/deny per shape — the enforcement matrix an embedding
// distribution's role ladder maps onto.
func TestRoleShapedMatrix(t *testing.T) {
	read := []ext.Permission{ext.PermStatusRead, ext.PermServersRead}
	analyze := append(append([]ext.Permission{}, read...), ext.PermQueryExecute, ext.PermReconstructExecute, ext.PermExtViewRead)
	operate := append(append([]ext.Permission{}, analyze...), ext.PermRecoverExecute, ext.PermBaselineCreate)
	admin := ext.AllPermissions()

	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "static-tok", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	mint := func(perms []ext.Permission) string {
		tok, _, err := srv.sessions.IssueWithPolicy("matrix-user", &ext.AccessPolicy{Permissions: perms})
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	toks := map[string]string{"read": mint(read), "analyze": mint(analyze), "operate": mint(operate), "admin": mint(admin)}

	// method, path → the weakest shape that may call it. Routes chosen one
	// per family; authz is checked BEFORE handlers run, so a 403 vs not-403
	// distinction is all this matrix needs (handlers may 4xx/5xx later).
	cases := []struct {
		method, path string
		minShape     string // "read" | "analyze" | "operate" | "admin"
	}{
		{"GET", "/api/status", "read"},
		{"GET", "/api/servers", "read"},
		{"GET", "/api/events", "analyze"},
		{"GET", "/api/reconstruct", "analyze"},
		{"POST", "/api/recover", "operate"},
		{"POST", "/api/servers/x/baseline", "operate"},
		{"POST", "/api/servers", "admin"},
		{"DELETE", "/api/servers/x", "admin"},
		{"PUT", "/api/rotation", "admin"},
		{"GET", "/api/storage", "admin"},
	}
	rank := map[string]int{"read": 0, "analyze": 1, "operate": 2, "admin": 3}
	for _, tc := range cases {
		for shape, tok := range toks {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, "http://127.0.0.1:8090"+tc.path, nil)
			req.Host = "127.0.0.1:8090"
			req.Header.Set("Authorization", "Bearer "+tok)
			srv.Handler().ServeHTTP(rec, req)
			wantAllowed := rank[shape] >= rank[tc.minShape]
			// Handlers may 403 for their own reasons on this bare server (no
			// monitor/baseline controllers, read-only rotation); only the authz
			// denial body identifies a ROLE denial.
			gotAllowed := !(rec.Code == http.StatusForbidden && strings.Contains(rec.Body.String(), "your role lacks"))
			if gotAllowed != wantAllowed {
				t.Errorf("%s %s as %s: code=%d, want allowed=%v", tc.method, tc.path, shape, rec.Code, wantAllowed)
			}
		}
	}
}

// TestExtViewsSuppressedWithoutExtViewRead pins the capability listing: a
// session lacking extview:read gets NO extension_views advertised (the
// data routes would 403), while a full-access session is unaffected.
func TestExtViewsSuppressedWithoutExtViewRead(t *testing.T) {
	ext.SetConsoleView(fakeView{})
	t.Cleanup(func() { ext.SetConsoleView(nil) })

	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "static-tok", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	noExt, _, _ := srv.sessions.IssueWithPolicy("v", &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermStatusRead, ext.PermServersRead}})
	withExt, _, _ := srv.sessions.IssueWithPolicy("a", &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermStatusRead, ext.PermExtViewRead}})

	views := func(bearer string) []any {
		rec := getPath(t, srv, "127.0.0.1:8090", "/api/capabilities", bearer)
		var caps map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &caps); err != nil {
			t.Fatal(err)
		}
		vs, _ := caps["extension_views"].([]any)
		return vs
	}
	if vs := views(noExt); len(vs) != 0 {
		t.Errorf("session without extview:read sees extension_views = %v, want none", vs)
	}
	if vs := views(withExt); len(vs) != 1 {
		t.Errorf("session with extview:read sees %d views, want 1", len(vs))
	}
	if vs := views("static-tok"); len(vs) != 1 {
		t.Errorf("policy-less token sees %d views, want 1 (OSS unchanged)", len(vs))
	}
}

// fakeView is a minimal valid ConsoleViewProvider for the suppression test.
type fakeView struct{}

func (fakeView) ID() string    { return "fakeview" }
func (fakeView) Label() string { return "Fake" }
func (fakeView) Script() string {
	return "/ext/fakeview/view.js"
}
func (fakeView) StaticHandler(string) http.Handler { return http.NotFoundHandler() }
func (fakeView) DataHandler(string, ext.ConsoleQueryContextFunc) http.Handler {
	return http.NotFoundHandler()
}

// TestSessionCarriesIdentity pins the identity round trip: minted with an
// identity, Lookup returns it; the built-in Issue() mints "" (anonymous).
func TestSessionCarriesIdentity(t *testing.T) {
	st := newSessionStore()
	tok, _, err := st.IssueWithPolicy("ana@example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	id, pol, ok := st.Lookup(tok)
	if !ok || id != "ana@example.com" || pol != nil {
		t.Errorf("Lookup = (%q, %v, %v)", id, pol, ok)
	}
	anon, _, _ := st.Issue()
	if id, _, _ := st.Lookup(anon); id != "" {
		t.Errorf("Issue() identity = %q, want empty", id)
	}
}
