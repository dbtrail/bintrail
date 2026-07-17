package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// These tests mutate the process-global ext.ConsoleAuth seam, so none of them
// may run in parallel; every install is restored with t.Cleanup. New() reads
// the seam at construction time (route mount + bind policy), so the provider
// is always installed BEFORE console.New.

// fakeAuthProvider is a minimal ext.ConsoleAuthProvider: <prefix>start stands
// in for the login-initiation endpoint (a real provider redirects to its IdP
// here) and <prefix>finish stands in for the callback — it mints a session
// via issue and redirects to the SPA's existing /?token= bootstrap.
type fakeAuthProvider struct{}

func (fakeAuthProvider) DisplayName() string { return "Example SSO" }

func (fakeAuthProvider) Handler(prefix string, issue ext.ConsoleSessionIssuer) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+prefix+"start", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET "+prefix+"finish", func(w http.ResponseWriter, r *http.Request) {
		token, _, err := issue("tester@example.com")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/?token="+url.QueryEscape(token), http.StatusFound)
	})
	return mux
}

func installFakeProvider(t *testing.T) {
	t.Helper()
	ext.SetConsoleAuth(fakeAuthProvider{})
	t.Cleanup(func() { ext.SetConsoleAuth(nil) })
}

// newExtServer builds a loopback, credential-less server (first-run state)
// with an isolated auth path so the host's real credential file never leaks in.
func newExtServer(t *testing.T) *Server {
	t.Helper()
	srv, err := New(Config{Listen: "127.0.0.1:8090", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func getPath(t *testing.T, srv *Server, host, path, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://"+host+path, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// mintExtSession runs the fake provider's finish flow and extracts the session
// token from the /?token= redirect.
func mintExtSession(t *testing.T, srv *Server) string {
	t.Helper()
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/auth/ext/finish", "")
	if rec.Code != http.StatusFound {
		t.Fatalf("finish = %d body = %s, want 302", rec.Code, rec.Body.String())
	}
	loc := rec.Header().Get("Location")
	u, err := url.Parse(loc)
	if err != nil || u.Path != "/" {
		t.Fatalf("finish redirected to %q, want the SPA bootstrap /?token=...", loc)
	}
	token := u.Query().Get("token")
	if token == "" {
		t.Fatalf("no token in redirect %q", loc)
	}
	return token
}

func TestExtAuthStartIsUnauthenticated(t *testing.T) {
	installFakeProvider(t)
	srv := newExtServer(t)
	// (a) the login-initiation endpoint is reachable with NO Authorization
	// header — it is the pre-auth entry point the login screen links to.
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/auth/ext/start", ""); rec.Code != http.StatusOK {
		t.Errorf("GET /api/auth/ext/start without a bearer = %d, want 200", rec.Code)
	}
}

func TestExtAuthRoutesBehindHostGuard(t *testing.T) {
	installFakeProvider(t)
	srv := newExtServer(t)
	// (b) the DNS-rebinding defense still covers provider routes: a domain
	// Host is refused before the provider handler ever runs.
	if rec := getPath(t, srv, "attacker.example", "/api/auth/ext/start", ""); rec.Code != http.StatusForbidden {
		t.Errorf("provider route with a domain Host = %d, want 403", rec.Code)
	}
}

func TestExtIssuedSessionPassesTokenMiddleware(t *testing.T) {
	installFakeProvider(t)
	srv := newExtServer(t)
	token := mintExtSession(t, srv)
	if !strings.HasPrefix(token, sessionPrefix) {
		t.Errorf("issued token %q lacks the %s session prefix — extSessionIssuer must mint real sessions", token, sessionPrefix)
	}
	// (c) the minted token is a working bearer on a tokenMiddleware-wrapped
	// endpoint (logout answers 204 only after the middleware admitted it).
	if rec := postJSON(t, srv, "/api/auth/logout", token, `{}`); rec.Code != http.StatusNoContent {
		t.Errorf("authed call with an ext-issued session = %d, want 204", rec.Code)
	}
}

func TestAuthInfoAdvertisesSSOWithProvider(t *testing.T) {
	installFakeProvider(t)
	srv := newExtServer(t)
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/auth", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/auth = %d", rec.Code)
	}
	var info map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if got := info["sso_name"]; got != "Example SSO" {
		t.Errorf("sso_name = %v, want %q", got, "Example SSO")
	}
	if got := info["sso_start"]; got != "/api/auth/ext/start" {
		t.Errorf("sso_start = %v, want /api/auth/ext/start", got)
	}
}

func TestAuthInfoOmitsSSOWithoutProvider(t *testing.T) {
	srv := newExtServer(t)
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/auth", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/auth = %d", rec.Code)
	}
	var info map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	// (d) both fields absent — not null, not empty-string — without a provider.
	for _, k := range []string{"sso_name", "sso_start"} {
		if _, ok := info[k]; ok {
			t.Errorf("/api/auth includes %q without a provider installed", k)
		}
	}
}

func TestExtRoutes401WithoutProvider(t *testing.T) {
	srv := newExtServer(t)
	// (e) with no provider the prefix falls into the tokenMiddleware-wrapped
	// /api/ catch-all: locked, never a silent 404 surface.
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/auth/ext/start", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("GET /api/auth/ext/start without a provider = %d, want 401", rec.Code)
	}
}

func TestNonLoopbackBindAllowedWithProviderOnly(t *testing.T) {
	// (f) a non-loopback bind with no token and no password is refused in the
	// stock build, but an installed provider is a valid sole credential path.
	cfg := Config{Listen: "0.0.0.0:8090", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")}
	if _, err := New(cfg); err == nil {
		t.Fatal("New() accepted a credential-less non-loopback bind without a provider")
	}
	installFakeProvider(t)
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() with a provider installed = %v, want success", err)
	}
	// The lifted refusal must NOT leak into first-run setup: a provider is a
	// credential path, not a trust assertion about who can reach the port, so
	// on this credential-less non-loopback bind the unauthenticated
	// POST /api/auth/setup stays closed — were it open, the first stranger to
	// reach the port could create the console password and own the console.
	// This pins the "It does NOT change willSetup/setupAllowed" invariant
	// stated in New(); without it, merging the provider check into
	// setupAllowed() would pass the whole suite.
	if rec := postJSON(t, srv, "/api/auth/setup", "", `{"username":"admin","password":"long-enough-pass"}`); rec.Code != http.StatusForbidden {
		t.Errorf("POST /api/auth/setup on a provider-only non-loopback bind = %d body = %s, want 403", rec.Code, rec.Body.String())
	}
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/auth", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/auth = %d", rec.Code)
	}
	var info map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if got, ok := info["setup"].(bool); !ok || got {
		t.Errorf("/api/auth setup = %v on a provider-only non-loopback bind, want false", info["setup"])
	}
}

func TestExtSessionCannotClaimFirstPassword(t *testing.T) {
	installFakeProvider(t)
	srv := newExtServer(t)
	token := mintExtSession(t, srv)
	// (g) the first-set branch of change-password keys on the STATIC token
	// (the bootstrap trust root); an ext-issued session is still just a
	// session and must be refused.
	rec := postJSON(t, srv, "/api/auth/password", token, `{"current_password":"","new_password":"long-enough-pass"}`)
	if rec.Code != http.StatusForbidden {
		t.Errorf("first password set with an ext session = %d body = %s, want 403", rec.Code, rec.Body.String())
	}
}
