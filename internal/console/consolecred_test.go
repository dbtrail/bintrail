package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// These tests mutate the process-global ext.ConsoleCredential seam, so none may
// run in parallel; every install is restored with t.Cleanup. New() reads the
// seam at construction (bind/setup policy), so the backend is installed BEFORE
// console.New.

// fakeCredBackend admits exactly one (user, pass) pair, attaching policy to the
// minted session. Any other pair is a uniform nil (bad user and bad pass are
// indistinguishable), mirroring the contract the real backend must honor.
type fakeCredBackend struct {
	user, pass string
	policy     *ext.AccessPolicy
}

func (f fakeCredBackend) Verify(username, password string) *ext.Credential {
	if username == f.user && password == f.pass {
		return &ext.Credential{Identity: username, Policy: f.policy}
	}
	return nil
}

func installCredBackend(t *testing.T, b ext.ConsoleCredentialProvider) {
	t.Helper()
	ext.SetConsoleCredentialProvider(b)
	t.Cleanup(func() { ext.SetConsoleCredentialProvider(nil) })
}

// newCredServer builds a loopback server whose only login authority is the
// installed backend (no auth file, no token).
func newCredServer(t *testing.T, b ext.ConsoleCredentialProvider) *Server {
	t.Helper()
	installCredBackend(t, b)
	srv, err := New(Config{Listen: "127.0.0.1:8090", AuthPath: filepath.Join(t.TempDir(), "absent.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func TestBackendLoginIssuesSession(t *testing.T) {
	srv := newCredServer(t, fakeCredBackend{user: "alice", pass: "s3cret-pass"})
	tok := loginToken(t, srv, "alice", "s3cret-pass")
	if !strings.HasPrefix(tok, sessionPrefix) {
		t.Errorf("backend-issued token %q lacks the %s prefix", tok, sessionPrefix)
	}
	// The minted token is a working Bearer on an authenticated endpoint.
	if rec := postJSON(t, srv, "/api/auth/logout", tok, `{}`); rec.Code != http.StatusNoContent {
		t.Errorf("authed call with a backend session = %d, want 204", rec.Code)
	}
}

func TestBackendLoginUniformFailure(t *testing.T) {
	srv := newCredServer(t, fakeCredBackend{user: "alice", pass: "s3cret-pass"})
	badPass := postJSON(t, srv, "/api/auth/login", "", `{"username":"alice","password":"wrong"}`)
	badUser := postJSON(t, srv, "/api/auth/login", "", `{"username":"nobody","password":"wrong"}`)
	if badPass.Code != http.StatusUnauthorized || badUser.Code != http.StatusUnauthorized {
		t.Fatalf("codes = %d/%d, want 401/401", badPass.Code, badUser.Code)
	}
	if badPass.Body.String() != badUser.Body.String() {
		t.Errorf("bad-password and bad-username bodies differ (enumeration oracle):\n%s\n%s", badPass.Body.String(), badUser.Body.String())
	}
}

// TestBackendSupersedesAuthFile pins the precedence decision: with a backend
// installed, the built-in auth file's credentials no longer log in — only the
// backend's do.
func TestBackendSupersedesAuthFile(t *testing.T) {
	installCredBackend(t, fakeCredBackend{user: "alice", pass: "s3cret-pass"})
	// newPasswordServer writes an admin/correct-horse-battery auth file and calls
	// New with the backend already installed.
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")

	// The file's own credentials are refused — the file is not consulted.
	if rec := postJSON(t, srv, "/api/auth/login", "", `{"username":"admin","password":"correct-horse-battery"}`); rec.Code != http.StatusUnauthorized {
		t.Errorf("auth-file login with a backend installed = %d, want 401 (backend supersedes)", rec.Code)
	}
	// The backend's credentials work.
	tok := loginToken(t, srv, "alice", "s3cret-pass")
	if !strings.HasPrefix(tok, sessionPrefix) {
		t.Errorf("backend login token %q lacks the %s prefix", tok, sessionPrefix)
	}
}

func TestBackendLoginRateLimited(t *testing.T) {
	srv := newCredServer(t, fakeCredBackend{user: "alice", pass: "s3cret-pass"})
	for i := 0; i < ipShortMax; i++ {
		if rec := postJSON(t, srv, "/api/auth/login", "", `{"username":"alice","password":"wrong"}`); rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i, rec.Code)
		}
	}
	// The throttle fires before Verify — even the correct password is denied.
	rec := postJSON(t, srv, "/api/auth/login", "", `{"username":"alice","password":"s3cret-pass"}`)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("throttled backend login = %d, want 429", rec.Code)
	}
}

// TestBackendLoginContentTypeAndSize pins that the CSRF/size guards still run
// BEFORE the backend's Verify.
func TestBackendLoginContentTypeAndSize(t *testing.T) {
	called := false
	srv := newCredServer(t, credProbe{onVerify: func() { called = true }})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/auth/login", strings.NewReader("username=alice&password=x"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Errorf("form-encoded backend login = %d, want 415", rec.Code)
	}
	huge := `{"username":"alice","password":"` + strings.Repeat("a", maxLoginBody) + `"}`
	if rec := postJSON(t, srv, "/api/auth/login", "", huge); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized backend login body = %d, want 413", rec.Code)
	}
	if called {
		t.Error("backend Verify ran despite a rejected content-type / oversized body")
	}
}

// credProbe records whether Verify was reached; it never admits.
type credProbe struct{ onVerify func() }

func (c credProbe) Verify(string, string) *ext.Credential {
	if c.onVerify != nil {
		c.onVerify()
	}
	return nil
}

// TestBackendAttachesPolicy ties #1076 to #1074: a policy the backend returns is
// enforced on the minted session.
func TestBackendAttachesPolicy(t *testing.T) {
	viewer := &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermStatusRead, ext.PermServersRead}}
	srv := newCredServer(t, fakeCredBackend{user: "viewer", pass: "s3cret-pass", policy: viewer})
	tok := loginToken(t, srv, "viewer", "s3cret-pass")
	// The scoped session is denied a route its policy lacks (query:execute).
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events", tok); rec.Code != http.StatusForbidden {
		t.Errorf("scoped backend session GET /api/events = %d, want 403", rec.Code)
	}
}

// TestBackendLiftsNonLoopbackBind pins that an installed backend is a valid sole
// credential path — it makes a credential-less non-loopback bind legal, like an
// external auth provider does.
func TestBackendLiftsNonLoopbackBind(t *testing.T) {
	cfg := Config{Listen: "0.0.0.0:8090", AuthPath: filepath.Join(t.TempDir(), "absent.yaml")}
	if _, err := New(cfg); err == nil {
		t.Fatal("New() accepted a credential-less non-loopback bind without a backend")
	}
	installCredBackend(t, fakeCredBackend{user: "alice", pass: "s3cret-pass"})
	if _, err := New(cfg); err != nil {
		t.Fatalf("New() with a backend installed = %v, want success", err)
	}
}

// TestBackendClosesSetup pins that an installed backend counts as a configured
// credential: the login form shows, first-run setup is closed, and the
// unauthenticated setup endpoint refuses.
func TestBackendClosesSetup(t *testing.T) {
	srv := newCredServer(t, fakeCredBackend{user: "alice", pass: "s3cret-pass"})

	rec := getPath(t, srv, "127.0.0.1:8090", "/api/auth", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/auth = %d", rec.Code)
	}
	var info map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if pw, _ := info["password_login"].(bool); !pw {
		t.Errorf("password_login = %v with a backend installed, want true", info["password_login"])
	}
	if setup, _ := info["setup"].(bool); setup {
		t.Errorf("setup = %v with a backend installed, want false", info["setup"])
	}
	// The unauthenticated setup endpoint is closed.
	if rec := postJSON(t, srv, "/api/auth/setup", "", `{"username":"admin","password":"long-enough-pass"}`); rec.Code != http.StatusForbidden {
		t.Errorf("POST /api/auth/setup with a backend installed = %d, want 403", rec.Code)
	}
}
