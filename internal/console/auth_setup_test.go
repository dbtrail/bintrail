package console

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

// authInfo fetches the unauthenticated GET /api/auth probe.
func authInfo(t *testing.T, srv *Server) map[string]bool {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/auth", nil)
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("GET /api/auth = %d", rec.Code)
	}
	var out map[string]bool
	json.Unmarshal(rec.Body.Bytes(), &out)
	return out
}

func TestSetupCreatesPasswordAndSession(t *testing.T) {
	// Loopback, no credential → setup is open.
	path := filepath.Join(t.TempDir(), "auth.yaml")
	srv, err := New(Config{Listen: "127.0.0.1:8090", AuthPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if info := authInfo(t, srv); !info["setup"] || info["password_login"] {
		t.Fatalf("fresh loopback console: setup=%v password_login=%v, want true/false", info["setup"], info["password_login"])
	}

	rec := postJSON(t, srv, "/api/auth/setup", "", `{"username":"admin","password":"first-run-pass-1"}`)
	if rec.Code != 200 {
		t.Fatalf("setup = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct{ Token string }
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if !strings.HasPrefix(resp.Token, sessionPrefix) {
		t.Errorf("setup did not return a session token: %q", resp.Token)
	}
	// The returned session authenticates.
	if r := postJSON(t, srv, "/api/auth/logout", resp.Token, `{}`); r.Code != 204 {
		t.Errorf("setup session does not authenticate: %d", r.Code)
	}
	// The credential was written and verifies.
	a, err := LoadAuthFile(path)
	if err != nil || !a.VerifyPassword("admin", "first-run-pass-1") {
		t.Errorf("setup credential does not verify (err=%v)", err)
	}
}

func TestSetupSelfDisablesOncePasswordExists(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "already-set-pass") // no token, password configured
	// A password exists → setup must be closed, login open.
	if info := authInfo(t, srv); info["setup"] || !info["password_login"] {
		t.Fatalf("configured console: setup=%v password_login=%v, want false/true", info["setup"], info["password_login"])
	}
	if rec := postJSON(t, srv, "/api/auth/setup", "", `{"username":"admin","password":"hijack-attempt"}`); rec.Code != 403 {
		t.Errorf("setup on a configured console = %d, want 403", rec.Code)
	}
}

func TestSetupRefusedWithToken(t *testing.T) {
	// A static token is a credential → no setup (token mode).
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "tok", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	if authInfo(t, srv)["setup"] {
		t.Error("token-configured console should not be in setup")
	}
	if rec := postJSON(t, srv, "/api/auth/setup", "", `{"password":"whatever-pass"}`); rec.Code != 403 {
		t.Errorf("setup with a token configured = %d, want 403", rec.Code)
	}
}

func TestSetupPolicyEnforced(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	if rec := postJSON(t, srv, "/api/auth/setup", "", `{"password":"short"}`); rec.Code != 422 {
		t.Errorf("short setup password = %d, want 422", rec.Code)
	}
	// urlencoded body (a cross-site form) is rejected before any write.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/auth/setup", strings.NewReader("password=x"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 415 {
		t.Errorf("form-encoded setup = %d, want 415", rec.Code)
	}
}

func TestSetupAllowSetupNonLoopback(t *testing.T) {
	// Non-loopback + AllowSetup (the compose case) → setup is open.
	path := filepath.Join(t.TempDir(), "auth.yaml")
	srv, err := New(Config{Listen: "0.0.0.0:8090", AuthPath: path, AllowSetup: true})
	if err != nil {
		t.Fatal(err)
	}
	if !authInfo(t, srv)["setup"] {
		t.Fatal("non-loopback + AllowSetup should be in setup")
	}
	if rec := postJSON(t, srv, "/api/auth/setup", "", `{"password":"compose-setup-1"}`); rec.Code != 200 {
		t.Errorf("setup with AllowSetup = %d, want 200", rec.Code)
	}

	// Without AllowSetup, the same bind is refused at construction.
	if _, err := New(Config{Listen: "0.0.0.0:8090", AuthPath: filepath.Join(t.TempDir(), "x.yaml")}); err == nil {
		t.Error("non-loopback + no credential + no AllowSetup should be refused")
	}
}
