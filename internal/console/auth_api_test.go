package console

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// writeFastAuthFile writes a credential file with a MinCost hash directly —
// cost-12 hashing in every test would dominate the package's runtime. The
// login path's opportunistic rehash is exercised separately.
func writeFastAuthFile(t *testing.T, username, password string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "auth.yaml")
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("version: 1\nusername: "+username+"\npassword_bcrypt: "+string(hash)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// newPasswordServer builds a Server with password login configured. token may
// be "" (password-only mode).
func newPasswordServer(t *testing.T, token, username, password string) (*Server, string) {
	t.Helper()
	p := writeFastAuthFile(t, username, password)
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: token, AuthPath: p})
	if err != nil {
		t.Fatal(err)
	}
	return srv, p
}

func postJSON(t *testing.T, srv *Server, path, bearer, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090"+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

func loginToken(t *testing.T, srv *Server, username, password string) string {
	t.Helper()
	rec := postJSON(t, srv, "/api/auth/login", "", `{"username":"`+username+`","password":"`+password+`"}`)
	if rec.Code != 200 {
		t.Fatalf("login code = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Token == "" || resp.ExpiresAt == "" {
		t.Fatalf("login response missing fields: %s", rec.Body.String())
	}
	return resp.Token
}

func TestLoginIssuesWorkingSession(t *testing.T) {
	srv, _ := newPasswordServer(t, "secret", "admin", "correct-horse-battery")
	tok := loginToken(t, srv, "admin", "correct-horse-battery")
	if !strings.HasPrefix(tok, sessionPrefix) {
		t.Errorf("session token %q lacks the %s prefix", tok, sessionPrefix)
	}
	// The session is a real Bearer credential: an authed endpoint accepts it.
	if rec := postJSON(t, srv, "/api/auth/logout", tok, `{}`); rec.Code != 204 {
		t.Errorf("authed call with session = %d, want 204", rec.Code)
	}
}

func TestLoginFailureIsUniform(t *testing.T) {
	srv, _ := newPasswordServer(t, "secret", "admin", "correct-horse-battery")
	badPass := postJSON(t, srv, "/api/auth/login", "", `{"username":"admin","password":"wrong"}`)
	badUser := postJSON(t, srv, "/api/auth/login", "", `{"username":"nobody","password":"wrong"}`)
	if badPass.Code != 401 || badUser.Code != 401 {
		t.Fatalf("codes = %d/%d, want 401/401", badPass.Code, badUser.Code)
	}
	if badPass.Body.String() != badUser.Body.String() {
		t.Errorf("bad-password and bad-username bodies differ (enumeration oracle):\n%s\n%s", badPass.Body.String(), badUser.Body.String())
	}
}

func TestLoginUnknownUserRunsBcrypt(t *testing.T) {
	calls := 0
	orig := bcryptCompare
	bcryptCompare = func(hash, pw []byte) error { calls++; return orig(hash, pw) }
	t.Cleanup(func() { bcryptCompare = orig })

	srv, _ := newPasswordServer(t, "secret", "admin", "correct-horse-battery")
	postJSON(t, srv, "/api/auth/login", "", `{"username":"nobody","password":"x"}`)
	if calls == 0 {
		t.Error("unknown-username login skipped the bcrypt compare — timing oracle reintroduced")
	}
}

func TestLoginNotConfigured(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "secret", AuthPath: filepath.Join(t.TempDir(), "absent.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	if rec := postJSON(t, srv, "/api/auth/login", "", `{"username":"admin","password":"whatever1"}`); rec.Code != 403 {
		t.Errorf("login without an auth file = %d, want 403", rec.Code)
	}
	// And the probe reports it.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/auth", nil)
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"password_login":false`) {
		t.Errorf("GET /api/auth = %d %s, want password_login:false", rec.Code, rec.Body.String())
	}
}

func TestLoginContentTypeAndSize(t *testing.T) {
	srv, _ := newPasswordServer(t, "secret", "admin", "correct-horse-battery")

	// urlencoded (what a cross-site <form> can send) → 415, never verified.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/auth/login", strings.NewReader("username=admin&password=x"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 415 {
		t.Errorf("form-encoded login = %d, want 415", rec.Code)
	}

	// Oversized body → 413.
	huge := `{"username":"admin","password":"` + strings.Repeat("a", maxLoginBody) + `"}`
	if rec := postJSON(t, srv, "/api/auth/login", "", huge); rec.Code != 413 {
		t.Errorf("oversized login body = %d, want 413", rec.Code)
	}
}

func TestLoginRateLimited(t *testing.T) {
	srv, _ := newPasswordServer(t, "secret", "admin", "correct-horse-battery")
	for i := 0; i < ipShortMax; i++ {
		if rec := postJSON(t, srv, "/api/auth/login", "", `{"username":"admin","password":"wrong"}`); rec.Code != 401 {
			t.Fatalf("attempt %d = %d, want 401", i, rec.Code)
		}
	}
	rec := postJSON(t, srv, "/api/auth/login", "", `{"username":"admin","password":"wrong"}`)
	if rec.Code != 429 {
		t.Fatalf("throttled attempt = %d, want 429", rec.Code)
	}
	// Retry-After must be >= 1: the handler's int(seconds)+1 ceiling avoids a
	// "Retry-After: 0" that tells the client to retry immediately and re-trip.
	if ra := rec.Header().Get("Retry-After"); ra == "" || ra == "0" {
		t.Errorf("Retry-After = %q, want a positive integer", ra)
	}
	// The throttle also denies a CORRECT password (pre-bcrypt check), and the
	// static token keeps working — automation is never throttled.
	if rec := postJSON(t, srv, "/api/auth/login", "", `{"username":"admin","password":"correct-horse-battery"}`); rec.Code != 429 {
		t.Errorf("throttle must apply before verification: %d", rec.Code)
	}
	if rec := postJSON(t, srv, "/api/auth/logout", "secret", `{}`); rec.Code != 204 {
		t.Errorf("static token throttled: %d", rec.Code)
	}
}

func TestLogoutRevokesSession(t *testing.T) {
	srv, _ := newPasswordServer(t, "secret", "admin", "correct-horse-battery")
	tok := loginToken(t, srv, "admin", "correct-horse-battery")
	if rec := postJSON(t, srv, "/api/auth/logout", tok, `{}`); rec.Code != 204 {
		t.Fatalf("logout = %d, want 204", rec.Code)
	}
	if rec := postJSON(t, srv, "/api/auth/logout", tok, `{}`); rec.Code != 401 {
		t.Errorf("revoked session still authenticates: %d", rec.Code)
	}
	// Static-token logout is a 204 no-op and the token survives.
	if rec := postJSON(t, srv, "/api/auth/logout", "secret", `{}`); rec.Code != 204 {
		t.Errorf("static-token logout = %d, want 204", rec.Code)
	}
	if rec := postJSON(t, srv, "/api/auth/logout", "secret", `{}`); rec.Code != 204 {
		t.Errorf("static token revoked by logout: %d", rec.Code)
	}
}

func TestChangePasswordRequiresCurrent(t *testing.T) {
	srv, path := newPasswordServer(t, "secret", "admin", "correct-horse-battery")
	tok := loginToken(t, srv, "admin", "correct-horse-battery")

	// Wrong current password → 401, regardless of credential kind.
	for _, bearer := range []string{tok, "secret"} {
		rec := postJSON(t, srv, "/api/auth/password", bearer, `{"current_password":"wrong","new_password":"a-new-password"}`)
		if rec.Code != 401 {
			t.Errorf("change with wrong current (bearer %q) = %d, want 401", bearer[:4], rec.Code)
		}
	}
	// Policy violation → 422.
	rec := postJSON(t, srv, "/api/auth/password", tok, `{"current_password":"correct-horse-battery","new_password":"short"}`)
	if rec.Code != 422 {
		t.Errorf("short new password = %d, want 422", rec.Code)
	}

	// Success: second session dies, the caller's fresh token works, file verifies.
	other := loginToken(t, srv, "admin", "correct-horse-battery")
	rec = postJSON(t, srv, "/api/auth/password", tok, `{"current_password":"correct-horse-battery","new_password":"a-new-password"}`)
	if rec.Code != 200 {
		t.Fatalf("change = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Token string `json:"token"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if rec := postJSON(t, srv, "/api/auth/logout", other, `{}`); rec.Code != 401 {
		t.Error("password change did not revoke the other session")
	}
	if rec := postJSON(t, srv, "/api/auth/logout", resp.Token, `{}`); rec.Code != 204 {
		t.Error("fresh post-change session does not authenticate")
	}
	a, err := LoadAuthFile(path)
	if err != nil || !a.VerifyPassword("admin", "a-new-password") {
		t.Errorf("file does not verify the new password (err=%v)", err)
	}
}

func TestFirstSetRequiresStaticToken(t *testing.T) {
	// No auth file: the bootstrap trust root is the static token. A session
	// could only have come from a since-removed credential — it must NOT be
	// able to claim the first password (privilege escalation after
	// `user remove`).
	path := filepath.Join(t.TempDir(), "auth.yaml")
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "secret", AuthPath: path})
	if err != nil {
		t.Fatal(err)
	}
	// Forge the scenario: a session minted while a password existed, file
	// then removed out-of-band.
	stale, _, _ := srv.sessions.Issue()
	rec := postJSON(t, srv, "/api/auth/password", stale, `{"current_password":"","new_password":"hijacked-pass"}`)
	if rec.Code != 403 {
		t.Fatalf("first-set via session = %d, want 403", rec.Code)
	}
	// current_password must be empty on first set.
	rec = postJSON(t, srv, "/api/auth/password", "secret", `{"current_password":"x","new_password":"valid-password"}`)
	if rec.Code != 422 {
		t.Errorf("first-set with current_password = %d, want 422", rec.Code)
	}
	// Token-authenticated first set works and the credential verifies.
	rec = postJSON(t, srv, "/api/auth/password", "secret", `{"current_password":"","new_password":"valid-password"}`)
	if rec.Code != 200 {
		t.Fatalf("first-set via token = %d body=%s", rec.Code, rec.Body.String())
	}
	a, err := LoadAuthFile(path)
	if err != nil || !a.VerifyPassword("admin", "valid-password") {
		t.Errorf("first-set credential does not verify (err=%v)", err)
	}
}

func TestChangePasswordReadOnlyFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "auth.yaml")
	hash, _ := bcrypt.GenerateFromPassword([]byte("future-pass-1"), bcrypt.MinCost)
	os.WriteFile(p, []byte("version: 99\nusername: admin\npassword_bcrypt: "+string(hash)+"\n"), 0o600)
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "secret", AuthPath: p})
	if err != nil {
		t.Fatal(err)
	}
	rec := postJSON(t, srv, "/api/auth/password", "secret", `{"current_password":"future-pass-1","new_password":"replacement-1"}`)
	if rec.Code != 409 {
		t.Errorf("change on newer-version file = %d, want 409", rec.Code)
	}
}

func TestCapabilitiesReportAuth(t *testing.T) {
	srv, _ := newPasswordServer(t, "secret", "admin", "correct-horse-battery")
	srv.cm.boot = &bundle{} // a resolvable default so capabilities answers
	tok := loginToken(t, srv, "admin", "correct-horse-battery")

	get := func(bearer string) capabilitiesResponse {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/capabilities", nil)
		req.Header.Set("Authorization", "Bearer "+bearer)
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("capabilities = %d body=%s", rec.Code, rec.Body.String())
		}
		var caps capabilitiesResponse
		json.Unmarshal(rec.Body.Bytes(), &caps)
		return caps
	}
	caps := get(tok)
	if !caps.Auth.PasswordSet {
		t.Error("auth.password_set = false with a configured file")
	}
	if caps.Auth.AuthKind != "session" {
		t.Errorf("auth.auth_kind = %q for a session caller, want session", caps.Auth.AuthKind)
	}
	if got := get("secret"); got.Auth.AuthKind != "token" {
		t.Errorf("auth.auth_kind = %q for a token caller, want token", got.Auth.AuthKind)
	}
}

func TestLoginFailureNeverLogsUsername(t *testing.T) {
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })

	srv, _ := newPasswordServer(t, "secret", "admin", "correct-horse-battery")
	// Users mistype passwords into username fields — the attempted username
	// must never reach the logs.
	postJSON(t, srv, "/api/auth/login", "", `{"username":"my-actual-password-oops","password":"x"}`)
	if strings.Contains(buf.String(), "my-actual-password-oops") {
		t.Errorf("failed-login log contains the attempted username:\n%s", buf.String())
	}
}
