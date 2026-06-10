package console

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestTokenMiddlewareEmptyTokenBypass pins THE load-bearing guard: password-
// only mode legitimately runs with s.token == "", and
// subtle.ConstantTimeCompare("", "") == 1 would wave every credential-less
// request through without the explicit empty-got and empty-token checks.
func TestTokenMiddlewareEmptyTokenBypass(t *testing.T) {
	srv := &Server{} // token "", nil sessions — fail closed everywhere
	h := srv.tokenMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler reached without a credential")
	}))
	for _, auth := range []string{"", "Bearer ", "Bearer x"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/x", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("auth %q: code = %d, want 401", auth, rec.Code)
		}
	}
}

func TestTokenMiddlewareAcceptsSessions(t *testing.T) {
	srv := &Server{token: "secret", sessions: newSessionStore()}
	tok, _, _ := srv.sessions.Issue()

	var kinds []authKind
	h := srv.tokenMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kinds = append(kinds, authKindFrom(r.Context()))
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, bearer := range []string{"secret", tok} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/x", nil)
		req.Header.Set("Authorization", "Bearer "+bearer)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Errorf("bearer %q: code = %d, want 204", bearer[:6], rec.Code)
		}
	}
	if len(kinds) != 2 || kinds[0] != authKindToken || kinds[1] != authKindSession {
		t.Errorf("authKinds = %v, want [token, session]", kinds)
	}

	// Both credentials valid simultaneously; an expired/revoked session 401s.
	srv.sessions.Revoke(tok)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/x", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("revoked session: code = %d, want 401", rec.Code)
	}
}

// TestNewPasswordModePolicy covers the rewritten bind/credential gate.
func TestNewPasswordModePolicy(t *testing.T) {
	authPath := writeFastAuthFile(t, "admin", "some-password-1")

	// Password configured + empty token: non-loopback binds become legal and
	// no token is auto-generated (it would leak into logs for nothing).
	srv, err := New(Config{Listen: "0.0.0.0:8090", AuthPath: authPath})
	if err != nil {
		t.Fatalf("password mode non-loopback: %v", err)
	}
	if srv.Token() != "" {
		t.Errorf("password mode auto-generated a token: %q", srv.Token())
	}
	if !srv.PasswordLogin() {
		t.Error("PasswordLogin() = false with a configured file")
	}
	if u := srv.URL(); strings.Contains(u, "token=") {
		t.Errorf("URL %q leaks a token in password mode", u)
	}

	// Explicit token + password: both stand, banner URL still tokenless.
	srv2, err := New(Config{Listen: "127.0.0.1:8090", Token: "tok", AuthPath: authPath})
	if err != nil {
		t.Fatal(err)
	}
	if srv2.Token() != "tok" {
		t.Errorf("explicit token dropped: %q", srv2.Token())
	}
	if u := srv2.URL(); strings.Contains(u, "token=") {
		t.Errorf("URL %q prints the automation token in password mode", u)
	}

	// Neither credential + non-loopback: refused, and the error teaches both
	// escape hatches.
	_, err = New(Config{Listen: "0.0.0.0:8090", AuthPath: filepath.Join(t.TempDir(), "absent.yaml")})
	if err == nil || !strings.Contains(err.Error(), "set-password") {
		t.Errorf("credential-less non-loopback bind: err = %v, want refusal mentioning set-password", err)
	}

	// Corrupt auth file: New fails loud rather than silently downgrading.
	bad := filepath.Join(t.TempDir(), "auth.yaml")
	os.WriteFile(bad, []byte("::"), 0o600)
	if _, err := New(Config{Listen: "127.0.0.1:8090", AuthPath: bad}); err == nil {
		t.Error("corrupt auth file accepted at boot")
	}
}

// writeSelfSigned generates a throwaway cert/key pair (no fixtures on disk).
func writeSelfSigned(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "bintrail-console-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	cf, _ := os.Create(certPath)
	pem.Encode(cf, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	cf.Close()
	kder, _ := x509.MarshalECPrivateKey(key)
	kf, _ := os.Create(keyPath)
	pem.Encode(kf, &pem.Block{Type: "EC PRIVATE KEY", Bytes: kder})
	kf.Close()
	return certPath, keyPath
}

func TestNewTLSValidation(t *testing.T) {
	cert, key := writeSelfSigned(t)

	// Both-or-neither.
	if _, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", TLSCert: cert}); err == nil {
		t.Error("--tls-cert without --tls-key accepted")
	}
	if _, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", TLSKey: key}); err == nil {
		t.Error("--tls-key without --tls-cert accepted")
	}
	// Unreadable files fail fast at New, not at first request.
	if _, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", TLSCert: "/nope.pem", TLSKey: "/nope.key"}); err == nil {
		t.Error("bad cert paths accepted")
	}

	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", TLSCert: cert, TLSKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(srv.URL(), "https://") {
		t.Errorf("URL() = %q, want https scheme under TLS", srv.URL())
	}
}

func TestMuxAuthEndpointsUnauthenticated(t *testing.T) {
	srv := newTestServer(t)
	// GET /api/auth needs no credential (the SPA boots from it).
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/auth", nil)
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Errorf("GET /api/auth = %d, want 200 without a token", rec.Code)
	}
	// POST /api/auth/login is reachable without a credential (403 here: no
	// auth file — the point is it is NOT a 401 from tokenMiddleware).
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "http://127.0.0.1:8090/api/auth/login", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code == 401 {
		t.Error("login is behind the token middleware — unreachable before authenticating")
	}
}

func TestMuxSecurityHeaders(t *testing.T) {
	srv := newTestServer(t)
	for _, path := range []string{"/", "/api/healthz"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://127.0.0.1:8090"+path, nil)
		srv.Handler().ServeHTTP(rec, req)
		for header, want := range map[string]string{
			"Referrer-Policy":        "no-referrer",
			"X-Content-Type-Options": "nosniff",
			"X-Frame-Options":        "DENY",
		} {
			if got := rec.Header().Get(header); got != want {
				t.Errorf("%s: %s = %q, want %q", path, header, got, want)
			}
		}
	}
}
