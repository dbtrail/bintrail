package console

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestServeShutsDownOnContextCancel covers the Listen()/Serve() split (the only
// path that binds a real socket and drains on ctx cancel — the rest of the
// suite goes through Handler()). It binds an ephemeral port, then asserts Serve
// returns nil (clean shutdown, not an error) once the context is cancelled.
func TestServeShutsDownOnContextCancel(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:0", Token: "x"})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := srv.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve returned %v, want nil on context cancel", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return within 10s of context cancellation")
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	// DB is nil: these tests exercise routing/middleware/assets only, never a
	// data handler.
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func TestMuxHealthzUnauthenticated(t *testing.T) {
	srv := newTestServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/healthz", nil)
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Errorf("healthz code = %d, want 200 (no token required)", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "ok") {
		t.Errorf("healthz body = %q", rec.Body.String())
	}
}

func TestMuxAPIRequiresToken(t *testing.T) {
	srv := newTestServer(t)
	for _, path := range []string{"/api/status", "/api/events", "/api/schemas", "/api/capabilities", "/api/reconstruct"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://127.0.0.1:8090"+path, nil)
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != 401 {
			t.Errorf("%s without token: code = %d, want 401", path, rec.Code)
		}
	}

	// POST /api/recover must also require the token — the bearer-header
	// requirement exists specifically to stop a cross-site form POST from
	// reaching it with ambient credentials (see auth.go).
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/recover",
		strings.NewReader(`{"schema":"app"}`))
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Errorf("POST /api/recover without token: code = %d, want 401", rec.Code)
	}
}

func TestMuxServesAssets(t *testing.T) {
	srv := newTestServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/", nil)
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("/ code = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "bintrail console") {
		t.Error("index.html (with 'bintrail console') was not served at /")
	}
}

func TestMuxRejectsForeignHost(t *testing.T) {
	srv := newTestServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/", nil)
	req.Host = "attacker.example" // DNS-rebinding attempt
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 403 {
		t.Errorf("foreign Host code = %d, want 403", rec.Code)
	}
}

func TestMuxNoCORSHeaders(t *testing.T) {
	srv := newTestServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/healthz", nil)
	srv.Handler().ServeHTTP(rec, req)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want empty (no CORS)", got)
	}
}

func TestURLContainsToken(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "abc123"})
	if err != nil {
		t.Fatal(err)
	}
	want := "http://127.0.0.1:8090/?token=abc123"
	if srv.URL() != want {
		t.Errorf("URL() = %q, want %q", srv.URL(), want)
	}
}

func TestDisplayHostRewritesWildcard(t *testing.T) {
	cases := map[string]string{
		"0.0.0.0:8090":      "127.0.0.1:8090",
		"127.0.0.1:8090":    "127.0.0.1:8090",
		"[::]:8090":         "[::1]:8090",        // wildcard IPv6 → loopback
		"[2001:db8::1]:443": "[2001:db8::1]:443", // IPv6 literal kept, bracketed
		"::":                "::",                // no port → returned as-is
	}
	for in, want := range cases {
		if got := displayHost(in); got != want {
			t.Errorf("displayHost(%q) = %q, want %q", in, got, want)
		}
	}
}
