package console

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIsLoopbackAddr(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:8090":   true,
		"localhost:8090":   true,
		"[::1]:8090":       true,
		"127.0.0.1":        true,
		"0.0.0.0:8090":     false,
		":8090":            false,
		"192.168.1.5:8090": false,
		"[::]:8090":        false,
		"example.com:8090": false,
	}
	for addr, want := range cases {
		if got := isLoopbackAddr(addr); got != want {
			t.Errorf("isLoopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}

// TestNewTokenPolicy verifies the bind/credential policy: a non-loopback bind
// with no credential is refused; a loopback bind with no credential enters
// first-run setup (NO token is generated — password is the primary path); an
// explicit token always stands.
func TestNewTokenPolicy(t *testing.T) {
	if _, err := New(Config{Listen: "0.0.0.0:8090"}); err == nil {
		t.Error("non-loopback bind without a credential should be refused")
	}

	srv, err := New(Config{Listen: "127.0.0.1:8090"})
	if err != nil {
		t.Fatalf("loopback bind: %v", err)
	}
	if srv.Token() != "" {
		t.Errorf("loopback bind with no credential auto-generated a token %q — should enter setup instead", srv.Token())
	}
	if !srv.NeedsSetup() {
		t.Error("loopback bind with no credential should report NeedsSetup")
	}

	srv2, err := New(Config{Listen: "0.0.0.0:9000", Token: "explicit-token"})
	if err != nil {
		t.Fatalf("non-loopback bind with token: %v", err)
	}
	if srv2.Token() != "explicit-token" {
		t.Errorf("Token() = %q, want explicit-token", srv2.Token())
	}
	if srv2.NeedsSetup() {
		t.Error("a token-configured server is not in setup")
	}

	// AllowSetup makes a non-loopback bind legal and puts it in setup mode.
	srv3, err := New(Config{Listen: "0.0.0.0:9001", AllowSetup: true})
	if err != nil {
		t.Fatalf("non-loopback bind with AllowSetup: %v", err)
	}
	if !srv3.NeedsSetup() {
		t.Error("AllowSetup non-loopback bind should report NeedsSetup")
	}
}

func TestTokenMiddleware(t *testing.T) {
	srv := &Server{token: "secret"}
	pass := func() (http.Handler, *bool) {
		called := false
		h := srv.tokenMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusNoContent)
		}))
		return h, &called
	}

	t.Run("valid token passes", func(t *testing.T) {
		h, called := pass()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/x", nil)
		req.Header.Set("Authorization", "Bearer secret")
		h.ServeHTTP(rec, req)
		if !*called || rec.Code != http.StatusNoContent {
			t.Errorf("called=%v code=%d, want true/204", *called, rec.Code)
		}
	})

	for _, tc := range []struct{ name, auth string }{
		{"missing header", ""},
		{"wrong token", "Bearer nope"},
		{"not bearer", "secret"},
	} {
		t.Run(tc.name+" is rejected", func(t *testing.T) {
			h, called := pass()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/api/x", nil)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			h.ServeHTTP(rec, req)
			if *called || rec.Code != http.StatusUnauthorized {
				t.Errorf("called=%v code=%d, want false/401", *called, rec.Code)
			}
		})
	}
}

func TestHostGuard(t *testing.T) {
	srv := &Server{}
	h := srv.hostGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	cases := map[string]int{
		"127.0.0.1:8090":    http.StatusNoContent,
		"localhost:8090":    http.StatusNoContent,
		"[::1]:8090":        http.StatusNoContent,
		"evil.example:8090": http.StatusForbidden, // DNS-rebinding domain
		"attacker.com":      http.StatusForbidden,
		"":                  http.StatusForbidden,
	}
	for host, want := range cases {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.Host = host
		h.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Errorf("hostGuard Host=%q code = %d, want %d", host, rec.Code, want)
		}
	}
}

func TestHostGuardAllowlist(t *testing.T) {
	srv := &Server{allowedHosts: []string{"console.internal"}}
	h := srv.hostGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "console.internal:8090"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Errorf("allowlisted host code = %d, want 204", rec.Code)
	}
}
