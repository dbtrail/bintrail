package console

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGenerateToken(t *testing.T) {
	a, err := generateToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 32 {
		t.Errorf("token length = %d, want 32 hex chars", len(a))
	}
	b, _ := generateToken()
	if a == b {
		t.Error("two generated tokens should differ")
	}
}

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

// TestNewTokenPolicy verifies the bind/token security policy: non-loopback
// binds demand an explicit token; loopback binds auto-generate one.
func TestNewTokenPolicy(t *testing.T) {
	if _, err := New(Config{Listen: "0.0.0.0:8090"}); err == nil {
		t.Error("non-loopback bind without a token should be refused")
	}

	srv, err := New(Config{Listen: "127.0.0.1:8090"})
	if err != nil {
		t.Fatalf("loopback bind: %v", err)
	}
	if srv.Token() == "" {
		t.Error("loopback bind should auto-generate a token")
	}

	srv2, err := New(Config{Listen: "0.0.0.0:9000", Token: "explicit-token"})
	if err != nil {
		t.Fatalf("non-loopback bind with token: %v", err)
	}
	if srv2.Token() != "explicit-token" {
		t.Errorf("Token() = %q, want explicit-token", srv2.Token())
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
