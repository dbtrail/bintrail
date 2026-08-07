package console

// Console hardening backstops (#848): server timeout posture, the
// authenticated-API JSON body cap, the CSP header, and the probe log line.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestHTTPServerTimeouts pins the Serve timeout posture: header/read/idle
// timeouts set, WriteTimeout deliberately absent (it would sever /mcp SSE
// streams and long recover/verify responses mid-write — see httpServer).
func TestHTTPServerTimeouts(t *testing.T) {
	srv := newTestServer(t)
	hs := srv.httpServer()
	if hs.ReadHeaderTimeout != 10*time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want 10s", hs.ReadHeaderTimeout)
	}
	if hs.ReadTimeout != 30*time.Second {
		t.Errorf("ReadTimeout = %v, want 30s (slowloris backstop)", hs.ReadTimeout)
	}
	if hs.IdleTimeout != 2*time.Minute {
		t.Errorf("IdleTimeout = %v, want 2m", hs.IdleTimeout)
	}
	if hs.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, must stay unset (SSE /mcp + long responses)", hs.WriteTimeout)
	}
	if hs.Handler == nil {
		t.Error("httpServer must carry the console mux")
	}
}

// TestSecurityHeadersCSP asserts the CSP (and the pre-existing headers) are
// emitted through the real handler chain, on both an asset and an API route.
func TestSecurityHeadersCSP(t *testing.T) {
	srv := newTestServer(t)
	for _, path := range []string{"/", "/api/healthz"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://127.0.0.1:8090"+path, nil)
		srv.Handler().ServeHTTP(rec, req)
		csp := rec.Header().Get("Content-Security-Policy")
		// Pin the exact value: every directive is load-bearing (see the
		// securityHeaders comment), and blob: in script-src is what keeps the
		// ext-view module-import surface (console-e2e scenario 10) alive.
		const want = "default-src 'self'; script-src 'self' blob:; " +
			"style-src 'self' 'unsafe-inline'; img-src 'self' data:; " +
			"connect-src 'self'; frame-ancestors 'none'"
		if csp != want {
			t.Errorf("%s: CSP = %q, want %q", path, csp, want)
		}
		// script-src must NOT allow inline script — that is the invariant the
		// header exists to freeze.
		if strings.Contains(csp, "script-src 'self' 'unsafe-inline'") {
			t.Errorf("%s: CSP allows inline script: %q", path, csp)
		}
		// The pre-existing headers must survive the addition.
		if rec.Header().Get("X-Frame-Options") != "DENY" ||
			rec.Header().Get("X-Content-Type-Options") != "nosniff" ||
			rec.Header().Get("Referrer-Policy") != "no-referrer" {
			t.Errorf("%s: pre-existing security headers regressed: %v", path, rec.Header())
		}
	}
}

// TestAPIBodyCapRejectsOversizedJSON sends a syntactically VALID oversized
// JSON body through the real mux to a representative decode endpoint.
// Mutation rule: without the MaxBytesReader in apiGuard this body decodes
// fine (the unknown "pad" field is ignored) and the request proceeds — only
// the cap can produce the asserted 413.
func TestAPIBodyCapRejectsOversizedJSON(t *testing.T) {
	srv := newRegistryServer(t)
	body := `{"name":"big","host":"127.0.0.1","port":"3306","user":"u","dbname":"d","pad":"` +
		strings.Repeat("a", maxAPIBody) + `"}`
	rec, respBody := doServersReq(t, srv, "POST", "/api/servers", body)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: code = %d, want 413", rec.Code)
	}
	if !strings.Contains(string(respBody), "request body too large") {
		t.Errorf("413 body = %s, want the static too-large message", respBody)
	}
	// The overflow response must not echo decoder internals or the payload.
	if strings.Contains(string(respBody), "pad") {
		t.Errorf("413 body echoed request content: %s", respBody)
	}

	// A normal-sized request on the same endpoint still works — the cap must
	// not change behavior for legitimate clients.
	rec, respBody = doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"ok","host":"127.0.0.1","port":"3306","user":"u","dbname":"d"}`)
	if rec.Code != 201 {
		t.Fatalf("normal body after cap: code = %d, body = %s, want 201", rec.Code, respBody)
	}
}
