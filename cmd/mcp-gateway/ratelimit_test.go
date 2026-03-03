package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRateLimiter_Allow(t *testing.T) {
	rl := NewRateLimiter(1 * time.Minute)
	defer rl.Stop()

	// First 3 requests should pass.
	for i := range 3 {
		if !rl.Allow("192.168.1.1", 3) {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}

	// 4th should be denied.
	if rl.Allow("192.168.1.1", 3) {
		t.Fatal("4th request should be denied")
	}

	// Different IP should be independent.
	if !rl.Allow("10.0.0.1", 3) {
		t.Fatal("different IP should be allowed")
	}
}

func TestRateLimiter_disabledWhenZero(t *testing.T) {
	rl := NewRateLimiter(1 * time.Minute)
	defer rl.Stop()

	for range 100 {
		if !rl.Allow("1.2.3.4", 0) {
			t.Fatal("limit=0 should allow all requests")
		}
	}
}

func TestRateLimitMiddleware_returns429(t *testing.T) {
	rl := NewRateLimiter(1 * time.Minute)
	defer rl.Stop()

	handler := RateLimitMiddleware(rl, 1, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First request passes.
	req := httptest.NewRequest(http.MethodPost, "/oauth/register", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Second request is rate-limited.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("expected Retry-After header")
	}
}

func TestRateLimitMiddleware_disabledWhenZero(t *testing.T) {
	rl := NewRateLimiter(1 * time.Minute)
	defer rl.Stop()

	handler := RateLimitMiddleware(rl, 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for range 10 {
		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		req.RemoteAddr = "1.2.3.4:1234"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
	}
}

func TestClientIP_xForwardedFor(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.50, 70.41.3.18, 150.172.238.178")
	if got := clientIP(req); got != "203.0.113.50" {
		t.Errorf("expected 203.0.113.50, got %s", got)
	}
}

func TestClientIP_remoteAddr(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:54321"
	if got := clientIP(req); got != "10.0.0.1" {
		t.Errorf("expected 10.0.0.1, got %s", got)
	}
}
