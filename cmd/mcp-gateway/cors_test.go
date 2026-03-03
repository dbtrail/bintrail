package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORSMiddleware_allowedOrigin(t *testing.T) {
	handler := CORSMiddleware(
		[]string{"https://claude.ai", "https://claude.com"},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Origin", "https://claude.ai")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://claude.ai" {
		t.Errorf("expected Allow-Origin %q, got %q", "https://claude.ai", got)
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "Mcp-Session-Id" {
		t.Errorf("expected Expose-Headers %q, got %q", "Mcp-Session-Id", got)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestCORSMiddleware_disallowedOrigin(t *testing.T) {
	handler := CORSMiddleware(
		[]string{"https://claude.ai"},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Origin", "https://evil.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("expected no Allow-Origin header, got %q", got)
	}
	// Request still goes through — origin validation is handled by OriginMiddleware.
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestCORSMiddleware_preflight(t *testing.T) {
	handler := CORSMiddleware(
		[]string{"https://claude.ai"},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("next handler should not be called for OPTIONS")
		}),
	)

	req := httptest.NewRequest(http.MethodOptions, "/mcp", nil)
	req.Header.Set("Origin", "https://claude.ai")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got == "" {
		t.Error("expected Allow-Methods header on preflight")
	}
}

func TestCORSMiddleware_noOrigin(t *testing.T) {
	handler := CORSMiddleware(
		[]string{"https://claude.ai"},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	// No Origin header — non-browser client.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("expected no Allow-Origin header for originless request, got %q", got)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}
