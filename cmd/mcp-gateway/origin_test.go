package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOriginMiddleware_allowedOrigin(t *testing.T) {
	handler := OriginMiddleware(
		[]string{"https://claude.ai", "https://claude.com"},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Origin", "https://claude.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestOriginMiddleware_rejectedOrigin(t *testing.T) {
	handler := OriginMiddleware(
		[]string{"https://claude.ai"},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("next handler should not be called for rejected origin")
		}),
	)

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Origin", "https://evil.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403, got %d", rec.Code)
	}
}

func TestOriginMiddleware_noOrigin(t *testing.T) {
	handler := OriginMiddleware(
		[]string{"https://claude.ai"},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	// No Origin — non-browser client (Claude Desktop, Claude Code).
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for originless request, got %d", rec.Code)
	}
}

func TestOriginMiddleware_caseInsensitive(t *testing.T) {
	handler := OriginMiddleware(
		[]string{"https://claude.ai"},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Origin", "HTTPS://CLAUDE.AI")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for case-insensitive match, got %d", rec.Code)
	}
}
