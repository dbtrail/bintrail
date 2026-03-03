package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReverseProxy_forwardsToBackend(t *testing.T) {
	// Start a mock backend.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mcp" {
			t.Errorf("expected path /mcp, got %s", r.URL.Path)
		}
		tenantHeader := r.Header.Get("X-Bintrail-Tenant")
		if tenantHeader != "acme-corp" {
			t.Errorf("expected X-Bintrail-Tenant acme-corp, got %s", tenantHeader)
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("Authorization header should be stripped")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
	}))
	defer backend.Close()

	store := NewMemoryStore()
	store.Tenants["acme-corp"] = &Tenant{
		TenantID:   "acme-corp",
		BackendURL: backend.URL + "/mcp",
		Status:     "active",
	}

	proxy := NewReverseProxy(store, "", nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	req.Header.Set("Authorization", "Bearer should-be-stripped")
	ctx := context.WithValue(req.Context(), tenantContextKey, store.Tenants["acme-corp"])
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), `"result":"ok"`) {
		t.Errorf("unexpected response body: %s", body)
	}
}

func TestReverseProxy_fallsBackToDefaultBackend(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()

	store := NewMemoryStore()

	// Tenant has no backend URL — should use default.
	tenant := &Tenant{TenantID: "no-backend", Status: "active"}

	proxy := NewReverseProxy(store, backend.URL+"/mcp", nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	ctx := context.WithValue(req.Context(), tenantContextKey, tenant)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestReverseProxy_noBackendConfigured(t *testing.T) {
	store := NewMemoryStore()
	tenant := &Tenant{TenantID: "orphan", Status: "active"}

	proxy := NewReverseProxy(store, "", nil) // no default backend

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	ctx := context.WithValue(req.Context(), tenantContextKey, tenant)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", rec.Code)
	}
}

func TestReverseProxy_noTenantInContext(t *testing.T) {
	proxy := NewReverseProxy(NewMemoryStore(), "http://localhost:8080", nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestReverseProxy_unhealthyBackend(t *testing.T) {
	store := NewMemoryStore()

	// Set up a health checker with the backend marked unhealthy.
	checker := NewHealthChecker(time.Hour, 1) // long interval — we control state manually
	checker.RegisterBackend("http://backend:8080/mcp")
	// Force unhealthy by recording failures.
	checker.recordFailure("http://backend:8080/mcp", "test: forced unhealthy")

	tenant := &Tenant{TenantID: "acme", BackendURL: "http://backend:8080/mcp", Status: "active"}
	proxy := NewReverseProxy(store, "", checker)

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	ctx := context.WithValue(req.Context(), tenantContextKey, tenant)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Errorf("expected 502 for unhealthy backend, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unhealthy") {
		t.Errorf("expected unhealthy message, got: %s", rec.Body.String())
	}
}
