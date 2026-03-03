package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAuthMiddleware_validToken(t *testing.T) {
	store := NewMemoryStore()
	token := "valid-access-token"
	store.Tokens[HashToken(token)] = &TokenRecord{
		AccessTokenHash: HashToken(token),
		ClientID:        "client",
		TenantID:        "acme-corp",
		ExpiresAt:       time.Now().Add(1 * time.Hour).Unix(),
	}
	store.Tenants["acme-corp"] = &Tenant{
		TenantID: "acme-corp",
		Tier:     "paid",
		Status:   "active",
	}

	var gotTenant *Tenant
	handler := AuthMiddleware(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTenant = TenantFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if gotTenant == nil {
		t.Fatal("expected tenant in context")
	}
	if gotTenant.TenantID != "acme-corp" {
		t.Errorf("expected tenant acme-corp, got %s", gotTenant.TenantID)
	}
}

func TestAuthMiddleware_missingToken(t *testing.T) {
	handler := AuthMiddleware(NewMemoryStore(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next handler should not be called")
	}))

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Error("expected WWW-Authenticate header")
	}
}

func TestAuthMiddleware_invalidToken(t *testing.T) {
	handler := AuthMiddleware(NewMemoryStore(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next handler should not be called")
	}))

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer nonexistent-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestAuthMiddleware_expiredToken(t *testing.T) {
	store := NewMemoryStore()
	token := "expired-token"
	store.Tokens[HashToken(token)] = &TokenRecord{
		AccessTokenHash: HashToken(token),
		ClientID:        "client",
		TenantID:        "acme",
		ExpiresAt:       time.Now().Add(-1 * time.Hour).Unix(), // expired
	}

	handler := AuthMiddleware(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next handler should not be called")
	}))

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestAuthMiddleware_inactiveTenant(t *testing.T) {
	store := NewMemoryStore()
	token := "good-token"
	store.Tokens[HashToken(token)] = &TokenRecord{
		AccessTokenHash: HashToken(token),
		ClientID:        "client",
		TenantID:        "suspended",
		ExpiresAt:       time.Now().Add(1 * time.Hour).Unix(),
	}
	store.Tenants["suspended"] = &Tenant{
		TenantID: "suspended",
		Status:   "suspended",
	}

	handler := AuthMiddleware(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next handler should not be called")
	}))

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403, got %d", rec.Code)
	}
}
