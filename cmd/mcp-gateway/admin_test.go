package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdmin_createTenant(t *testing.T) {
	store := NewMemoryStore()
	handler := NewAdminHandler(store, "secret")

	body := `{"tenant_id":"acme","tier":"paid","backend_url":"http://backend:8080/mcp","status":"active"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/tenants", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var tenant Tenant
	json.NewDecoder(rec.Body).Decode(&tenant)
	if tenant.TenantID != "acme" {
		t.Errorf("expected tenant_id acme, got %s", tenant.TenantID)
	}
	if tenant.Tier != "paid" {
		t.Errorf("expected tier paid, got %s", tenant.Tier)
	}

	// Verify it's in the store.
	if store.Tenants["acme"] == nil {
		t.Error("tenant not found in store")
	}
}

func TestAdmin_createTenantDuplicate(t *testing.T) {
	store := NewMemoryStore()
	store.Tenants["acme"] = &Tenant{TenantID: "acme", Status: "active"}
	handler := NewAdminHandler(store, "secret")

	body := `{"tenant_id":"acme","tier":"free"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/tenants", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Errorf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAdmin_createTenantDefaultStatus(t *testing.T) {
	store := NewMemoryStore()
	handler := NewAdminHandler(store, "secret")

	body := `{"tenant_id":"newco","tier":"free"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/tenants", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
	if store.Tenants["newco"].Status != "active" {
		t.Errorf("expected default status active, got %s", store.Tenants["newco"].Status)
	}
}

func TestAdmin_createTenantMissingID(t *testing.T) {
	handler := NewAdminHandler(NewMemoryStore(), "secret")

	req := httptest.NewRequest(http.MethodPost, "/admin/tenants", strings.NewReader(`{"tier":"free"}`))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestAdmin_listTenants(t *testing.T) {
	store := NewMemoryStore()
	store.Tenants["a"] = &Tenant{TenantID: "a", Status: "active"}
	store.Tenants["b"] = &Tenant{TenantID: "b", Status: "active"}
	handler := NewAdminHandler(store, "secret")

	req := httptest.NewRequest(http.MethodGet, "/admin/tenants", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var tenants []*Tenant
	json.NewDecoder(rec.Body).Decode(&tenants)
	if len(tenants) != 2 {
		t.Errorf("expected 2 tenants, got %d", len(tenants))
	}
}

func TestAdmin_getTenant(t *testing.T) {
	store := NewMemoryStore()
	store.Tenants["acme"] = &Tenant{TenantID: "acme", Tier: "paid", Status: "active"}
	handler := NewAdminHandler(store, "secret")

	req := httptest.NewRequest(http.MethodGet, "/admin/tenants/acme", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var tenant Tenant
	json.NewDecoder(rec.Body).Decode(&tenant)
	if tenant.TenantID != "acme" {
		t.Errorf("expected acme, got %s", tenant.TenantID)
	}
}

func TestAdmin_getTenantNotFound(t *testing.T) {
	handler := NewAdminHandler(NewMemoryStore(), "secret")

	req := httptest.NewRequest(http.MethodGet, "/admin/tenants/missing", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestAdmin_updateTenant(t *testing.T) {
	store := NewMemoryStore()
	store.Tenants["acme"] = &Tenant{TenantID: "acme", Tier: "free", Status: "active"}
	handler := NewAdminHandler(store, "secret")

	body := `{"tier":"paid","backend_url":"http://new-backend:8080/mcp","status":"active"}`
	req := httptest.NewRequest(http.MethodPut, "/admin/tenants/acme", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.Tenants["acme"].Tier != "paid" {
		t.Errorf("expected tier paid, got %s", store.Tenants["acme"].Tier)
	}
	if store.Tenants["acme"].BackendURL != "http://new-backend:8080/mcp" {
		t.Errorf("expected new backend URL, got %s", store.Tenants["acme"].BackendURL)
	}
}

func TestAdmin_deleteTenant(t *testing.T) {
	store := NewMemoryStore()
	store.Tenants["acme"] = &Tenant{TenantID: "acme", Status: "active"}
	handler := NewAdminHandler(store, "secret")

	req := httptest.NewRequest(http.MethodDelete, "/admin/tenants/acme", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rec.Code)
	}
	if store.Tenants["acme"] != nil {
		t.Error("tenant should be deleted")
	}
}

func TestAdmin_unauthorized(t *testing.T) {
	handler := NewAdminHandler(NewMemoryStore(), "secret")

	tests := []struct {
		name string
		auth string
	}{
		{"no auth", ""},
		{"wrong token", "Bearer wrong"},
		{"not bearer", "Basic secret"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/admin/tenants", nil)
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("expected 401, got %d", rec.Code)
			}
		})
	}
}
