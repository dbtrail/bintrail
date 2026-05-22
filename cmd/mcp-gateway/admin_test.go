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

	body := `{"tenant_id":"acme","tier":"paid","backend_url":"http://backend:8080/mcp","status":"active","auth_secret":"hunter2"}`
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

	body := `{"tenant_id":"acme","tier":"free","auth_secret":"hunter2"}`
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

	body := `{"tenant_id":"newco","tier":"free","auth_secret":"hunter2"}`
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

// TestAdmin_createTenantRejectsMissingSecret pins the strict-mode
// invariant from the #295 review: POST without an auth_secret is
// rejected with 400 so no tenant is ever stored in a state where
// VerifySecret would unconditionally fail.
func TestAdmin_createTenantRejectsMissingSecret(t *testing.T) {
	store := NewMemoryStore()
	handler := NewAdminHandler(store, "secret")

	body := `{"tenant_id":"acme","tier":"paid","status":"active"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/tenants", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.Tenants["acme"] != nil {
		t.Errorf("tenant must not be stored when auth_secret is missing; found %+v", store.Tenants["acme"])
	}
}

// TestAdmin_createTenantRejectsBadTenantID pins the tenantIDRE
// charset validation from the #295 review. Embedded newlines could
// otherwise forge log lines in any future code path that string-
// interpolates tenant_id into a message.
func TestAdmin_createTenantRejectsBadTenantID(t *testing.T) {
	store := NewMemoryStore()
	handler := NewAdminHandler(store, "secret")

	for _, bad := range []string{
		"acme\nFAKE LOG: granted",
		"has space",
		"has/slash",
		strings.Repeat("a", 65),
	} {
		body, _ := json.Marshal(map[string]string{"tenant_id": bad, "auth_secret": "hunter2"})
		req := httptest.NewRequest(http.MethodPost, "/admin/tenants", strings.NewReader(string(body)))
		req.Header.Set("Authorization", "Bearer secret")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("tenant_id=%q: expected 400, got %d", bad, rec.Code)
		}
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

// ─── #132 per-tenant auth_secret on admin CRUD ───────────────────────────────

// TestAdmin_createTenantWithAuthSecret pins that POST /admin/tenants
// accepts auth_secret, hashes it with bcrypt, and never echoes the
// cleartext back. The store ends up with a bcrypt hash that
// round-trips through Tenant.VerifySecret.
func TestAdmin_createTenantWithAuthSecret(t *testing.T) {
	store := NewMemoryStore()
	handler := NewAdminHandler(store, "secret")

	body := `{"tenant_id":"acme","tier":"paid","status":"active","auth_secret":"hunter2"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/tenants", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	stored := store.Tenants["acme"]
	if stored == nil {
		t.Fatal("tenant not stored")
	}
	if stored.AuthSecretHash == "" {
		t.Fatal("expected AuthSecretHash to be set after POST with auth_secret")
	}
	if stored.AuthSecretHash == "hunter2" {
		t.Fatal("AuthSecretHash must be bcrypt-hashed, not cleartext")
	}
	if !stored.VerifySecret("hunter2") {
		t.Errorf("stored hash does not verify against the submitted cleartext")
	}
	if stored.VerifySecret("wrong") {
		t.Errorf("stored hash verifies against the wrong cleartext")
	}

	// Response body must NOT include the raw auth_secret. The
	// AuthSecretHash field IS exposed (json tag omitempty), but the
	// `auth_secret` cleartext input is request-only and must not
	// round-trip.
	if strings.Contains(rec.Body.String(), `"auth_secret"`) {
		t.Errorf("response body must not echo the cleartext auth_secret field; got: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "hunter2") {
		t.Errorf("response body must never contain the cleartext password; got: %s", rec.Body.String())
	}
}

// TestAdmin_updateTenantPreservesAuthSecret pins the PUT contract:
// an update with an empty auth_secret must preserve the existing
// AuthSecretHash on the tenant. Otherwise a routine update (e.g.
// changing tier) would silently downgrade the tenant to legacy
// no-secret mode.
func TestAdmin_updateTenantPreservesAuthSecret(t *testing.T) {
	hash, err := HashSecret("hunter2")
	if err != nil {
		t.Fatalf("HashSecret: %v", err)
	}
	store := NewMemoryStore()
	store.Tenants["acme"] = &Tenant{
		TenantID: "acme", Tier: "free", Status: "active", AuthSecretHash: hash,
	}
	handler := NewAdminHandler(store, "secret")

	// PUT with NO auth_secret — should preserve hash.
	body := `{"tier":"paid","status":"active"}`
	req := httptest.NewRequest(http.MethodPut, "/admin/tenants/acme", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	stored := store.Tenants["acme"]
	if stored.AuthSecretHash != hash {
		t.Errorf("PUT with empty auth_secret cleared the hash:\n  before: %q\n  after:  %q", hash, stored.AuthSecretHash)
	}
	if stored.Tier != "paid" {
		t.Errorf("Tier update did not apply: got %q", stored.Tier)
	}
}

// TestAdmin_updateTenantRotatesAuthSecret pins the rotation path:
// PUT with a non-empty auth_secret replaces the existing hash.
func TestAdmin_updateTenantRotatesAuthSecret(t *testing.T) {
	oldHash, err := HashSecret("old-password")
	if err != nil {
		t.Fatalf("HashSecret: %v", err)
	}
	store := NewMemoryStore()
	store.Tenants["acme"] = &Tenant{
		TenantID: "acme", Status: "active", AuthSecretHash: oldHash,
	}
	handler := NewAdminHandler(store, "secret")

	body := `{"status":"active","auth_secret":"new-password"}`
	req := httptest.NewRequest(http.MethodPut, "/admin/tenants/acme", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	stored := store.Tenants["acme"]
	if stored.AuthSecretHash == oldHash {
		t.Errorf("PUT with auth_secret did not rotate the hash")
	}
	if !stored.VerifySecret("new-password") {
		t.Errorf("new hash does not verify against the rotated cleartext")
	}
	if stored.VerifySecret("old-password") {
		t.Errorf("old cleartext still verifies — rotation incomplete")
	}
}
