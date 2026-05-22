package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// tenantIDRE constrains tenant IDs to a safe charset so they can be
// embedded in log lines, URLs, and admin paths without escape worries.
// Specifically prevents log-injection (newlines / control chars in
// tenant_id end up forging log lines, since slog text/JSON handlers
// don't escape non-attribute message strings).
var tenantIDRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// AdminHandler provides tenant CRUD endpoints.
//
// All endpoints require a Bearer token matching the --admin-token flag.
// Routes:
//
//	POST   /admin/tenants      — create a new tenant
//	GET    /admin/tenants      — list all tenants
//	GET    /admin/tenants/{id} — get a single tenant
//	PUT    /admin/tenants/{id} — update a tenant
//	DELETE /admin/tenants/{id} — delete a tenant
type AdminHandler struct {
	store Store
	token string // shared secret for admin auth
}

// NewAdminHandler creates an admin API handler secured with the given token.
func NewAdminHandler(store Store, adminToken string) *AdminHandler {
	return &AdminHandler{store: store, token: adminToken}
}

// ServeHTTP dispatches admin API requests.
func (h *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(w, r) {
		return
	}

	// Route: /admin/tenants or /admin/tenants/{id}
	path := strings.TrimPrefix(r.URL.Path, "/admin/tenants")
	id := strings.TrimPrefix(path, "/")

	switch {
	case id == "" && r.Method == http.MethodPost:
		h.createTenant(w, r)
	case id == "" && r.Method == http.MethodGet:
		h.listTenants(w, r)
	case id != "" && r.Method == http.MethodGet:
		h.getTenant(w, r, id)
	case id != "" && r.Method == http.MethodPut:
		h.updateTenant(w, r, id)
	case id != "" && r.Method == http.MethodDelete:
		h.deleteTenant(w, r, id)
	default:
		jsonError(w, "invalid_request", "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *AdminHandler) authenticate(w http.ResponseWriter, r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		w.Header().Set("WWW-Authenticate", `Bearer`)
		jsonError(w, "invalid_request", "invalid or missing admin token", http.StatusUnauthorized)
		return false
	}
	got := strings.TrimPrefix(auth, "Bearer ")
	if subtle.ConstantTimeCompare([]byte(got), []byte(h.token)) != 1 {
		w.Header().Set("WWW-Authenticate", `Bearer`)
		jsonError(w, "invalid_request", "invalid or missing admin token", http.StatusUnauthorized)
		return false
	}
	return true
}

// tenantRequest is the JSON body for create/update operations.
//
// AuthSecret is the cleartext per-tenant secret operators set when creating
// or rotating a tenant; it is bcrypt-hashed before persistence and never
// echoed back on a GET. On PUT, an empty AuthSecret preserves the existing
// stored hash — clearing a secret requires an explicit admin workflow (not
// in this PR).
type tenantRequest struct {
	TenantID   string `json:"tenant_id"`
	Tier       string `json:"tier"`
	BackendURL string `json:"backend_url"`
	IndexDSN   string `json:"index_dsn"`
	Status     string `json:"status"`
	AuthSecret string `json:"auth_secret,omitempty"`
}

func (h *AdminHandler) createTenant(w http.ResponseWriter, r *http.Request) {
	var req tenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid_request", "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.TenantID == "" {
		jsonError(w, "invalid_request", "tenant_id is required", http.StatusBadRequest)
		return
	}
	if !tenantIDRE.MatchString(req.TenantID) {
		jsonError(w, "invalid_request", "tenant_id must match ^[A-Za-z0-9_-]{1,64}$", http.StatusBadRequest)
		return
	}
	// Strict-mode invariant (#132 review): a tenant without
	// auth_secret cannot authorize anyway; reject creation so a
	// fat-fingered admin POST never lands an unmigratable tenant in
	// the store. Rotation/clearing flows belong in a future PR.
	if req.AuthSecret == "" {
		jsonError(w, "invalid_request", "auth_secret is required (#132 strict-mode); tenants without a secret cannot authorize", http.StatusBadRequest)
		return
	}
	if req.Status == "" {
		req.Status = "active"
	}

	tenant := &Tenant{
		TenantID:   req.TenantID,
		Tier:       req.Tier,
		BackendURL: req.BackendURL,
		IndexDSN:   req.IndexDSN,
		Status:     req.Status,
	}

	hash, err := HashSecret(req.AuthSecret)
	if err != nil {
		if errors.Is(err, bcrypt.ErrPasswordTooLong) {
			jsonError(w, "invalid_request", "auth_secret exceeds bcrypt's 72-byte limit; choose a shorter secret", http.StatusBadRequest)
			return
		}
		slog.Error("hash tenant auth_secret", "tenant_id", req.TenantID, "error", err)
		jsonError(w, "server_error", "failed to hash auth_secret", http.StatusInternalServerError)
		return
	}
	tenant.AuthSecretHash = hash

	if err := h.store.CreateTenant(r.Context(), tenant); err != nil {
		slog.Error("create tenant", "tenant_id", req.TenantID, "error", err)
		jsonError(w, "server_error", "failed to create tenant: "+err.Error(), http.StatusConflict)
		return
	}

	slog.Info("tenant created", "tenant_id", req.TenantID, "tier", req.Tier)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(tenant)
}

func (h *AdminHandler) listTenants(w http.ResponseWriter, r *http.Request) {
	tenants, err := h.store.ListTenants(r.Context())
	if err != nil {
		slog.Error("list tenants", "error", err)
		jsonError(w, "server_error", "failed to list tenants", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tenants)
}

func (h *AdminHandler) getTenant(w http.ResponseWriter, r *http.Request, id string) {
	tenant, err := h.store.GetTenant(r.Context(), id)
	if err != nil {
		slog.Error("get tenant", "tenant_id", id, "error", err)
		jsonError(w, "server_error", "failed to get tenant", http.StatusInternalServerError)
		return
	}
	if tenant == nil {
		jsonError(w, "not_found", "tenant not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tenant)
}

func (h *AdminHandler) updateTenant(w http.ResponseWriter, r *http.Request, id string) {
	var req tenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid_request", "invalid JSON body", http.StatusBadRequest)
		return
	}

	// The URL path ID takes precedence over the body.
	tenant := &Tenant{
		TenantID:   id,
		Tier:       req.Tier,
		BackendURL: req.BackendURL,
		IndexDSN:   req.IndexDSN,
		Status:     req.Status,
	}

	// Auth-secret handling: a non-empty AuthSecret in the body rotates
	// the secret (bcrypt-hash before storing). An empty AuthSecret
	// preserves the existing hash — the admin must use an explicit
	// future "clear secret" workflow to remove one, otherwise a
	// PUT that omits auth_secret would silently downgrade the tenant
	// to legacy-no-secret mode.
	if req.AuthSecret != "" {
		hash, err := HashSecret(req.AuthSecret)
		if err != nil {
			if errors.Is(err, bcrypt.ErrPasswordTooLong) {
				jsonError(w, "invalid_request", "auth_secret exceeds bcrypt's 72-byte limit; choose a shorter secret", http.StatusBadRequest)
				return
			}
			slog.Error("hash tenant auth_secret", "tenant_id", id, "error", err)
			jsonError(w, "server_error", "failed to hash auth_secret", http.StatusInternalServerError)
			return
		}
		tenant.AuthSecretHash = hash
	} else {
		existing, err := h.store.GetTenant(r.Context(), id)
		if err != nil {
			slog.Error("load tenant for auth_secret preservation", "tenant_id", id, "error", err)
			jsonError(w, "server_error", "failed to load tenant for update", http.StatusInternalServerError)
			return
		}
		// Reject PUT on a non-existent tenant with 404 explicitly
		// instead of letting the store's UpdateTenant condition error
		// at 500. Without this, a PUT to /admin/tenants/<unknown>
		// with no auth_secret would propagate as "tenant not found"
		// at the DynamoStore layer → 500, hiding what was really a
		// user-input error.
		if existing == nil {
			jsonError(w, "not_found", "tenant not found", http.StatusNotFound)
			return
		}
		tenant.AuthSecretHash = existing.AuthSecretHash
	}

	if err := h.store.UpdateTenant(r.Context(), tenant); err != nil {
		slog.Error("update tenant", "tenant_id", id, "error", err)
		jsonError(w, "server_error", "failed to update tenant: "+err.Error(), http.StatusInternalServerError)
		return
	}

	slog.Info("tenant updated", "tenant_id", id, "auth_secret_rotated", req.AuthSecret != "")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tenant)
}

func (h *AdminHandler) deleteTenant(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.store.DeleteTenant(r.Context(), id); err != nil {
		slog.Error("delete tenant", "tenant_id", id, "error", err)
		jsonError(w, "server_error", "failed to delete tenant", http.StatusInternalServerError)
		return
	}

	slog.Info("tenant deleted", "tenant_id", id)
	w.WriteHeader(http.StatusNoContent)
}
