package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
)

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
	if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") != h.token {
		w.Header().Set("WWW-Authenticate", `Bearer`)
		jsonError(w, "invalid_request", "invalid or missing admin token", http.StatusUnauthorized)
		return false
	}
	return true
}

// tenantRequest is the JSON body for create/update operations.
type tenantRequest struct {
	TenantID   string `json:"tenant_id"`
	Tier       string `json:"tier"`
	BackendURL string `json:"backend_url"`
	IndexDSN   string `json:"index_dsn"`
	Status     string `json:"status"`
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

	if err := h.store.UpdateTenant(r.Context(), tenant); err != nil {
		slog.Error("update tenant", "tenant_id", id, "error", err)
		jsonError(w, "server_error", "failed to update tenant: "+err.Error(), http.StatusInternalServerError)
		return
	}

	slog.Info("tenant updated", "tenant_id", id)
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
