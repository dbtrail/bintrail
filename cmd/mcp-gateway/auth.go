package main

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
)

type contextKey string

const tenantContextKey contextKey = "tenant"

// TenantFromContext extracts the authenticated Tenant from the request context.
func TenantFromContext(ctx context.Context) *Tenant {
	t, _ := ctx.Value(tenantContextKey).(*Tenant)
	return t
}

// AuthMiddleware validates the Bearer token on incoming /mcp requests,
// resolves the tenant, and injects it into the request context.
func AuthMiddleware(store Store, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := extractBearerToken(r)
		if token == "" {
			w.Header().Set("WWW-Authenticate", `Bearer`)
			jsonError(w, "invalid_request", "missing or malformed Authorization header", http.StatusUnauthorized)
			return
		}

		rec, err := store.ValidateToken(r.Context(), token)
		if err != nil {
			slog.Error("validate token", "error", err)
			jsonError(w, "server_error", "token validation failed", http.StatusInternalServerError)
			return
		}
		if rec == nil {
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			jsonError(w, "invalid_token", "token is invalid or expired", http.StatusUnauthorized)
			return
		}

		// Resolve tenant.
		tenant, err := store.GetTenant(r.Context(), rec.TenantID)
		if err != nil {
			slog.Error("get tenant", "error", err)
			jsonError(w, "server_error", "tenant lookup failed", http.StatusInternalServerError)
			return
		}
		if tenant == nil || tenant.Status != "active" {
			jsonError(w, "invalid_token", "tenant not found or inactive", http.StatusForbidden)
			return
		}

		ctx := context.WithValue(r.Context(), tenantContextKey, tenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func extractBearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(auth, "Bearer ")
}
