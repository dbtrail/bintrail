package main

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// NewReverseProxy creates an http.Handler that proxies authenticated /mcp
// requests to the appropriate backend bintrail-mcp instance.
//
// It resolves the backend from the tenant (set by AuthMiddleware). If no
// tenant-specific backend is configured, it falls back to defaultBackend.
//
// For shared (free-tier) backends, it injects an X-Bintrail-Tenant header
// so the backend can resolve the correct index DSN per-request.
func NewReverseProxy(store Store, defaultBackend string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant := TenantFromContext(r.Context())
		if tenant == nil {
			// Should not happen — AuthMiddleware runs first.
			jsonError(w, "server_error", "no tenant in context", http.StatusInternalServerError)
			return
		}

		backend := tenant.BackendURL
		if backend == "" {
			backend = defaultBackend
		}
		if backend == "" {
			slog.Error("no backend URL", "tenant_id", tenant.TenantID)
			jsonError(w, "server_error", "no backend configured for tenant", http.StatusBadGateway)
			return
		}

		target, err := url.Parse(backend)
		if err != nil {
			slog.Error("parse backend URL", "url", backend, "error", err)
			jsonError(w, "server_error", "invalid backend URL", http.StatusBadGateway)
			return
		}

		proxy := &httputil.ReverseProxy{
			// FlushInterval -1 flushes after every write — required for SSE streaming.
			FlushInterval: -1,
			Director: func(req *http.Request) {
				req.URL.Scheme = target.Scheme
				req.URL.Host = target.Host
				req.URL.Path = target.Path
				if req.URL.Path == "" {
					req.URL.Path = "/mcp"
				}
				req.Host = target.Host

				// Inject tenant header for shared backends.
				req.Header.Set("X-Bintrail-Tenant", tenant.TenantID)

				// Remove the Authorization header — backends don't need it.
				req.Header.Del("Authorization")
			},
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				slog.Error("proxy error", "tenant_id", tenant.TenantID, "backend", backend, "error", err)
				jsonError(w, "server_error", "backend unavailable", http.StatusBadGateway)
			},
		}
		proxy.ServeHTTP(w, r)
	})
}
