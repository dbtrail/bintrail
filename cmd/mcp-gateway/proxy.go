package main

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
)

// NewReverseProxy creates an http.Handler that proxies authenticated /mcp
// requests to the appropriate backend bintrail-mcp instance.
//
// It resolves the backend from the tenant (set by AuthMiddleware). If no
// tenant-specific backend is configured, it falls back to defaultBackend.
//
// For shared (free-tier) backends, it injects an X-Bintrail-Tenant header
// so the backend can resolve the correct index DSN per-request.
//
// Reverse proxies are cached per backend URL so that HTTP connections and
// transport pools are reused across requests to the same backend.
func NewReverseProxy(store Store, defaultBackend string, health *HealthChecker) http.Handler {
	var mu sync.RWMutex
	proxies := make(map[string]*httputil.ReverseProxy)

	getProxy := func(backend string) (*httputil.ReverseProxy, error) {
		mu.RLock()
		p, ok := proxies[backend]
		mu.RUnlock()
		if ok {
			return p, nil
		}

		target, err := url.Parse(backend)
		if err != nil {
			return nil, err
		}

		p = &httputil.ReverseProxy{
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

				// Remove the Authorization header — backends don't need it.
				req.Header.Del("Authorization")
			},
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				slog.Error("proxy error", "backend", backend, "error", err)
				jsonError(w, "server_error", "backend unavailable", http.StatusBadGateway)
			},
		}

		mu.Lock()
		proxies[backend] = p
		mu.Unlock()
		return p, nil
	}

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

		// Check backend health before proxying.
		if health != nil && !health.IsHealthy(backend) {
			slog.Warn("backend unhealthy, rejecting request",
				"tenant_id", tenant.TenantID,
				"backend", backend,
			)
			jsonError(w, "server_error", "backend is currently unhealthy", http.StatusBadGateway)
			return
		}

		proxy, err := getProxy(backend)
		if err != nil {
			slog.Error("parse backend URL", "url", backend, "error", err)
			jsonError(w, "server_error", "invalid backend URL", http.StatusBadGateway)
			return
		}

		// Register the backend for health checking on first use.
		if health != nil {
			health.RegisterBackend(backend)
		}

		// Inject tenant header per-request (not in the cached Director,
		// since different tenants may share the same backend).
		r.Header.Set("X-Bintrail-Tenant", tenant.TenantID)
		proxy.ServeHTTP(w, r)
	})
}
