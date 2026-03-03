// mcp-gateway is an authentication and routing gateway for bintrail-mcp.
//
// It exposes a public HTTPS endpoint that Claude Connectors connect to,
// handles OAuth 2.1 authentication (with Dynamic Client Registration and PKCE),
// and reverse-proxies authenticated MCP requests to the appropriate backend
// bintrail-mcp instance based on the authenticated tenant.
//
//	mcp-gateway --addr :8443 --backend-url http://localhost:8080
//
// In production, TLS is terminated at the ALB; the gateway listens on plain HTTP.
//
// Configuration:
//
//	--addr                  Listen address (default :8443)
//	--allowed-origins       Comma-separated allowed CORS origins
//	--backend-url           Default backend URL (single-backend mode)
//	--table-prefix          DynamoDB table name prefix (default "bintrail-oauth")
//	--issuer                OAuth issuer URL (default "https://mcp.dbtrail.com")
//	--rate-limit-register   Per-IP requests/min for /oauth/register (default 10, 0=off)
//	--rate-limit-token      Per-IP requests/min for /oauth/token (default 30, 0=off)
//	--rate-limit-authorize  Per-IP requests/min for POST /oauth/authorize (default 20, 0=off)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// gatewayVersion is injected at build time via -ldflags.
var gatewayVersion = "dev"

func main() {
	addr := flag.String("addr", ":8443", "HTTP listen address")
	allowedOrigins := flag.String("allowed-origins", "https://claude.ai,https://claude.com", "Comma-separated allowed CORS origins")
	backendURL := flag.String("backend-url", "", "Default backend URL for single-backend mode (e.g. http://localhost:8080)")
	tablePrefix := flag.String("table-prefix", "bintrail-oauth", "DynamoDB table name prefix")
	issuer := flag.String("issuer", "https://mcp.dbtrail.com", "OAuth issuer URL")
	adminToken := flag.String("admin-token", "", "Bearer token for admin API (required for /admin/* endpoints)")
	healthInterval := flag.Duration("health-interval", 30*time.Second, "Backend health check interval (0 to disable)")
	healthThreshold := flag.Int("health-threshold", 3, "Consecutive failures before marking a backend unhealthy")
	rateLimitRegister := flag.Int("rate-limit-register", 10, "Per-IP requests/min for /oauth/register (0 to disable)")
	rateLimitToken := flag.Int("rate-limit-token", 30, "Per-IP requests/min for /oauth/token (0 to disable)")
	rateLimitAuthorize := flag.Int("rate-limit-authorize", 20, "Per-IP requests/min for POST /oauth/authorize (0 to disable)")
	flag.Parse()

	origins := parseOrigins(*allowedOrigins)

	store, err := NewDynamoStore(*tablePrefix)
	if err != nil {
		slog.Error("failed to initialize store", "error", err)
		os.Exit(1)
	}

	oauthCfg := &OAuthConfig{
		Issuer: *issuer,
		Store:  store,
	}

	// Rate limiter for OAuth endpoints.
	rateLimiter := NewRateLimiter(1 * time.Minute)
	defer rateLimiter.Stop()

	mux := http.NewServeMux()

	// OAuth endpoints (no auth required, rate-limited).
	mux.HandleFunc("GET /.well-known/oauth-authorization-server", oauthCfg.MetadataHandler)
	mux.Handle("POST /oauth/register", RateLimitMiddleware(rateLimiter, *rateLimitRegister,
		http.HandlerFunc(oauthCfg.RegisterHandler)))
	mux.HandleFunc("GET /oauth/authorize", oauthCfg.AuthorizeHandler)
	mux.Handle("POST /oauth/authorize", RateLimitMiddleware(rateLimiter, *rateLimitAuthorize,
		http.HandlerFunc(oauthCfg.AuthorizeSubmitHandler)))
	mux.Handle("POST /oauth/token", RateLimitMiddleware(rateLimiter, *rateLimitToken,
		http.HandlerFunc(oauthCfg.TokenHandler)))

	// Health checker for backend monitoring.
	checker := NewHealthChecker(*healthInterval, *healthThreshold)
	if *backendURL != "" {
		checker.RegisterBackend(*backendURL)
	}

	// Health check endpoint — includes backend health status.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":   "ok",
			"version":  gatewayVersion,
			"backends": checker.Status(),
		})
	})

	// Admin API for tenant provisioning (requires --admin-token).
	if *adminToken != "" {
		admin := NewAdminHandler(store, *adminToken)
		mux.Handle("/admin/tenants", admin)
		mux.Handle("/admin/tenants/", admin)
	}

	// MCP endpoint — auth required, proxied to backend.
	proxy := NewReverseProxy(store, *backendURL, checker)
	mux.Handle("/mcp", AuthMiddleware(store, proxy))

	// Wrap everything with logging, request IDs, CORS, and origin validation.
	handler := RequestIDMiddleware(
		LoggingMiddleware(
			CORSMiddleware(origins, OriginMiddleware(origins, mux))))

	srv := &http.Server{Addr: *addr, Handler: handler}

	// Start backend health checking in the background.
	healthCtx, healthCancel := context.WithCancel(context.Background())
	defer healthCancel()
	go checker.Run(healthCtx)

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-sigCtx.Done()
		slog.Info("MCP gateway shutting down")
		if err := srv.Shutdown(context.Background()); err != nil {
			slog.Error("MCP gateway shutdown error", "error", err)
		}
	}()

	slog.Info("MCP gateway starting", "addr", *addr, "version", gatewayVersion)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("MCP gateway error", "error", err)
		os.Exit(1)
	}
}

func parseOrigins(s string) []string {
	var origins []string
	for _, o := range strings.Split(s, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			origins = append(origins, o)
		}
	}
	return origins
}
