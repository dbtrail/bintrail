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
//	--addr              Listen address (default :8443)
//	--allowed-origins   Comma-separated allowed CORS origins
//	--backend-url       Default backend URL (single-backend mode)
//	--table-prefix      DynamoDB table name prefix (default "bintrail-oauth")
//	--issuer            OAuth issuer URL (default "https://mcp.dbtrail.com")
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
)

// gatewayVersion is injected at build time via -ldflags.
var gatewayVersion = "dev"

func main() {
	addr := flag.String("addr", ":8443", "HTTP listen address")
	allowedOrigins := flag.String("allowed-origins", "https://claude.ai,https://claude.com", "Comma-separated allowed CORS origins")
	backendURL := flag.String("backend-url", "", "Default backend URL for single-backend mode (e.g. http://localhost:8080)")
	tablePrefix := flag.String("table-prefix", "bintrail-oauth", "DynamoDB table name prefix")
	issuer := flag.String("issuer", "https://mcp.dbtrail.com", "OAuth issuer URL")
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

	mux := http.NewServeMux()

	// OAuth endpoints (no auth required).
	mux.HandleFunc("GET /.well-known/oauth-authorization-server", oauthCfg.MetadataHandler)
	mux.HandleFunc("POST /oauth/register", oauthCfg.RegisterHandler)
	mux.HandleFunc("GET /oauth/authorize", oauthCfg.AuthorizeHandler)
	mux.HandleFunc("POST /oauth/authorize", oauthCfg.AuthorizeSubmitHandler)
	mux.HandleFunc("POST /oauth/token", oauthCfg.TokenHandler)

	// Health check.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "version": gatewayVersion})
	})

	// MCP endpoint — auth required, proxied to backend.
	proxy := NewReverseProxy(store, *backendURL)
	mux.Handle("/mcp", AuthMiddleware(store, proxy))

	// Wrap everything with CORS and origin validation.
	handler := CORSMiddleware(origins, OriginMiddleware(origins, mux))

	srv := &http.Server{Addr: *addr, Handler: handler}

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
