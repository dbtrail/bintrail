package main

import (
	"net/http"
	"strings"
)

// OriginMiddleware validates the Origin header per the MCP spec to prevent
// DNS rebinding attacks. Requests with an unrecognized Origin are rejected
// with 403 Forbidden. Requests with no Origin header (non-browser clients
// like Claude Desktop or Claude Code) are allowed through — they still
// require a valid Bearer token on /mcp.
func OriginMiddleware(allowedOrigins []string, next http.Handler) http.Handler {
	originSet := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		originSet[strings.ToLower(o)] = true
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && !originSet[strings.ToLower(origin)] {
			http.Error(w, "Forbidden: unrecognized origin", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
