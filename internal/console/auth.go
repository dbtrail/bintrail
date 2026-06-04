package console

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"net/http"
	"strings"
)

// generateToken returns a cryptographically random 128-bit token rendered as
// 32 lowercase hex characters. It is used to gate the API when the operator
// does not supply an explicit token.
func generateToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// isLoopbackAddr reports whether a listen address binds only to the loopback
// interface. An empty host or a wildcard (0.0.0.0 / ::) binds to every
// interface and is NOT loopback — those require an explicit token.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// No port present; treat the whole string as the host.
		host = addr
	}
	switch host {
	case "localhost":
		return true
	case "", "0.0.0.0", "::":
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// bearerToken extracts the token from an "Authorization: Bearer <token>"
// header. Returns "" when the header is missing or malformed.
func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return ""
	}
	return h[len(prefix):]
}

// tokenMiddleware requires a valid bearer token on every wrapped request.
// The comparison is constant-time for equal-length inputs (subtle returns 0
// immediately on a length mismatch, so token *length* is not hidden — that's
// fine here, the token is a fixed 32 hex chars).
//
// The token is required in the Authorization header specifically (not a
// cookie): a browser fetch() must opt in to sending it, which keeps a
// cross-site form-POST from carrying ambient credentials to /api/recover.
func (s *Server) tokenMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := bearerToken(r)
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) != 1 {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized: missing or invalid token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// hostGuard rejects requests whose Host header is not in the allowlist. This
// defeats DNS-rebinding: a malicious page on attacker.example whose DNS
// rebinds to 127.0.0.1 still sends "Host: attacker.example", which is refused.
// IP-literal and localhost Hosts are accepted because a rebinding attack needs
// an attacker-controlled *domain name*, never a bare IP.
func (s *Server) hostGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.hostAllowed(r.Host) {
			writeJSONError(w, http.StatusForbidden, "forbidden: host not allowed")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// hostAllowed reports whether the request Host header is acceptable. The host
// is allowed when it is an IP literal, "localhost", or appears in the
// configured allowlist (operator-supplied hostnames). Domain-name Hosts are
// rejected.
func (s *Server) hostAllowed(host string) bool {
	if host == "" {
		return false
	}
	h, _, err := net.SplitHostPort(host)
	if err != nil {
		h = host
	}
	if h == "" {
		return false
	}
	for _, a := range s.allowedHosts {
		if strings.EqualFold(h, a) {
			return true
		}
	}
	if strings.EqualFold(h, "localhost") {
		return true
	}
	return net.ParseIP(h) != nil
}
