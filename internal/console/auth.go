package console

import (
	"context"
	"crypto/subtle"
	"net"
	"net/http"
	"strings"
)

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

// authKind records, in the request context, which credential authenticated
// the request. The change-password first-set branch keys on it: only the
// static token (the bootstrap trust root) may claim the first password.
type authKind int

const (
	authKindNone authKind = iota
	authKindToken
	authKindSession
)

type authKindCtxKey struct{}

func authKindFrom(ctx context.Context) authKind {
	k, _ := ctx.Value(authKindCtxKey{}).(authKind)
	return k
}

// tokenMiddleware requires a valid bearer credential on every wrapped
// request: either the static access token or a login session. Both checks
// run on the same path with no prefix branching, so response shape never
// reveals which credential kind a guess was tested against. The static
// compare is constant-time for equal-length inputs (subtle returns 0
// immediately on a length mismatch, so credential *length* is not hidden —
// fine: the token is 32 hex chars and sessions are "bcs_"+64 hex by
// construction, not secrets in their shape).
//
// Two guards are independent and BOTH load-bearing in password-only mode,
// where s.token == "" and ConstantTimeCompare("", "") returns 1: the empty-got
// early return rejects credential-less requests up front, and the s.token != ""
// short-circuit additionally stops the static compare from ever running against
// an empty configured token. Removing either leaves the other as the last line
// of defense — keep both.
//
// The credential is required in the Authorization header specifically (not a
// cookie): a browser fetch() must opt in to sending it, which keeps a
// cross-site form-POST from carrying ambient credentials to /api/recover.
func (s *Server) tokenMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := bearerToken(r)
		if got == "" {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized: missing or invalid token")
			return
		}
		if s.token != "" && subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) == 1 {
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), authKindCtxKey{}, authKindToken)))
			return
		}
		if s.sessions.Validate(got) {
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), authKindCtxKey{}, authKindSession)))
			return
		}
		writeJSONError(w, http.StatusUnauthorized, "unauthorized: missing or invalid token")
	})
}

// securityHeaders sets three static response headers on everything:
// Referrer-Policy keeps the ?token= bootstrap URL out of Referer headers,
// nosniff hardens the embedded assets, and DENY blocks framing the login
// overlay (clickjacking).
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
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
