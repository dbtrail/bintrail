package console

import (
	"context"
	"crypto/subtle"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/ext"
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

// policyCtxKey carries the authenticated session's access policy (from
// sessionStore.Lookup) into the request. It is nil for the static token, the
// password login, and any OSS session — a nil policy means full access, so the
// authz middleware and capabilities report everything, preserving OSS behavior.
// Only an EE build attaches a non-nil policy via the ext session issuer.
type policyCtxKey struct{}

func policyFrom(ctx context.Context) *ext.AccessPolicy {
	p, _ := ctx.Value(policyCtxKey{}).(*ext.AccessPolicy)
	return p
}

// identityCtxKey carries the session's verified login identity ("" for the
// static token and for sessions minted with no identity). Display/audit only —
// authorization decisions key on the policy, never on this string.
type identityCtxKey struct{}

func identityFrom(ctx context.Context) string {
	s, _ := ctx.Value(identityCtxKey{}).(string)
	return s
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
		if identity, policy, ok := s.sessions.Lookup(got); ok {
			ctx := context.WithValue(r.Context(), authKindCtxKey{}, authKindSession)
			ctx = context.WithValue(ctx, policyCtxKey{}, policy)
			ctx = context.WithValue(ctx, identityCtxKey{}, identity)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		writeJSONError(w, http.StatusUnauthorized, "unauthorized: missing or invalid token")
	})
}

// maxAPIBody caps authenticated JSON request bodies (#848). 1 MiB is far
// above any legitimate console request (the largest are recover filters and
// server CRUD payloads, well under a kilobyte) while keeping a leaked
// automation token from buffering a multi-GB JSON string in the watch
// daemon's heap — which shares the process with the capture stream. The
// pre-auth login/setup bodies have their own tighter cap (maxLoginBody).
const maxAPIBody = 1 << 20

// apiGuard runs after authentication on every /api handler. It does two
// things (#848):
//
//   - caps the request body at maxAPIBody, so json.Decode fails with
//     *http.MaxBytesError on overflow (writeBodyDecodeError maps it to 413);
//   - clears the connection read deadline armed by the server-wide
//     ReadTimeout. That timeout exists to bound unauthenticated slow-drip
//     connections; several authenticated handlers (recover, verify explain,
//     reconstruct over S3 archives) legitimately run past it, and net/http's
//     background read hitting the deadline cancels the request context
//     mid-flight. Best-effort: recorders and non-deadline writers just skip
//     it.
func apiGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxAPIBody)
		_ = http.NewResponseController(w).SetReadDeadline(time.Time{})
		next.ServeHTTP(w, r)
	})
}

// securityHeaders sets four static response headers on everything:
// Referrer-Policy keeps the ?token= bootstrap URL out of Referer headers,
// nosniff hardens the embedded assets, DENY blocks framing the login
// overlay (clickjacking), and the CSP freezes the frontend's no-inline-script
// invariant structurally (#848) — app.js builds the DOM with el()/textContent
// and never innerHTML, so even a stored-XSS payload smuggled through source
// data (row images, schema/table names, source error messages) has no
// executable sink, and script-src 'self' guarantees that stays true.
//
// The CSP value matches what the embedded frontend actually needs:
// script-src 'self' (index.html has exactly one <script src="app.js">, no
// inline scripts; real extension views are same-origin module imports —
// ext.ConsoleViewProvider.Script names a URL under the provider's /ext/<ID>/
// StaticHandler subtree) plus blob:, because dynamically minted module URLs
// are part of the ext-view import surface (the console-e2e ext-view contract
// stubs Script() with a blob: ES module, and an embedding build may do the
// same to hand the SPA a module it assembled client-side). blob: is an
// acceptable relaxation, not a hole: a blob URL can only be minted by
// same-origin script that is ALREADY executing, so it cannot serve as an
// initial injection vector the way 'unsafe-inline' or a remote origin could;
// style-src needs 'unsafe-inline' (index.html carries a <style> block and
// inline style attributes, including on the DOMParser-built SVG icons);
// img-src needs data: (style.css embeds data:image/svg+xml select arrows);
// frame-ancestors 'none' restates X-Frame-Options DENY for CSP-first
// browsers.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Content-Security-Policy",
			"default-src 'self'; script-src 'self' blob:; style-src 'self' 'unsafe-inline'; "+
				"img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
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
