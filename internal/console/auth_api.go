package console

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxLoginBody bounds the login/change-password request bodies; both are a
// few short strings.
const maxLoginBody = 4 << 10

// clientIP returns the host part of RemoteAddr. X-Forwarded-For is never
// consulted — it is attacker-controlled.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// requireJSONBody enforces Content-Type: application/json and a size cap on
// the pre-auth endpoints. The Content-Type check is a CSRF defense in its own
// right: an HTML form can only submit urlencoded/multipart/text-plain, so a
// cross-site form POST can never reach the login verifier.
func requireJSONBody(w http.ResponseWriter, r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	if mt, _, _ := strings.Cut(ct, ";"); strings.TrimSpace(mt) != "application/json" {
		writeJSONError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxLoginBody)
	return true
}

// handleAuthInfo serves GET /api/auth — unauthenticated (root mux, like
// healthz; hostGuard still applies). It reveals only whether password login
// exists, which the login form's existence would reveal anyway. The file is
// statted per request so `user set-password` against a live server lights the
// login form up without a restart.
func (s *Server) handleAuthInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"password_login": s.passwordLoginEnabled()})
}

// passwordLoginEnabled re-checks the auth file at call time. A corrupt file
// reads as enabled: login attempts against it fail loud (LoadAuthFile error)
// rather than silently downgrading the console to token-only auth.
func (s *Server) passwordLoginEnabled() bool {
	if s.authPath == "" {
		return false
	}
	a, err := LoadAuthFile(s.authPath)
	return err != nil || a != nil
}

// handleLogin serves POST /api/auth/login — unauthenticated, rate-limited,
// bcrypt-verified. Success mints an in-memory session token the SPA uses as
// its Bearer credential.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if !requireJSONBody(w, r) {
		return
	}
	ip := clientIP(r)
	// Rate-limit check FIRST: a throttled attacker gets a cheap 429 before
	// any file read or bcrypt work.
	if ok, retry := s.loginLimiter.Allow(ip); !ok {
		secs := int(retry.Seconds()) + 1
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		writeJSONError(w, http.StatusTooManyRequests, fmt.Sprintf("too many attempts; retry in %ds", secs))
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		writeJSONError(w, status, "invalid JSON body")
		return
	}

	a, err := LoadAuthFile(s.authPath)
	if err != nil {
		slog.Error("console auth file unreadable", "path", s.authPath, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "console auth file is unreadable; see server log")
		return
	}
	if a == nil {
		writeJSONError(w, http.StatusForbidden, "password login is not enabled")
		return
	}

	if !verifyAndMaybeRehash(s.authPath, a, req.Username, req.Password) {
		s.loginLimiter.Fail(ip)
		// One uniform body for bad user and bad password, and the attempted
		// username is NEVER logged (passwords get typed into username fields).
		slog.Warn("console login failed", "remote", ip)
		writeJSONError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}

	s.loginLimiter.Success(ip)
	token, expires, err := s.sessions.Issue()
	if err != nil {
		slog.Error("console session issue failed", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to issue session")
		return
	}
	slog.Info("console login", "remote", ip)
	writeJSON(w, http.StatusOK, map[string]string{
		"token":      token,
		"expires_at": expires.UTC().Format(time.RFC3339),
	})
}

// handleLogout serves POST /api/auth/logout (authenticated). It revokes the
// presented Bearer when it is a session; a static token is not revocable over
// HTTP by design, so logout with one is a 204 no-op. Idempotent.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	s.sessions.Revoke(bearerToken(r))
	w.WriteHeader(http.StatusNoContent)
}

// handlePasswordChange serves POST /api/auth/password (authenticated,
// rate-limited — it bcrypt-verifies current_password, making it the
// privilege-escalation path for a stolen session token).
//
// Rules: with a password configured, current_password is required and
// verified no matter how the caller authenticated. With none configured
// (first set), the caller must hold the STATIC token: the token is the
// bootstrap trust root, and a session that outlived `user remove` must not
// be able to claim the new password.
func (s *Server) handlePasswordChange(w http.ResponseWriter, r *http.Request) {
	if !requireJSONBody(w, r) {
		return
	}
	ip := clientIP(r)
	if ok, retry := s.loginLimiter.Allow(ip); !ok {
		secs := int(retry.Seconds()) + 1
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		writeJSONError(w, http.StatusTooManyRequests, fmt.Sprintf("too many attempts; retry in %ds", secs))
		return
	}

	var req struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		writeJSONError(w, status, "invalid JSON body")
		return
	}

	a, err := LoadAuthFile(s.authPath)
	if err != nil {
		slog.Error("console auth file unreadable", "path", s.authPath, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "console auth file is unreadable; see server log")
		return
	}

	switch {
	case a != nil:
		if !a.VerifyPassword(a.Username, req.CurrentPassword) {
			s.loginLimiter.Fail(ip)
			writeJSONError(w, http.StatusUnauthorized, "invalid current password")
			return
		}
	default: // first set
		if authKindFrom(r.Context()) != authKindToken {
			writeJSONError(w, http.StatusForbidden, "setting the first password requires authenticating with the access token")
			return
		}
		if req.CurrentPassword != "" {
			writeJSONError(w, http.StatusUnprocessableEntity, "no password is set; current_password must be empty")
			return
		}
	}

	if err := ValidateNewPassword(req.NewPassword); err != nil {
		writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if err := SetAuthPassword(s.authPath, "", req.NewPassword); err != nil {
		if errors.Is(err, ErrAuthFileReadOnly) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		slog.Error("console password write failed", "path", s.authPath, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to write the auth file; see server log")
		return
	}

	// Every existing session dies with the old password; the caller gets a
	// fresh one so they stay signed in.
	s.loginLimiter.Success(ip)
	s.sessions.RevokeAll()
	token, expires, err := s.sessions.Issue()
	if err != nil {
		slog.Error("console session issue failed after password change", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "password changed, but failed to issue a session — sign in again")
		return
	}
	slog.Info("console password changed", "remote", ip)
	writeJSON(w, http.StatusOK, map[string]string{
		"token":      token,
		"expires_at": expires.UTC().Format(time.RFC3339),
	})
}
