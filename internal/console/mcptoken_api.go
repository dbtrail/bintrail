package console

import (
	"errors"
	"log/slog"
	"net/http"
)

// mcpTokenStatusDTO is the wire view of the MCP-token configuration. Values
// are never serialized — only presence, provenance, and (for the managed
// token) the creation time the UI renders.
type mcpTokenStatusDTO struct {
	// Static: a token was supplied via --token / BINTRAIL_CONSOLE_TOKEN. It
	// is environment-owned — the console cannot rotate or revoke it.
	Static bool `json:"static"`
	// Managed: a UI-generated token exists (only its SHA-256 is stored).
	Managed   bool   `json:"managed"`
	CreatedAt string `json:"created_at,omitempty"`
	// ReadOnly: the on-disk token file was written by a newer bintrail —
	// the token authenticates, but generate/rotate/revoke refuse.
	ReadOnly bool `json:"read_only,omitempty"`
}

func (s *Server) mcpTokenStatus() mcpTokenStatusDTO {
	createdAt, readOnly, configured := s.managedTok.info()
	return mcpTokenStatusDTO{
		Static:    s.token != "",
		Managed:   configured,
		CreatedAt: createdAt,
		ReadOnly:  readOnly,
	}
}

// handleMCPTokenGet reports whether tokens are configured — never their
// values. Behind tokenMiddleware like every /api route.
func (s *Server) handleMCPTokenGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.mcpTokenStatus())
}

// handleMCPTokenGenerate mints (or rotates) the managed MCP token: persists
// its SHA-256 to the token file and swaps the in-memory credential so the new
// value authenticates immediately, no restart. The response is the ONE place
// the plaintext ever appears — it is not stored and cannot be re-displayed.
//
// Any authenticated caller may generate/rotate: reaching this handler already
// required the static token, the managed token, or a login session, and the
// minted credential grants the same read-only class — no escalation.
func (s *Server) handleMCPTokenGenerate(w http.ResponseWriter, r *http.Request) {
	token, f, err := GenerateMCPToken(s.mcpTokenPath)
	if err != nil {
		if errors.Is(err, ErrMCPTokenFileReadOnly) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.managedTok.set(f)
	slog.Info("console: managed MCP token generated", "path", s.mcpTokenPath)
	writeJSON(w, http.StatusOK, struct {
		Token     string `json:"token"`
		CreatedAt string `json:"created_at"`
	}{Token: token, CreatedAt: f.CreatedAt})
}

// handleMCPTokenRevoke deletes the managed token (file and in-memory). The
// static environment token, if any, is untouched — it is not ours to revoke.
func (s *Server) handleMCPTokenRevoke(w http.ResponseWriter, r *http.Request) {
	if err := RevokeMCPToken(s.mcpTokenPath); err != nil {
		if errors.Is(err, ErrMCPTokenFileReadOnly) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.managedTok.set(nil)
	slog.Info("console: managed MCP token revoked", "path", s.mcpTokenPath)
	writeJSON(w, http.StatusOK, s.mcpTokenStatus())
}
