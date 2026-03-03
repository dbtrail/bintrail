package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	accessTokenTTL  = 1 * time.Hour
	refreshTokenTTL = 30 * 24 * time.Hour
	codeTTL         = 10 * time.Minute
	tokenBytes      = 32
)

// OAuthConfig holds the configuration for the OAuth endpoints.
type OAuthConfig struct {
	Issuer      string
	Store       Store
	BackendURL  string
	TablePrefix string
}

// MetadataHandler serves the OAuth 2.1 authorization server metadata.
// GET /.well-known/oauth-authorization-server
func (c *OAuthConfig) MetadataHandler(w http.ResponseWriter, r *http.Request) {
	meta := map[string]any{
		"issuer":                                c.Issuer,
		"authorization_endpoint":                c.Issuer + "/oauth/authorize",
		"token_endpoint":                        c.Issuer + "/oauth/token",
		"registration_endpoint":                 c.Issuer + "/oauth/register",
		"response_types_supported":              []string{"code"},
		"grant_types_supported":                 []string{"authorization_code", "refresh_token"},
		"token_endpoint_auth_methods_supported": []string{"client_secret_post"},
		"code_challenge_methods_supported":      []string{"S256"},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(meta)
}

// RegisterHandler handles Dynamic Client Registration.
// POST /oauth/register
func (c *OAuthConfig) RegisterHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientName              string   `json:"client_name"`
		RedirectURIs            []string `json:"redirect_uris"`
		GrantTypes              []string `json:"grant_types"`
		ResponseTypes           []string `json:"response_types"`
		TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid_request", "malformed JSON body", http.StatusBadRequest)
		return
	}

	if len(req.RedirectURIs) == 0 {
		jsonError(w, "invalid_request", "redirect_uris is required", http.StatusBadRequest)
		return
	}

	clientID := uuid.New().String()
	secret, err := GenerateToken(tokenBytes)
	if err != nil {
		slog.Error("generate client secret", "error", err)
		jsonError(w, "server_error", "failed to generate credentials", http.StatusInternalServerError)
		return
	}

	client := &OAuthClient{
		ClientID:     clientID,
		ClientSecret: secret,
		ClientName:   req.ClientName,
		RedirectURIs: req.RedirectURIs,
		GrantTypes:   req.GrantTypes,
		CreatedAt:    time.Now().Unix(),
	}

	if err := c.Store.CreateClient(r.Context(), client); err != nil {
		slog.Error("store client", "error", err)
		jsonError(w, "server_error", "failed to store client", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"client_id":     clientID,
		"client_secret": secret,
		"client_name":   req.ClientName,
		"redirect_uris": req.RedirectURIs,
	})
}

// AuthorizeHandler shows the login/consent page.
// GET /oauth/authorize
func (c *OAuthConfig) AuthorizeHandler(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("client_id")
	redirectURI := r.URL.Query().Get("redirect_uri")
	state := r.URL.Query().Get("state")
	codeChallenge := r.URL.Query().Get("code_challenge")
	codeChallengeMethod := r.URL.Query().Get("code_challenge_method")

	if clientID == "" || redirectURI == "" || codeChallenge == "" {
		jsonError(w, "invalid_request", "client_id, redirect_uri, and code_challenge are required", http.StatusBadRequest)
		return
	}
	if codeChallengeMethod != "S256" {
		jsonError(w, "invalid_request", "code_challenge_method must be S256", http.StatusBadRequest)
		return
	}

	// Validate client exists.
	client, err := c.Store.GetClient(r.Context(), clientID)
	if err != nil {
		slog.Error("get client", "error", err)
		jsonError(w, "server_error", "internal error", http.StatusInternalServerError)
		return
	}
	if client == nil {
		jsonError(w, "invalid_client", "unknown client_id", http.StatusUnauthorized)
		return
	}

	// Validate redirect_uri is registered.
	if !slices.Contains(client.RedirectURIs, redirectURI) {
		jsonError(w, "invalid_request", "redirect_uri not registered", http.StatusBadRequest)
		return
	}

	// Serve the login page. The form POSTs back to /oauth/authorize with the
	// same query params plus the user's tenant selection.
	// HTML-escape all values to prevent XSS via crafted query parameters.
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, authorizePage,
		html.EscapeString(clientID),
		html.EscapeString(redirectURI),
		html.EscapeString(state),
		html.EscapeString(codeChallenge))
}

// AuthorizeSubmitHandler processes the login form submission.
// POST /oauth/authorize
func (c *OAuthConfig) AuthorizeSubmitHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		jsonError(w, "invalid_request", "malformed form data", http.StatusBadRequest)
		return
	}

	clientID := r.FormValue("client_id")
	redirectURI := r.FormValue("redirect_uri")
	state := r.FormValue("state")
	codeChallenge := r.FormValue("code_challenge")
	tenantID := r.FormValue("tenant_id")

	if clientID == "" || redirectURI == "" || codeChallenge == "" || tenantID == "" {
		jsonError(w, "invalid_request", "missing required fields", http.StatusBadRequest)
		return
	}

	// Validate client.
	client, err := c.Store.GetClient(r.Context(), clientID)
	if err != nil {
		slog.Error("get client", "error", err)
		jsonError(w, "server_error", "internal error", http.StatusInternalServerError)
		return
	}
	if client == nil {
		jsonError(w, "invalid_client", "unknown client_id", http.StatusUnauthorized)
		return
	}

	// Validate redirect_uri is registered for this client.
	if !slices.Contains(client.RedirectURIs, redirectURI) {
		jsonError(w, "invalid_request", "redirect_uri not registered", http.StatusBadRequest)
		return
	}

	// Validate tenant exists.
	tenant, err := c.Store.GetTenant(r.Context(), tenantID)
	if err != nil {
		slog.Error("get tenant", "error", err)
		jsonError(w, "server_error", "internal error", http.StatusInternalServerError)
		return
	}
	if tenant == nil || tenant.Status != "active" {
		jsonError(w, "invalid_request", "unknown or inactive tenant", http.StatusBadRequest)
		return
	}

	// Generate authorization code.
	code, err := GenerateToken(tokenBytes)
	if err != nil {
		slog.Error("generate auth code", "error", err)
		jsonError(w, "server_error", "failed to generate code", http.StatusInternalServerError)
		return
	}

	ac := &AuthCode{
		Code:          code,
		ClientID:      clientID,
		TenantID:      tenantID,
		RedirectURI:   redirectURI,
		CodeChallenge: codeChallenge,
		State:         state,
		ExpiresAt:     time.Now().Add(codeTTL).Unix(),
	}
	if err := c.Store.CreateCode(r.Context(), ac); err != nil {
		slog.Error("store auth code", "error", err)
		jsonError(w, "server_error", "failed to store code", http.StatusInternalServerError)
		return
	}

	// Redirect back to Claude with the authorization code.
	sep := "?"
	if strings.Contains(redirectURI, "?") {
		sep = "&"
	}
	location := redirectURI + sep + "code=" + code
	if state != "" {
		location += "&state=" + state
	}
	http.Redirect(w, r, location, http.StatusFound)
}

// TokenHandler handles token exchange and refresh.
// POST /oauth/token
func (c *OAuthConfig) TokenHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		jsonError(w, "invalid_request", "malformed form data", http.StatusBadRequest)
		return
	}

	grantType := r.FormValue("grant_type")
	switch grantType {
	case "authorization_code":
		c.handleAuthCodeGrant(w, r)
	case "refresh_token":
		c.handleRefreshGrant(w, r)
	default:
		jsonError(w, "unsupported_grant_type", "grant_type must be authorization_code or refresh_token", http.StatusBadRequest)
	}
}

func (c *OAuthConfig) handleAuthCodeGrant(w http.ResponseWriter, r *http.Request) {
	code := r.FormValue("code")
	clientID := r.FormValue("client_id")
	clientSecret := r.FormValue("client_secret")
	codeVerifier := r.FormValue("code_verifier")
	redirectURI := r.FormValue("redirect_uri")

	if code == "" || clientID == "" || clientSecret == "" || codeVerifier == "" {
		jsonError(w, "invalid_request", "code, client_id, client_secret, and code_verifier are required", http.StatusBadRequest)
		return
	}

	// Validate client credentials.
	client, err := c.Store.GetClient(r.Context(), clientID)
	if err != nil {
		slog.Error("get client", "error", err)
		jsonError(w, "server_error", "internal error", http.StatusInternalServerError)
		return
	}
	if client == nil || subtle.ConstantTimeCompare([]byte(client.ClientSecret), []byte(clientSecret)) != 1 {
		jsonError(w, "invalid_client", "invalid client credentials", http.StatusUnauthorized)
		return
	}

	// Consume the authorization code (single-use).
	ac, err := c.Store.ConsumeCode(r.Context(), code)
	if err != nil {
		slog.Error("consume code", "error", err)
		jsonError(w, "server_error", "internal error", http.StatusInternalServerError)
		return
	}
	if ac == nil {
		jsonError(w, "invalid_grant", "invalid or expired authorization code", http.StatusBadRequest)
		return
	}

	// Verify the code belongs to this client.
	if ac.ClientID != clientID {
		jsonError(w, "invalid_grant", "code was not issued to this client", http.StatusBadRequest)
		return
	}

	// Verify redirect_uri matches if provided.
	if redirectURI != "" && redirectURI != ac.RedirectURI {
		jsonError(w, "invalid_grant", "redirect_uri mismatch", http.StatusBadRequest)
		return
	}

	// Verify PKCE: SHA256(code_verifier) must match code_challenge.
	if !verifyPKCE(codeVerifier, ac.CodeChallenge) {
		jsonError(w, "invalid_grant", "PKCE verification failed", http.StatusBadRequest)
		return
	}

	// Issue tokens.
	c.issueTokens(w, r, clientID, ac.TenantID)
}

func (c *OAuthConfig) handleRefreshGrant(w http.ResponseWriter, r *http.Request) {
	refreshToken := r.FormValue("refresh_token")
	clientID := r.FormValue("client_id")
	clientSecret := r.FormValue("client_secret")

	if refreshToken == "" || clientID == "" || clientSecret == "" {
		jsonError(w, "invalid_request", "refresh_token, client_id, and client_secret are required", http.StatusBadRequest)
		return
	}

	// Validate client credentials.
	client, err := c.Store.GetClient(r.Context(), clientID)
	if err != nil {
		slog.Error("get client", "error", err)
		jsonError(w, "server_error", "internal error", http.StatusInternalServerError)
		return
	}
	if client == nil || subtle.ConstantTimeCompare([]byte(client.ClientSecret), []byte(clientSecret)) != 1 {
		jsonError(w, "invalid_client", "invalid client credentials", http.StatusUnauthorized)
		return
	}

	// Consume the refresh token (single-use, rotated).
	rt, err := c.Store.ConsumeRefreshToken(r.Context(), refreshToken)
	if err != nil {
		slog.Error("consume refresh token", "error", err)
		jsonError(w, "server_error", "internal error", http.StatusInternalServerError)
		return
	}
	if rt == nil {
		jsonError(w, "invalid_grant", "invalid or expired refresh token", http.StatusBadRequest)
		return
	}

	if rt.ClientID != clientID {
		jsonError(w, "invalid_grant", "refresh token was not issued to this client", http.StatusBadRequest)
		return
	}

	// Issue new tokens (rotates refresh token).
	c.issueTokens(w, r, clientID, rt.TenantID)
}

func (c *OAuthConfig) issueTokens(w http.ResponseWriter, r *http.Request, clientID, tenantID string) {
	accessToken, err := GenerateToken(tokenBytes)
	if err != nil {
		slog.Error("generate access token", "error", err)
		jsonError(w, "server_error", "failed to generate tokens", http.StatusInternalServerError)
		return
	}

	refreshToken, err := GenerateToken(tokenBytes)
	if err != nil {
		slog.Error("generate refresh token", "error", err)
		jsonError(w, "server_error", "failed to generate tokens", http.StatusInternalServerError)
		return
	}

	now := time.Now()

	tokenRec := &TokenRecord{
		AccessTokenHash: HashToken(accessToken),
		ClientID:        clientID,
		TenantID:        tenantID,
		ExpiresAt:       now.Add(accessTokenTTL).Unix(),
	}
	if err := c.Store.CreateToken(r.Context(), tokenRec); err != nil {
		slog.Error("store access token", "error", err)
		jsonError(w, "server_error", "failed to store token", http.StatusInternalServerError)
		return
	}

	refreshRec := &RefreshTokenRecord{
		RefreshTokenHash: HashToken(refreshToken),
		ClientID:         clientID,
		TenantID:         tenantID,
		ExpiresAt:        now.Add(refreshTokenTTL).Unix(),
	}
	if err := c.Store.CreateRefreshToken(r.Context(), refreshRec); err != nil {
		slog.Error("store refresh token", "error", err)
		jsonError(w, "server_error", "failed to store refresh token", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"access_token":  accessToken,
		"token_type":    "Bearer",
		"expires_in":    int(accessTokenTTL.Seconds()),
		"refresh_token": refreshToken,
	})
}

// ─── PKCE helpers ────────────────────────────────────────────────────────────

// verifyPKCE checks that SHA256(code_verifier) matches the stored code_challenge.
// Uses S256 method per RFC 7636: BASE64URL(SHA256(verifier)) == challenge.
func verifyPKCE(codeVerifier, codeChallenge string) bool {
	h := sha256.Sum256([]byte(codeVerifier))
	computed := base64.RawURLEncoding.EncodeToString(h[:])
	return computed == codeChallenge
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func jsonError(w http.ResponseWriter, errorCode, description string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"error":             errorCode,
		"error_description": description,
	})
}

// authorizePage is a minimal HTML login/consent page. In production, this would
// redirect to a full Bintrail auth UI. For now it accepts a tenant ID directly.
//
// Template placeholders: %s = client_id, redirect_uri, state, code_challenge.
var authorizePage = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bintrail — Authorize</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; max-width: 400px; margin: 80px auto; padding: 0 20px; color: #333; }
    h1 { font-size: 1.4em; }
    label { display: block; margin-top: 16px; font-weight: 600; }
    input[type=text] { width: 100%%; padding: 8px; margin-top: 4px; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box; }
    button { margin-top: 20px; padding: 10px 24px; background: #2563eb; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 1em; }
    button:hover { background: #1d4ed8; }
    .note { font-size: 0.85em; color: #666; margin-top: 8px; }
  </style>
</head>
<body>
  <h1>Authorize Claude to access Bintrail</h1>
  <p>Enter your Bintrail tenant ID to grant Claude access to your binlog data.</p>
  <form method="POST" action="/oauth/authorize">
    <input type="hidden" name="client_id" value="%s">
    <input type="hidden" name="redirect_uri" value="%s">
    <input type="hidden" name="state" value="%s">
    <input type="hidden" name="code_challenge" value="%s">
    <label for="tenant_id">Tenant ID</label>
    <input type="text" id="tenant_id" name="tenant_id" required placeholder="e.g. acme-corp">
    <p class="note">This is the identifier you received when you set up Bintrail.</p>
    <button type="submit">Authorize</button>
  </form>
</body>
</html>`
