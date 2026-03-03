package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestMetadataHandler(t *testing.T) {
	cfg := &OAuthConfig{
		Issuer: "https://mcp.dbtrail.com",
		Store:  NewMemoryStore(),
	}

	req := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", nil)
	rec := httptest.NewRecorder()
	cfg.MetadataHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var meta map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&meta); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if meta["issuer"] != "https://mcp.dbtrail.com" {
		t.Errorf("unexpected issuer: %v", meta["issuer"])
	}
	if meta["authorization_endpoint"] != "https://mcp.dbtrail.com/oauth/authorize" {
		t.Errorf("unexpected authorization_endpoint: %v", meta["authorization_endpoint"])
	}
	if meta["registration_endpoint"] != "https://mcp.dbtrail.com/oauth/register" {
		t.Errorf("unexpected registration_endpoint: %v", meta["registration_endpoint"])
	}

	// Verify S256 is the only supported code challenge method.
	methods, ok := meta["code_challenge_methods_supported"].([]any)
	if !ok || len(methods) != 1 || methods[0] != "S256" {
		t.Errorf("unexpected code_challenge_methods: %v", meta["code_challenge_methods_supported"])
	}
}

func TestRegisterHandler(t *testing.T) {
	store := NewMemoryStore()
	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: store}

	body := `{
		"client_name": "Claude",
		"redirect_uris": ["https://claude.ai/api/mcp/auth_callback"],
		"grant_types": ["authorization_code", "refresh_token"],
		"response_types": ["code"],
		"token_endpoint_auth_method": "client_secret_post"
	}`
	req := httptest.NewRequest(http.MethodPost, "/oauth/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	cfg.RegisterHandler(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["client_id"] == "" {
		t.Error("expected non-empty client_id")
	}
	if resp["client_secret"] == "" {
		t.Error("expected non-empty client_secret")
	}
	if resp["client_name"] != "Claude" {
		t.Errorf("expected client_name Claude, got %v", resp["client_name"])
	}

	// Verify stored in memory.
	clientID := resp["client_id"].(string)
	if store.Clients[clientID] == nil {
		t.Error("client not stored")
	}
}

func TestRegisterHandler_missingRedirectURIs(t *testing.T) {
	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: NewMemoryStore()}
	body := `{"client_name": "Claude"}`
	req := httptest.NewRequest(http.MethodPost, "/oauth/register", strings.NewReader(body))
	rec := httptest.NewRecorder()
	cfg.RegisterHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestAuthorizeHandler_showsPage(t *testing.T) {
	store := NewMemoryStore()
	store.Clients["test-client"] = &OAuthClient{
		ClientID:     "test-client",
		ClientSecret: "secret",
		RedirectURIs: []string{"https://claude.ai/api/mcp/auth_callback"},
	}
	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: store}

	req := httptest.NewRequest(http.MethodGet,
		"/oauth/authorize?client_id=test-client&redirect_uri=https://claude.ai/api/mcp/auth_callback&code_challenge=abc123&code_challenge_method=S256&state=xyz",
		nil,
	)
	rec := httptest.NewRecorder()
	cfg.AuthorizeHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("expected HTML response, got %s", ct)
	}
	if !strings.Contains(rec.Body.String(), "test-client") {
		t.Error("expected page to contain client_id")
	}
}

func TestAuthorizeHandler_unknownClient(t *testing.T) {
	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: NewMemoryStore()}
	req := httptest.NewRequest(http.MethodGet,
		"/oauth/authorize?client_id=unknown&redirect_uri=https://example.com&code_challenge=abc&code_challenge_method=S256",
		nil,
	)
	rec := httptest.NewRecorder()
	cfg.AuthorizeHandler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestAuthorizeHandler_wrongChallengeMethod(t *testing.T) {
	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: NewMemoryStore()}
	req := httptest.NewRequest(http.MethodGet,
		"/oauth/authorize?client_id=test&redirect_uri=https://example.com&code_challenge=abc&code_challenge_method=plain",
		nil,
	)
	rec := httptest.NewRecorder()
	cfg.AuthorizeHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestAuthorizeSubmitHandler_redirectsWithCode(t *testing.T) {
	store := NewMemoryStore()
	store.Clients["test-client"] = &OAuthClient{
		ClientID:     "test-client",
		ClientSecret: "secret",
		RedirectURIs: []string{"https://claude.ai/api/mcp/auth_callback"},
	}
	store.Tenants["acme-corp"] = &Tenant{
		TenantID: "acme-corp",
		Tier:     "paid",
		Status:   "active",
	}
	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: store}

	form := url.Values{
		"client_id":      {"test-client"},
		"redirect_uri":   {"https://claude.ai/api/mcp/auth_callback"},
		"state":          {"xyz"},
		"code_challenge": {"abc123"},
		"tenant_id":      {"acme-corp"},
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth/authorize", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	cfg.AuthorizeSubmitHandler(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d: %s", rec.Code, rec.Body.String())
	}

	location := rec.Header().Get("Location")
	if !strings.HasPrefix(location, "https://claude.ai/api/mcp/auth_callback?code=") {
		t.Errorf("unexpected redirect location: %s", location)
	}
	if !strings.Contains(location, "state=xyz") {
		t.Errorf("expected state in redirect, got: %s", location)
	}

	// Verify code was stored.
	if len(store.Codes) != 1 {
		t.Errorf("expected 1 code stored, got %d", len(store.Codes))
	}
}

func TestTokenHandler_authCodeGrant(t *testing.T) {
	store := NewMemoryStore()
	store.Clients["test-client"] = &OAuthClient{
		ClientID:     "test-client",
		ClientSecret: "secret",
		RedirectURIs: []string{"https://claude.ai/api/mcp/auth_callback"},
	}

	// Pre-create an authorization code with a known PKCE challenge.
	codeVerifier := "test-verifier-that-is-long-enough-for-pkce"
	codeChallenge := computeS256Challenge(codeVerifier)

	store.Codes["test-code"] = &AuthCode{
		Code:          "test-code",
		ClientID:      "test-client",
		TenantID:      "acme-corp",
		RedirectURI:   "https://claude.ai/api/mcp/auth_callback",
		CodeChallenge: codeChallenge,
		State:         "xyz",
		ExpiresAt:     time.Now().Add(5 * time.Minute).Unix(),
	}

	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: store}

	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {"test-code"},
		"client_id":     {"test-client"},
		"client_secret": {"secret"},
		"code_verifier": {codeVerifier},
		"redirect_uri":  {"https://claude.ai/api/mcp/auth_callback"},
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	cfg.TokenHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["access_token"] == "" {
		t.Error("expected non-empty access_token")
	}
	if resp["refresh_token"] == "" {
		t.Error("expected non-empty refresh_token")
	}
	if resp["token_type"] != "Bearer" {
		t.Errorf("expected Bearer token_type, got %v", resp["token_type"])
	}
	if resp["expires_in"].(float64) != 3600 {
		t.Errorf("expected 3600 expires_in, got %v", resp["expires_in"])
	}

	// Code should be consumed (deleted).
	if len(store.Codes) != 0 {
		t.Error("code should have been consumed")
	}

	// Token and refresh token should be stored.
	if len(store.Tokens) != 1 {
		t.Errorf("expected 1 token stored, got %d", len(store.Tokens))
	}
	if len(store.RefreshTokens) != 1 {
		t.Errorf("expected 1 refresh token stored, got %d", len(store.RefreshTokens))
	}
}

func TestTokenHandler_invalidPKCE(t *testing.T) {
	store := NewMemoryStore()
	store.Clients["test-client"] = &OAuthClient{
		ClientID:     "test-client",
		ClientSecret: "secret",
	}
	store.Codes["test-code"] = &AuthCode{
		Code:          "test-code",
		ClientID:      "test-client",
		TenantID:      "acme",
		CodeChallenge: "the-real-challenge",
		ExpiresAt:     time.Now().Add(5 * time.Minute).Unix(),
	}

	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: store}

	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {"test-code"},
		"client_id":     {"test-client"},
		"client_secret": {"secret"},
		"code_verifier": {"wrong-verifier"},
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	cfg.TokenHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad PKCE, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestTokenHandler_refreshGrant(t *testing.T) {
	store := NewMemoryStore()
	store.Clients["test-client"] = &OAuthClient{
		ClientID:     "test-client",
		ClientSecret: "secret",
	}

	// Create a refresh token.
	rt := "refresh-token-value"
	store.RefreshTokens[HashToken(rt)] = &RefreshTokenRecord{
		RefreshTokenHash: HashToken(rt),
		ClientID:         "test-client",
		TenantID:         "acme-corp",
		ExpiresAt:        time.Now().Add(24 * time.Hour).Unix(),
	}

	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: store}

	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {rt},
		"client_id":     {"test-client"},
		"client_secret": {"secret"},
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	cfg.TokenHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp["access_token"] == "" {
		t.Error("expected new access_token")
	}
	if resp["refresh_token"] == "" {
		t.Error("expected new refresh_token (rotation)")
	}

	// Old refresh token should be consumed.
	if store.RefreshTokens[HashToken(rt)] != nil {
		t.Error("old refresh token should have been consumed")
	}
}

func TestTokenHandler_invalidClientSecret(t *testing.T) {
	store := NewMemoryStore()
	store.Clients["test-client"] = &OAuthClient{
		ClientID:     "test-client",
		ClientSecret: "real-secret",
	}

	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: store}

	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {"some-code"},
		"client_id":     {"test-client"},
		"client_secret": {"wrong-secret"},
		"code_verifier": {"verifier"},
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	cfg.TokenHandler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestTokenHandler_unsupportedGrant(t *testing.T) {
	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: NewMemoryStore()}

	form := url.Values{"grant_type": {"client_credentials"}}
	req := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	cfg.TokenHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestVerifyPKCE(t *testing.T) {
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	challenge := computeS256Challenge(verifier)

	if !verifyPKCE(verifier, challenge) {
		t.Error("expected PKCE verification to pass")
	}
	if verifyPKCE("wrong-verifier", challenge) {
		t.Error("expected PKCE verification to fail for wrong verifier")
	}
}

// computeS256Challenge computes the S256 PKCE challenge for a verifier.
func computeS256Challenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}
