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

// ─── #132 per-tenant auth_secret on /oauth/authorize ─────────────────────────

// TestAuthorizeSubmitHandler_correctSecret pins the happy path: a
// tenant with AuthSecretHash set, the matching cleartext submitted,
// authorize succeeds with a 302 + code (same shape as the legacy
// test above).
func TestAuthorizeSubmitHandler_correctSecret(t *testing.T) {
	hash, err := HashSecret("hunter2")
	if err != nil {
		t.Fatalf("HashSecret: %v", err)
	}
	store := NewMemoryStore()
	store.Clients["test-client"] = &OAuthClient{
		ClientID: "test-client", ClientSecret: "secret",
		RedirectURIs: []string{"https://claude.ai/api/mcp/auth_callback"},
	}
	store.Tenants["acme-corp"] = &Tenant{
		TenantID: "acme-corp", Status: "active", AuthSecretHash: hash,
	}
	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: store}

	form := url.Values{
		"client_id":      {"test-client"},
		"redirect_uri":   {"https://claude.ai/api/mcp/auth_callback"},
		"state":          {"xyz"},
		"code_challenge": {"abc123"},
		"tenant_id":      {"acme-corp"},
		"tenant_secret":  {"hunter2"},
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth/authorize", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	cfg.AuthorizeSubmitHandler(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestAuthorizeSubmitHandler_wrongSecret pins the security-critical
// reject case. A tenant with AuthSecretHash set + a wrong cleartext
// must return 401 — the bug the issue fixes is that anyone guessing
// a valid tenant ID could obtain a token without authentication.
func TestAuthorizeSubmitHandler_wrongSecret(t *testing.T) {
	hash, err := HashSecret("hunter2")
	if err != nil {
		t.Fatalf("HashSecret: %v", err)
	}
	store := NewMemoryStore()
	store.Clients["test-client"] = &OAuthClient{
		ClientID: "test-client", ClientSecret: "secret",
		RedirectURIs: []string{"https://claude.ai/api/mcp/auth_callback"},
	}
	store.Tenants["acme-corp"] = &Tenant{
		TenantID: "acme-corp", Status: "active", AuthSecretHash: hash,
	}
	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: store}

	form := url.Values{
		"client_id":      {"test-client"},
		"redirect_uri":   {"https://claude.ai/api/mcp/auth_callback"},
		"code_challenge": {"abc123"},
		"tenant_id":      {"acme-corp"},
		"tenant_secret":  {"wrong-password"},
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth/authorize", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	cfg.AuthorizeSubmitHandler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(store.Codes) != 0 {
		t.Errorf("no auth code must be created on a secret mismatch, got %d", len(store.Codes))
	}
}

// TestAuthorizeSubmitHandler_missingSecretOnConfiguredTenant pins
// that omitting the secret entirely against a tenant that HAS one
// is rejected (no silent fallback to legacy-no-secret behaviour).
func TestAuthorizeSubmitHandler_missingSecretOnConfiguredTenant(t *testing.T) {
	hash, err := HashSecret("hunter2")
	if err != nil {
		t.Fatalf("HashSecret: %v", err)
	}
	store := NewMemoryStore()
	store.Clients["test-client"] = &OAuthClient{
		ClientID: "test-client", ClientSecret: "secret",
		RedirectURIs: []string{"https://claude.ai/api/mcp/auth_callback"},
	}
	store.Tenants["acme-corp"] = &Tenant{
		TenantID: "acme-corp", Status: "active", AuthSecretHash: hash,
	}
	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: store}

	form := url.Values{
		"client_id":      {"test-client"},
		"redirect_uri":   {"https://claude.ai/api/mcp/auth_callback"},
		"code_challenge": {"abc123"},
		"tenant_id":      {"acme-corp"},
		// tenant_secret intentionally omitted
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth/authorize", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	cfg.AuthorizeSubmitHandler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestAuthorizeSubmitHandler_legacyTenantRejected pins the
// strict-mode decision from the #295 review: a tenant with an EMPTY
// AuthSecretHash (migrated from before #132) CANNOT authorize. The
// earlier gradual-rollout design returned 302 with an auth code in
// this case, which was a wire-level oracle attackers could use to
// (a) enumerate unmigrated tenants by status code and (b) obtain
// free auth codes for them. Closing the oracle is the load-bearing
// security fix.
//
// Operators discover unmigrated tenants via the
// `legacy_no_secret_configured` structured attribute on the slog.Warn
// the handler emits on every rejection — same inventory, no
// wire-level signal to attackers.
func TestAuthorizeSubmitHandler_legacyTenantRejected(t *testing.T) {
	store := NewMemoryStore()
	store.Clients["test-client"] = &OAuthClient{
		ClientID: "test-client", ClientSecret: "secret",
		RedirectURIs: []string{"https://claude.ai/api/mcp/auth_callback"},
	}
	store.Tenants["legacy-tenant"] = &Tenant{
		TenantID: "legacy-tenant", Status: "active",
		// AuthSecretHash deliberately empty — pre-#132 tenant
	}
	cfg := &OAuthConfig{Issuer: "https://mcp.dbtrail.com", Store: store}

	form := url.Values{
		"client_id":      {"test-client"},
		"redirect_uri":   {"https://claude.ai/api/mcp/auth_callback"},
		"code_challenge": {"abc123"},
		"tenant_id":      {"legacy-tenant"},
		// tenant_secret omitted
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth/authorize", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	cfg.AuthorizeSubmitHandler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on legacy tenant (strict mode); got %d: %s", rec.Code, rec.Body.String())
	}
	if len(store.Codes) != 0 {
		t.Errorf("no auth code must be created on a legacy-tenant reject; got %d", len(store.Codes))
	}
}

// TestHashSecretAndVerify pins the bcrypt round-trip and the
// strict-mode behaviour of VerifySecret: HashSecret produces a
// valid bcrypt hash (prefix $2…), VerifySecret accepts the correct
// cleartext and rejects everything else — including any submission
// against an empty stored hash (#295 review).
func TestHashSecretAndVerify(t *testing.T) {
	hash, err := HashSecret("hunter2")
	if err != nil {
		t.Fatalf("HashSecret: %v", err)
	}
	if hash == "" || hash == "hunter2" {
		t.Errorf("HashSecret must not return empty or echo cleartext, got %q", hash)
	}
	if !strings.HasPrefix(hash, "$2") {
		t.Errorf("HashSecret output %q lacks the bcrypt prefix $2 — algorithm-substitution regression?", hash)
	}
	tenant := &Tenant{AuthSecretHash: hash}
	if !tenant.VerifySecret("hunter2") {
		t.Errorf("VerifySecret(correct) = false, want true")
	}
	if tenant.VerifySecret("wrong-password") {
		t.Errorf("VerifySecret(wrong) = true, want false")
	}
	// Strict mode (#295): empty hash must reject any submission.
	legacy := &Tenant{AuthSecretHash: ""}
	if legacy.VerifySecret("anything") {
		t.Errorf("VerifySecret on legacy tenant (empty hash) must return false in strict mode")
	}
	if legacy.VerifySecret("") {
		t.Errorf("VerifySecret on legacy tenant with empty input must return false in strict mode")
	}
}
