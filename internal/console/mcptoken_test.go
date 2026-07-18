package console

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

func TestMCPTokenFile_GenerateLoadRotateRevoke(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-token.yaml")

	// Missing file = not configured, no error.
	if f, err := LoadMCPTokenFile(path); err != nil || f != nil {
		t.Fatalf("missing file: got (%v, %v), want (nil, nil)", f, err)
	}

	token, f, err := GenerateMCPToken(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(token, mcpTokenPrefix) {
		t.Errorf("token %q missing %q prefix", token, mcpTokenPrefix)
	}
	sum := sha256.Sum256([]byte(token))
	if f.TokenSHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("stored sha %q does not match sha256(token)", f.TokenSHA256)
	}
	if strings.Contains(mustReadFile(t, path), token) {
		t.Error("plaintext token must never be persisted")
	}
	if runtime.GOOS != "windows" {
		if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
			t.Errorf("token file mode = %v (err %v), want 0600", info.Mode().Perm(), err)
		}
	}

	loaded, err := LoadMCPTokenFile(path)
	if err != nil || loaded == nil || loaded.TokenSHA256 != f.TokenSHA256 {
		t.Fatalf("reload mismatch: %+v, %v", loaded, err)
	}

	// Rotate replaces the hash.
	token2, f2, err := GenerateMCPToken(path)
	if err != nil {
		t.Fatal(err)
	}
	if token2 == token || f2.TokenSHA256 == f.TokenSHA256 {
		t.Error("rotation must mint a fresh token")
	}

	if err := RevokeMCPToken(path); err != nil {
		t.Fatal(err)
	}
	if f, err := LoadMCPTokenFile(path); err != nil || f != nil {
		t.Fatalf("after revoke: got (%v, %v), want (nil, nil)", f, err)
	}
	// Revoking again is a no-op.
	if err := RevokeMCPToken(path); err != nil {
		t.Fatalf("second revoke: %v", err)
	}
}

func TestMCPTokenFile_NewerVersionReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-token.yaml")
	sum := sha256.Sum256([]byte("bmt_future"))
	content := "version: 99\ntoken_sha256: " + hex.EncodeToString(sum[:]) + "\ncreated_at: 2030-01-01T00:00:00Z\nfuture_field: kept\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := LoadMCPTokenFile(path)
	if err != nil || !f.ReadOnly() {
		t.Fatalf("newer-version file: f=%+v err=%v, want read-only", f, err)
	}
	if _, _, err := GenerateMCPToken(path); !errors.Is(err, ErrMCPTokenFileReadOnly) {
		t.Errorf("generate on newer-version file: %v, want ErrMCPTokenFileReadOnly", err)
	}
	if err := RevokeMCPToken(path); !errors.Is(err, ErrMCPTokenFileReadOnly) {
		t.Errorf("revoke on newer-version file: %v, want ErrMCPTokenFileReadOnly", err)
	}
}

func TestMCPTokenFile_MalformedShaFailsLoud(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-token.yaml")
	if err := os.WriteFile(path, []byte("version: 1\ntoken_sha256: nothex\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadMCPTokenFile(path); err == nil {
		t.Fatal("malformed token_sha256 must fail loud, got nil error")
	}
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// newManagedServer builds a Server like newBootServer but with a managed-token
// path under t.TempDir() and an optional static token.
func newManagedServer(t *testing.T, staticToken string) *Server {
	t.Helper()
	db, _, closer := newSQLMock(t)
	t.Cleanup(closer)
	s := &Server{
		token:        staticToken,
		cm:           newConnManager(nil, false),
		mcpTokenPath: filepath.Join(t.TempDir(), "mcp-token.yaml"),
		sessions:     newSessionStore(),
		loginLimiter: newLoginLimiter(),
	}
	s.cm.boot = &bundle{db: db, engine: query.New(db), noArchive: true}
	s.mux = s.buildHandler()
	return s
}

func doJSON(t *testing.T, s *Server, method, path, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)
	return rec
}

// TestManagedToken_APILifecycle drives generate → authenticate → rotate →
// revoke through the HTTP surface, asserting the minted value works
// immediately (no restart) on both /api and /mcp, and stops working after
// rotate/revoke.
func TestManagedToken_APILifecycle(t *testing.T) {
	s := newManagedServer(t, "static-tok")

	// Status before: static only.
	rec := doJSON(t, s, "GET", "/api/mcp-token", "static-tok")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"static":true`) || !strings.Contains(rec.Body.String(), `"managed":false`) {
		t.Fatalf("initial status = %d %s", rec.Code, rec.Body.String())
	}

	// Generate via the static token.
	rec = doJSON(t, s, "POST", "/api/mcp-token", "static-tok")
	if rec.Code != 200 {
		t.Fatalf("generate = %d: %s", rec.Code, rec.Body.String())
	}
	var minted struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &minted); err != nil || !strings.HasPrefix(minted.Token, mcpTokenPrefix) {
		t.Fatalf("generate response %q: %v", rec.Body.String(), err)
	}

	// The managed token authenticates the API and /mcp immediately.
	if rec := doJSON(t, s, "GET", "/api/mcp-token", minted.Token); rec.Code != 200 {
		t.Fatalf("managed token on /api = %d", rec.Code)
	}
	if rec := doMCP(t, s, "/mcp", minted.Token); rec.Code != 200 {
		t.Fatalf("managed token on /mcp = %d: %s", rec.Code, rec.Body.String())
	}
	// Capabilities reflect it.
	if rec := doJSON(t, s, "GET", "/api/capabilities", minted.Token); !strings.Contains(rec.Body.String(), `"mcp":true`) {
		t.Fatalf("capabilities.mcp should be true: %s", rec.Body.String())
	}

	// Rotation invalidates the previous value at once.
	rec = doJSON(t, s, "POST", "/api/mcp-token", "static-tok")
	if rec.Code != 200 {
		t.Fatalf("rotate = %d", rec.Code)
	}
	if rec := doJSON(t, s, "GET", "/api/mcp-token", minted.Token); rec.Code != 401 {
		t.Fatalf("pre-rotation token still accepted: %d", rec.Code)
	}

	// Revoke drops the managed credential; the static one is untouched.
	if rec := doJSON(t, s, "DELETE", "/api/mcp-token", "static-tok"); rec.Code != 200 {
		t.Fatalf("revoke = %d", rec.Code)
	}
	if rec := doJSON(t, s, "GET", "/api/mcp-token", "static-tok"); !strings.Contains(rec.Body.String(), `"managed":false`) {
		t.Fatalf("post-revoke status: %s", rec.Body.String())
	}
}

// TestManagedToken_MCPWithoutStaticToken pins the zero-config path: no
// --token, only a managed token — the /mcp gate opens and the credential
// authenticates, while a wrong value still 401s and a token-less console
// still 403s.
func TestManagedToken_MCPWithoutStaticToken(t *testing.T) {
	s := newManagedServer(t, "")

	// No credential configured at all: the gate refuses and names the UI path.
	rec := doMCP(t, s, "/mcp", "anything")
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "Connect AI") {
		t.Fatalf("token-less /mcp = %d %s, want 403 naming Settings → Connect AI", rec.Code, rec.Body.String())
	}

	token, f, err := GenerateMCPToken(s.mcpTokenPath)
	if err != nil {
		t.Fatal(err)
	}
	s.managedTok.set(f)

	if rec := doMCP(t, s, "/mcp", token); rec.Code != 200 {
		t.Fatalf("managed-only /mcp = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doMCP(t, s, "/mcp", "wrong"); rec.Code != 401 {
		t.Fatalf("wrong token = %d, want 401", rec.Code)
	}
}

// TestManagedToken_CannotClaimFirstPassword pins the bootstrap-trust-root
// invariant: only the STATIC token may set the first password; the managed
// token (authKindManaged) is refused.
func TestManagedToken_CannotClaimFirstPassword(t *testing.T) {
	s := newManagedServer(t, "static-tok")
	s.authPath = filepath.Join(t.TempDir(), "auth.yaml") // no password set

	_, f, err := GenerateMCPToken(s.mcpTokenPath)
	if err != nil {
		t.Fatal(err)
	}
	s.managedTok.set(f)
	token := regenerateManagedPlaintext(t, s)

	req := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/auth/password", strings.NewReader(`{"new_password":"hunter22aa"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)
	if rec.Code != 403 {
		t.Fatalf("managed token claiming first password = %d, want 403; body: %s", rec.Code, rec.Body.String())
	}
}

// regenerateManagedPlaintext rotates the managed token and returns the fresh
// plaintext, keeping the server's in-memory credential in sync.
func regenerateManagedPlaintext(t *testing.T, s *Server) string {
	t.Helper()
	token, f, err := GenerateMCPToken(s.mcpTokenPath)
	if err != nil {
		t.Fatal(err)
	}
	s.managedTok.set(f)
	return token
}
