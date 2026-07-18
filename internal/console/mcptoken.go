package console

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.yaml.in/yaml/v2"
)

// mcpTokenFileVersion is the managed-MCP-token file schema version this binary
// writes and fully understands. A file with a HIGHER version loads read-only
// (the token still authenticates; generate/rotate/revoke refuse) — same
// contract as the auth file and the server registry.
const mcpTokenFileVersion = 1

// mcpTokenPrefix makes a managed token recognizable in configs and logs
// ("bmt_" = bintrail managed token) without revealing anything — the shape is
// not a secret, the value is.
const mcpTokenPrefix = "bmt_"

// ErrMCPTokenFileReadOnly is returned by write paths when the on-disk file was
// written by a newer bintrail (version > mcpTokenFileVersion).
var ErrMCPTokenFileReadOnly = errors.New("MCP token file was written by a newer bintrail; the token works but changes are refused")

// MCPTokenFile is the on-disk managed MCP token: only the SHA-256 of the
// token is persisted (API-key pattern — the plaintext is shown once at
// generation and never stored), so a read of the file does not yield a usable
// credential. The versioned envelope and Extra leave room for additive fields
// without a format break (authfile.go precedent).
type MCPTokenFile struct {
	Version     int    `yaml:"version"`
	TokenSHA256 string `yaml:"token_sha256"`
	CreatedAt   string `yaml:"created_at"`
	// Extra preserves unknown top-level fields a newer binary wrote, across
	// this binary's load→save cycle.
	Extra map[string]any `yaml:",inline"`

	readOnly bool
}

// mcpTokenFileMu serializes writers within this process (the generate/revoke
// handlers). Cross-process collisions are last-writer-wins on the atomic
// rename — acceptable for a single-credential file.
var mcpTokenFileMu sync.Mutex

// DefaultMCPTokenPath returns ~/.config/bintrail/console-mcp-token.yaml, with
// the same relative fallback as DefaultAuthPath for homeless environments.
func DefaultMCPTokenPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".config", "bintrail", "console-mcp-token.yaml")
	}
	return filepath.Join(home, ".config", "bintrail", "console-mcp-token.yaml")
}

// LoadMCPTokenFile reads the managed-token file at path. A missing file
// returns (nil, nil): no managed token configured. A present-but-unreadable
// or structurally-broken file is a loud error — silently ignoring a
// credential the operator minted would leave "why does my token 401" with no
// clue.
func LoadMCPTokenFile(path string) (*MCPTokenFile, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read console MCP token file %s: %w", path, err)
	}
	var f MCPTokenFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse console MCP token file %s: %w", path, err)
	}
	if f.Version > mcpTokenFileVersion {
		f.readOnly = true
	}
	if raw, err := hex.DecodeString(f.TokenSHA256); err != nil || len(raw) != sha256.Size {
		return nil, fmt.Errorf("console MCP token file %s has a malformed token_sha256; delete it and generate a new token from Settings → Connect AI", path)
	}
	return &f, nil
}

// ReadOnly reports whether write paths must refuse (newer-version file).
func (f *MCPTokenFile) ReadOnly() bool { return f != nil && f.readOnly }

// GenerateMCPToken mints a new managed token, persists its SHA-256 at path
// (atomic, 0600), and returns the plaintext and the stored record. A
// pre-existing file is replaced (rotate); a newer-version file refuses with
// ErrMCPTokenFileReadOnly.
func GenerateMCPToken(path string) (string, *MCPTokenFile, error) {
	mcpTokenFileMu.Lock()
	defer mcpTokenFileMu.Unlock()

	existing, err := LoadMCPTokenFile(path)
	if err != nil {
		return "", nil, err
	}
	if existing.ReadOnly() {
		return "", nil, fmt.Errorf("%s: %w", path, ErrMCPTokenFileReadOnly)
	}

	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, fmt.Errorf("generate MCP token: %w", err)
	}
	token := mcpTokenPrefix + hex.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))

	f := MCPTokenFile{
		Version:     mcpTokenFileVersion,
		TokenSHA256: hex.EncodeToString(sum[:]),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}
	if existing != nil {
		f.Extra = existing.Extra
	}
	if err := saveMCPTokenFile(path, &f); err != nil {
		return "", nil, err
	}
	return token, &f, nil
}

// RevokeMCPToken removes the managed-token file. A missing file is a no-op;
// a newer-version file refuses with ErrMCPTokenFileReadOnly.
func RevokeMCPToken(path string) error {
	mcpTokenFileMu.Lock()
	defer mcpTokenFileMu.Unlock()

	existing, err := LoadMCPTokenFile(path)
	if err != nil {
		return err
	}
	if existing == nil {
		return nil
	}
	if existing.ReadOnly() {
		return fmt.Errorf("%s: %w", path, ErrMCPTokenFileReadOnly)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove console MCP token file %s: %w", path, err)
	}
	return nil
}

// saveMCPTokenFile writes atomically: marshal → temp file in the same
// directory → fsync → rename. File 0600, directory 0700 — it holds a
// credential hash (same class as the auth file). Callers hold mcpTokenFileMu.
func saveMCPTokenFile(path string, f *MCPTokenFile) error {
	data, err := yaml.Marshal(f)
	if err != nil {
		return fmt.Errorf("marshal console MCP token file %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create MCP token directory %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".console-mcp-token-*.yaml")
	if err != nil {
		return fmt.Errorf("create temp MCP token file in %s: %w", dir, err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp MCP token file for %s: %w", path, err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write console MCP token file %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync console MCP token file %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp MCP token file for %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replace console MCP token file %s: %w", path, err)
	}
	return nil
}

// managedMCPToken is the server's in-memory view of the managed token,
// mutable at runtime (generate/rotate/revoke apply without a restart) and
// read on every authenticated request — hence the RWMutex.
type managedMCPToken struct {
	mu        sync.RWMutex
	sha256Hex string // "" = no managed token configured
	createdAt string
	readOnly  bool
}

func (m *managedMCPToken) set(f *MCPTokenFile) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if f == nil {
		m.sha256Hex, m.createdAt, m.readOnly = "", "", false
		return
	}
	m.sha256Hex, m.createdAt, m.readOnly = f.TokenSHA256, f.CreatedAt, f.readOnly
}

func (m *managedMCPToken) configured() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sha256Hex != ""
}

func (m *managedMCPToken) info() (createdAt string, readOnly bool, configured bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.createdAt, m.readOnly, m.sha256Hex != ""
}

// matches reports whether got is the managed token, comparing SHA-256 digests
// in constant time. Hashing first also equalizes the compare length, so the
// stored digest length leaks nothing about the token.
func (m *managedMCPToken) matches(got string) bool {
	m.mu.RLock()
	stored := m.sha256Hex
	m.mu.RUnlock()
	if stored == "" || got == "" {
		return false
	}
	sum := sha256.Sum256([]byte(got))
	return subtle.ConstantTimeCompare([]byte(hex.EncodeToString(sum[:])), []byte(stored)) == 1
}
