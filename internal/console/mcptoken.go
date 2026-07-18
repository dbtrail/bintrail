package console

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.yaml.in/yaml/v2"
)

// mcpTokenFileVersion is the managed-MCP-token file schema version this binary
// writes and fully understands. A file with a HIGHER version loads read-only
// (generate/rotate/revoke refuse; the token authenticates only if its digest
// field is still readable) — same contract as the auth file and the registry.
const mcpTokenFileVersion = 1

// mcpTokenPrefix makes a managed token recognizable in configs and logs
// ("bmt_" = bintrail managed token) without revealing anything — the shape is
// not a secret, the value is.
const mcpTokenPrefix = "bmt_"

// ErrMCPTokenFileReadOnly is returned by write paths when the on-disk file was
// written by a newer bintrail (version > mcpTokenFileVersion).
var ErrMCPTokenFileReadOnly = errors.New("MCP token file was written by a newer bintrail; changes are refused")

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

// mcpTokenFileMu serializes writers within this process. Cross-process
// collisions are last-writer-wins on the atomic rename — acceptable for a
// single-credential file.
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
// returns (nil, nil): no managed token configured. An unparseable file, or a
// malformed digest in a file THIS version owns, is an error. A NEWER-version
// file is never an error for its payload: it loads read-only, and a digest
// this binary can't read simply never authenticates — a newer format must
// degrade, not brick startup or demand deletion (the readOnly contract).
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
		return &f, nil
	}
	if _, err := f.digest(); err != nil {
		return nil, fmt.Errorf("console MCP token file %s has a malformed token_sha256; delete it and generate a new token from Settings → Connect AI", path)
	}
	return &f, nil
}

// ReadOnly reports whether write paths must refuse (newer-version file).
func (f *MCPTokenFile) ReadOnly() bool { return f != nil && f.readOnly }

// digest returns the decoded SHA-256, or an error when the field is not a
// 32-byte hex string.
func (f *MCPTokenFile) digest() ([]byte, error) {
	raw, err := hex.DecodeString(f.TokenSHA256)
	if err != nil {
		return nil, err
	}
	if len(raw) != sha256.Size {
		return nil, fmt.Errorf("digest is %d bytes, want %d", len(raw), sha256.Size)
	}
	return raw, nil
}

// GenerateMCPToken mints a new managed token, persists its SHA-256 at path
// (atomic, 0600), and returns the plaintext and the stored record. A
// pre-existing file is replaced (rotate); a corrupt v1 file is replaced too
// (the UI's Generate button is the documented self-heal); only a
// newer-version file refuses, with ErrMCPTokenFileReadOnly.
func GenerateMCPToken(path string) (string, *MCPTokenFile, error) {
	mcpTokenFileMu.Lock()
	defer mcpTokenFileMu.Unlock()

	existing, err := LoadMCPTokenFile(path)
	if err != nil {
		slog.Warn("console: replacing unreadable MCP token file", "path", path, "error", err)
		existing = nil
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

// RevokeMCPToken removes the managed-token file. A missing file is a no-op, a
// corrupt v1 file is removed (revoking junk is still revoking), and a
// newer-version file refuses with ErrMCPTokenFileReadOnly.
func RevokeMCPToken(path string) error {
	mcpTokenFileMu.Lock()
	defer mcpTokenFileMu.Unlock()

	existing, err := LoadMCPTokenFile(path)
	if err == nil && existing == nil {
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

// managedMCPToken is the server's live view of the managed token. It re-reads
// the file when its mtime/size changes, so a rotate or revoke performed by
// ANOTHER console process sharing the same path takes effect here without a
// restart — a revoke that reports success must actually revoke everywhere
// (the same staleness discipline as LoadAuthFile-per-login). The stat runs
// only on /mcp authentication and the Connect AI status endpoint — never on
// the general /api hot path, which does not accept this credential.
type managedMCPToken struct {
	mu   sync.Mutex
	path string

	digest    []byte // raw SHA-256; nil = no managed token usable
	createdAt string
	readOnly  bool

	statOK bool
	mtime  time.Time
	size   int64
}

// initFromDisk seeds the state at New() time. Load errors degrade to
// not-configured (already logged by the caller) — an auxiliary credential
// file must never block console startup.
func (m *managedMCPToken) initFromDisk(path string, f *MCPTokenFile) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.path = path
	m.apply(f)
	m.recordStat()
}

// apply projects a loaded file (or nil) into the live fields. Callers hold mu.
func (m *managedMCPToken) apply(f *MCPTokenFile) {
	if f == nil {
		m.digest, m.createdAt, m.readOnly = nil, "", false
		return
	}
	d, err := f.digest()
	if err != nil {
		// Only reachable for newer-version files (v1 digests are validated at
		// load): the file exists but this binary cannot authenticate against
		// it. readOnly is still reported so the UI can explain.
		d = nil
	}
	m.digest, m.createdAt, m.readOnly = d, f.CreatedAt, f.readOnly
}

// recordStat snapshots the file's identity for change detection. Callers hold mu.
func (m *managedMCPToken) recordStat() {
	fi, err := os.Stat(m.path)
	if err != nil {
		m.statOK, m.mtime, m.size = false, time.Time{}, 0
		return
	}
	m.statOK, m.mtime, m.size = true, fi.ModTime(), fi.Size()
}

// refresh re-loads the file if it changed on disk since the last look.
// Callers hold mu. A file that became unreadable degrades to not-configured
// (deny) with a warning — never a panic, never a stale accept.
func (m *managedMCPToken) refresh() {
	if m.path == "" {
		return
	}
	fi, err := os.Stat(m.path)
	switch {
	case err != nil:
		if m.statOK || m.digest != nil {
			// Present before, gone now: revoked (possibly by another process).
			m.apply(nil)
		}
		m.statOK, m.mtime, m.size = false, time.Time{}, 0
		return
	case m.statOK && fi.ModTime().Equal(m.mtime) && fi.Size() == m.size:
		return // unchanged
	}
	f, err := LoadMCPTokenFile(m.path)
	if err != nil {
		slog.Warn("console: MCP token file became unreadable; managed token disabled until regenerated", "path", m.path, "error", err)
		f = nil
	}
	m.apply(f)
	m.statOK, m.mtime, m.size = true, fi.ModTime(), fi.Size()
}

// reload force-applies freshly persisted state (the generate/revoke handlers
// call it right after writing, sidestepping mtime granularity).
func (m *managedMCPToken) reload(f *MCPTokenFile) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.apply(f)
	m.recordStat()
}

func (m *managedMCPToken) configured() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refresh()
	return m.digest != nil
}

func (m *managedMCPToken) info() (createdAt string, readOnly bool, configured bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refresh()
	return m.createdAt, m.readOnly, m.digest != nil
}

// matches reports whether got is the managed token, comparing raw SHA-256
// digests in constant time (hashing the presented value also equalizes the
// compared length, so nothing about the stored credential's shape leaks).
func (m *managedMCPToken) matches(got string) bool {
	if got == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refresh()
	if m.digest == nil {
		return false
	}
	sum := sha256.Sum256([]byte(got))
	return subtle.ConstantTimeCompare(sum[:], m.digest) == 1
}
