package console

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	yaml "go.yaml.in/yaml/v2"
)

// registryVersion is the registry file schema version this binary writes and
// fully understands. A file with a HIGHER version loads read-only (list/get
// still work) and every mutating operation is refused — otherwise an older
// binary could rewrite a newer file through its narrower schema and lose
// non-additive changes. Purely additive fields don't need a version bump: they
// round-trip through ServerEntry.Extra.
const registryVersion = 1

// bootServerID is the reserved id of the ephemeral entry seeded from the
// command line (--index-dsn / `up`'s stream DSN). It is never written to the
// registry file and can never be edited or deleted over the API.
const bootServerID = "default"

var (
	// ErrDuplicateName rejects two registry entries sharing a name — the name
	// is the operator-facing label in the UI switcher, so it must be unique.
	ErrDuplicateName = errors.New("a server with this name already exists")
	// ErrUnknownServer is returned for an id with no registry entry.
	ErrUnknownServer = errors.New("unknown server id")
	// ErrRegistryReadOnly is returned for mutating operations when the on-disk
	// file was written by a newer bintrail (version > registryVersion).
	ErrRegistryReadOnly = errors.New("server registry was written by a newer bintrail; upgrade bintrail to edit it")
)

// ServerEntry is one named index connection in the registry. DSN holds the
// full secret (including the password) and is NEVER serialized into an HTTP
// response — see serverDTO in servers_api.go for the masked wire view.
type ServerEntry struct {
	// ID is a server-generated stable 8-byte hex key; it survives renames and
	// is what the browser sends in the X-Bintrail-Server header.
	ID string `yaml:"id"`
	// Name is the mutable operator-facing label, unique across the registry.
	Name string `yaml:"name"`
	// DSN is the full index DSN, password included — a secret at rest (the
	// file is 0600, same class as shim.yaml's mysql_password).
	DSN         string `yaml:"index_dsn"`
	BaselineDir string `yaml:"baseline_dir,omitempty"`
	BaselineS3  string `yaml:"baseline_s3,omitempty"`
	NoArchive   bool   `yaml:"no_archive,omitempty"`

	// ── Control-plane fields (phase 2 of the approved blueprint) ──
	// These configure MONITORING of a source MySQL; the supervisor in
	// `bintrail-console watch` consumes them (phase 3). On binaries that
	// predate them they round-trip untouched through Extra.

	// SourceDSN is the source MySQL to monitor — replication credentials,
	// a secret exactly like DSN, never serialized to any HTTP response.
	// Empty = a view-only entry (no monitoring configured).
	SourceDSN string `yaml:"source_dsn,omitempty"`
	// SourceServerID overrides the auto-derived replica server id (0 = derive
	// from the source DSN, the same rule as `bintrail up`).
	SourceServerID uint32 `yaml:"source_server_id,omitempty"`
	// Schemas is the optional comma-separated schema filter for monitoring.
	Schemas string `yaml:"schemas,omitempty"`

	// ── Source family (#1019) ──
	// Flavor selects the capture engine: "" or "mysql" (MySQL), "mariadb"
	// (MySQL DSN, MariaDB GTID at stream), "postgres" (pgstreamrun). Empty →
	// mysql keeps every pre-#1019 entry working (mirrors an empty SSLMode).
	// This is the generic "which source family" field #623 (MariaDB) also
	// needs — one implementation, not two. Accessed via SourceFlavor().
	Flavor string `yaml:"flavor,omitempty"`
	// SourceSlot / SourcePublication configure PostgreSQL logical replication
	// (postgres flavor only): the operator-created publication and the
	// replication slot the capturer streams from. Empty for MySQL/MariaDB.
	SourceSlot        string `yaml:"source_slot,omitempty"`
	SourcePublication string `yaml:"source_publication,omitempty"`

	// ── Source TLS (#879) ──
	// SSLMode/SSLCA/SSLCert/SSLKey configure the TLS the supervisor uses when
	// connecting to the SOURCE MySQL. Empty SSLMode = the daemon default
	// ("preferred"), preserving pre-#879 behavior; SSLCA/SSLCert/SSLKey are file
	// paths on the daemon host (verify-ca / mutual TLS), not secrets like the
	// DSNs. On binaries that predate these they round-trip untouched via Extra.
	SSLMode string `yaml:"ssl_mode,omitempty"`
	SSLCA   string `yaml:"ssl_ca,omitempty"`
	SSLCert string `yaml:"ssl_cert,omitempty"`
	SSLKey  string `yaml:"ssl_key,omitempty"`
	// ArchiveS3 is the S3 destination (s3://bucket/prefix/) the daemon's
	// built-in rotation uploads this source's rotated Parquet partitions to
	// BEFORE dropping them — so the forensic record survives retention and
	// stays queryable (the console auto-discovers it). Empty = drop-only.
	// Region/credentials come from the ambient AWS chain (env / ~/.aws / IAM
	// role); a local staging dir is used transiently. Non-secret (a bucket
	// URL): unlike DSNs it is serialized to the masked HTTP responses.
	ArchiveS3 string `yaml:"archive_s3,omitempty"`
	// MonitorDesired records the operator's intent to monitor this source.
	// The supervisor reconciles running streams against it at boot and on
	// every edit; nothing reads it until phase 3.
	MonitorDesired bool `yaml:"monitor_desired,omitempty"`

	// Extra is the forward-compat catch-all: unknown fields written by a NEWER
	// bintrail (e.g. the phase-2 control plane's source_dsn / server_id /
	// monitor_state) land here on load and re-emit verbatim on save, so an
	// older binary editing the file never drops them. yaml:",inline" requires
	// map[string]any specifically; non-strict Unmarshal alone would only
	// tolerate unknown fields on read — a re-marshal would lose them.
	Extra map[string]any `yaml:",inline"`
}

// RotationConfig is the daemon-global built-in-rotation policy, editable from
// the console UI. It is stored once in the registry envelope, NOT per-server:
// the rotation loop is a single shared ticker, so retain/interval/add-future
// necessarily apply to every index the daemon rotates. Absent (nil) = the
// daemon's --rotate-* flags / BINTRAIL_ROTATE_* env stay in force. The fields
// hold the operator-typed strings (e.g. "30d", "1h") so they round-trip
// exactly and the engine parses them with the same grammar as the flags.
type RotationConfig struct {
	Retain    string `yaml:"retain"`
	Interval  string `yaml:"interval"`
	AddFuture int    `yaml:"add_future"`
}

// registryFile is the versioned on-disk envelope.
type registryFile struct {
	Version int `yaml:"version"`
	// Rotation is the optional global rotation override (omitted when the
	// daemon flags/env are in force). Additive at registryVersion 1 (no bump):
	// binaries from this release on preserve it (this field, plus the Extra
	// catch-all below for any future envelope key). A downgrade to a
	// PRE-rotation binary that re-saves the file would drop it — the version
	// gate, not round-tripping, is the cross-version safety net, since that
	// older binary has neither this field nor the inline catch-all.
	Rotation *RotationConfig `yaml:"rotation,omitempty"`
	Servers  []ServerEntry   `yaml:"servers"`
	// Extra preserves any FUTURE envelope-level key a (future) older binary
	// doesn't model, exactly as ServerEntry.Extra does at the entry level — so
	// the next additive envelope field is downgrade-safe from here on. (It does
	// not retroactively help binaries released before it.)
	Extra map[string]any `yaml:",inline"`
}

// Registry is the console's named-server store: a local YAML file, the ONLY
// thing the console ever writes. All mutations rewrite the file atomically
// (temp file + fsync + rename) under a mutex — this is bintrail's first
// programmatically-mutated config file, so a plain os.WriteFile (which can
// interleave concurrent writers) is not enough.
type Registry struct {
	path string // "" = in-memory only (unit tests); Save skips the disk
	mu   sync.Mutex
	file registryFile
	// readOnly is set when the on-disk version is newer than this binary
	// understands; see ErrRegistryReadOnly.
	readOnly bool
}

// DefaultRegistryPath returns ~/.config/bintrail/console-servers.yaml,
// mirroring generate-key's defaultKeyPath fallback behavior.
func DefaultRegistryPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".config", "bintrail", "console-servers.yaml")
	}
	return filepath.Join(home, ".config", "bintrail", "console-servers.yaml")
}

// LoadRegistry reads the registry file at path. A missing or empty file is an
// empty registry, not an error. The parse is deliberately NON-strict (unlike
// shim.yaml's UnmarshalStrict): an older binary must tolerate top-level fields
// a newer one added. path == "" creates an in-memory registry that never
// touches disk — for unit tests and callers without persistence.
func LoadRegistry(path string) (*Registry, error) {
	r := &Registry{path: path, file: registryFile{Version: registryVersion}}
	if path == "" {
		return r, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read server registry %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &r.file); err != nil {
		return nil, fmt.Errorf("parse server registry %s: %w", path, err)
	}
	if r.file.Version > registryVersion {
		r.readOnly = true
	}
	if r.file.Version == 0 {
		// Unset/zero version: a hand-written file; normalize on the next save.
		r.file.Version = registryVersion
	}
	return r, nil
}

// List returns a copy of the entries, in file order. The copy is shallow:
// each entry's Extra map is shared with the registry — treat returned entries
// as read-only (nothing in the console mutates Extra off a copy today).
func (r *Registry) List() []ServerEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ServerEntry, len(r.file.Servers))
	copy(out, r.file.Servers)
	return out
}

// Get returns the entry with the given id. Like List, the entry's Extra map
// is shared with the registry — treat it as read-only.
func (r *Registry) Get(id string) (ServerEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.file.Servers {
		if e.ID == id {
			return e, true
		}
	}
	return ServerEntry{}, false
}

// Len reports the number of registry entries.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.file.Servers)
}

// ReadOnly reports whether mutating operations are refused (newer-version file).
func (r *Registry) ReadOnly() bool { return r.readOnly }

// Rotation returns the saved global rotation policy, or false when none is set
// (the daemon's --rotate-* flags/env are in force). The rotation loop's
// settings provider reads this every cycle, so an override applies live.
func (r *Registry) Rotation() (RotationConfig, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file.Rotation == nil {
		return RotationConfig{}, false
	}
	return *r.file.Rotation, true
}

// SetRotation persists the global rotation policy. Like every registry
// mutation it rewrites the file atomically and is refused on a newer-version
// (read-only) file. The caller validates the field grammar.
func (r *Registry) SetRotation(rc RotationConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ErrRegistryReadOnly
	}
	prev := r.file.Rotation
	r.file.Rotation = &rc
	if err := r.save(); err != nil {
		r.file.Rotation = prev // roll back
		return err
	}
	return nil
}

// Add validates, mints an id, appends, and persists the entry. The returned
// entry carries the generated ID.
func (r *Registry) Add(e ServerEntry) (ServerEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ServerEntry{}, ErrRegistryReadOnly
	}
	if err := r.checkName(e.Name, ""); err != nil {
		return ServerEntry{}, err
	}
	id, err := genServerID()
	if err != nil {
		return ServerEntry{}, fmt.Errorf("generate server id: %w", err)
	}
	e.ID = id
	r.file.Servers = append(r.file.Servers, e)
	if err := r.save(); err != nil {
		r.file.Servers = r.file.Servers[:len(r.file.Servers)-1] // roll back
		return ServerEntry{}, err
	}
	return e, nil
}

// Update replaces the entry with e.ID and persists. The caller is responsible
// for keep-password merging — the registry stores exactly what it is given.
// Unknown phase-2 fields survive: the stored entry's Extra is carried over
// unless the caller supplied its own.
func (r *Registry) Update(e ServerEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ErrRegistryReadOnly
	}
	if err := r.checkName(e.Name, e.ID); err != nil {
		return err
	}
	for i, old := range r.file.Servers {
		if old.ID != e.ID {
			continue
		}
		if e.Extra == nil {
			e.Extra = old.Extra // preserve forward-compat fields across edits
		}
		r.file.Servers[i] = e
		if err := r.save(); err != nil {
			r.file.Servers[i] = old // roll back
			return err
		}
		return nil
	}
	return ErrUnknownServer
}

// Delete removes the entry with the given id and persists.
func (r *Registry) Delete(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ErrRegistryReadOnly
	}
	for i, old := range r.file.Servers {
		if old.ID != id {
			continue
		}
		r.file.Servers = append(r.file.Servers[:i], r.file.Servers[i+1:]...)
		if err := r.save(); err != nil {
			// Roll back: re-insert at the original position.
			r.file.Servers = append(r.file.Servers[:i], append([]ServerEntry{old}, r.file.Servers[i:]...)...)
			return err
		}
		return nil
	}
	return ErrUnknownServer
}

// checkName enforces non-empty unique names; selfID exempts the entry being
// updated. Callers hold r.mu. The boot entry's reserved id doubles as a
// reserved name so the switcher never shows two entries labeled "default".
func (r *Registry) checkName(name, selfID string) error {
	if name == "" {
		return errors.New("server name is required")
	}
	if name == bootServerID {
		return fmt.Errorf("%q is reserved for the command-line server", bootServerID)
	}
	for _, e := range r.file.Servers {
		if e.Name == name && e.ID != selfID {
			return ErrDuplicateName
		}
	}
	return nil
}

// save writes the registry atomically: marshal → temp file in the same
// directory → fsync → rename. Callers hold r.mu. The file is 0600 and its
// directory 0700 — it holds DSN passwords (same class as shim.yaml/dump.key).
func (r *Registry) save() error {
	if r.path == "" {
		return nil // in-memory registry
	}
	data, err := yaml.Marshal(&r.file)
	if err != nil {
		return fmt.Errorf("marshal server registry: %w", err)
	}
	dir := filepath.Dir(r.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create registry directory %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".console-servers-*.yaml")
	if err != nil {
		return fmt.Errorf("create temp registry file: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp registry file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write server registry: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync server registry: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp registry file: %w", err)
	}
	if err := os.Rename(tmp.Name(), r.path); err != nil {
		return fmt.Errorf("replace server registry %s: %w", r.path, err)
	}
	return nil
}

// genServerID returns a random 8-byte hex id (16 chars) — stable across
// renames, unguessable, and short enough to read in a YAML file.
func genServerID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
