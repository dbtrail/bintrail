package baseline

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// At-rest integrity (#636).
//
// Neither read path validates a baseline Parquet: DuckDB's parquet_scan and
// parquet-go's OpenFile both trust the bytes, so a file silently corrupted on
// disk or in S3 (bit-rot, a partial write) is read back as truth. The _MANIFEST
// sidecar closes that: it records a CRC-32C over each table's Parquet file BYTES
// at write time, and the read path re-hashes and compares, failing loud on a
// mismatch instead of returning garbage rows.
//
// Scope is bit-rot / partial-write, NOT deliberate tampering: an attacker who
// rewrites a Parquet file can also rewrite this manifest. True tamper-evidence
// needs signing or an external registry — a separate, larger effort, deliberately
// out of scope here. The manifest is a SEPARATE file from the data it covers, so
// corruption of one does not mask the other, and CRC-32C (Castagnoli, hardware-
// accelerated) detects any flipped byte with no parquet-library dependency.
//
// The content_digest (#633) is a different thing: a source-fidelity fingerprint
// of the ROWS, embedded in the Parquet metadata; it neither covers the file
// encoding nor survives being rewritten. This manifest covers the bytes.

// ManifestName is the per-snapshot integrity sidecar, written under the snapshot
// directory alongside the _SUCCESS marker.
const ManifestName = "_MANIFEST"

// manifestVersion is the on-disk schema version of the manifest JSON.
const manifestVersion = 1

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// ErrIntegrity is returned when a baseline file's CRC-32C does not match the
// manifest — the file is corrupt (bit-rot or a partial write). Callers fail loud.
var ErrIntegrity = errors.New("baseline integrity check failed (file corrupt)")

// Manifest is the integrity sidecar: each Parquet file's path (relative to the
// snapshot directory, forward-slashed so it is OS- and S3-portable) → its
// CRC-32C hex digest.
type Manifest struct {
	Version int               `json:"version"`
	Algo    string            `json:"algo"` // always "crc32c"
	Files   map[string]string `json:"files"`
}

// CRC32CFile streams path through CRC-32C (Castagnoli) and returns the 8-char
// lowercase hex digest.
func CRC32CFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := crc32.New(crc32cTable)
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%08x", h.Sum32()), nil
}

// WriteManifest hashes every .parquet file under snapshotDir and writes the
// integrity manifest. It is called on full baseline success, before the _SUCCESS
// marker, so a snapshot that has _SUCCESS also has its manifest.
func WriteManifest(snapshotDir string) error {
	m := Manifest{Version: manifestVersion, Algo: "crc32c", Files: map[string]string{}}
	err := filepath.WalkDir(snapshotDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() || !strings.HasSuffix(d.Name(), ".parquet") {
			return walkErr
		}
		crc, err := CRC32CFile(p)
		if err != nil {
			return fmt.Errorf("crc32c %s: %w", p, err)
		}
		rel, err := filepath.Rel(snapshotDir, p)
		if err != nil {
			return err
		}
		m.Files[filepath.ToSlash(rel)] = crc
		return nil
	})
	if err != nil {
		return err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(snapshotDir, ManifestName), b, 0o644)
}

// LoadManifest reads the integrity manifest from snapshotDir. ok=false with a nil
// error means the manifest is ABSENT — a legacy snapshot written before #636,
// which reads as "integrity not verified" rather than failing.
func LoadManifest(snapshotDir string) (m *Manifest, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(snapshotDir, ManifestName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var parsed Manifest
	if err := json.Unmarshal(b, &parsed); err != nil {
		return nil, false, fmt.Errorf("parse %s: %w", ManifestName, err)
	}
	return &parsed, true, nil
}

// ValidateLocalFile checks a local baseline Parquet file against its snapshot's
// integrity manifest. The snapshot directory is the file's grandparent
// (<snapshot>/<db>/<table>.parquet). It returns:
//   - nil (skip) when there is no manifest (a legacy snapshot, or a path that is
//     not inside a snapshot directory — e.g. a materialized S3 temp file or a
//     test fixture): unverifiable is not a failure.
//   - ErrIntegrity (wrapped) when the file's CRC-32C does not match the manifest.
//
// Fail-loud on mismatch is the whole point: the Parquet readers validate nothing.
func ValidateLocalFile(path string) error {
	snapshotDir := filepath.Dir(filepath.Dir(path))
	m, ok, err := LoadManifest(snapshotDir)
	if err != nil {
		return err
	}
	if !ok {
		return nil // no manifest — not verifiable, not a failure (legacy/temp/test)
	}
	rel, err := filepath.Rel(snapshotDir, path)
	if err != nil {
		return nil // path not under the snapshot dir — unexpected layout, skip
	}
	want, listed := m.Files[filepath.ToSlash(rel)]
	if !listed {
		return nil // file not in the manifest — skip rather than false-positive
	}
	got, err := CRC32CFile(path)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%w: %s (crc32c %s, manifest %s)", ErrIntegrity, path, got, want)
	}
	return nil
}
