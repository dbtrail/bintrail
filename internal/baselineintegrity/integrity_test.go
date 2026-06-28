package baselineintegrity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func writeFileT(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestCRC32CFile_detectsFlippedByte: a single flipped byte changes the digest —
// the property the whole at-rest check rests on.
func TestCRC32CFile_detectsFlippedByte(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.parquet")
	writeFileT(t, p, []byte("hello world parquet bytes"))
	a, err := CRC32CFile(p)
	if err != nil {
		t.Fatal(err)
	}
	writeFileT(t, p, []byte("hello world parquet byteX")) // flip the last byte
	b, err := CRC32CFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Errorf("CRC must change after a flipped byte: %s == %s", a, b)
	}
}

// TestManifest_roundtrip: WriteManifest hashes every .parquet under the snapshot
// directory (and nothing else); LoadManifest reads it back.
func TestManifest_roundtrip(t *testing.T) {
	snap := t.TempDir()
	writeFileT(t, filepath.Join(snap, "db", "t1.parquet"), []byte("aaaa"))
	writeFileT(t, filepath.Join(snap, "db", "t2.parquet"), []byte("bbbb"))
	writeFileT(t, filepath.Join(snap, "db", "notes.txt"), []byte("ignore me"))

	if err := WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	m, ok, err := LoadManifest(snap)
	if err != nil || !ok {
		t.Fatalf("LoadManifest: ok=%v err=%v", ok, err)
	}
	if len(m.Files) != 2 {
		t.Errorf("want 2 parquet files, got %d: %v", len(m.Files), m.Files)
	}
	if _, ok := m.Files["db/t1.parquet"]; !ok {
		t.Errorf("manifest missing db/t1.parquet: %v", m.Files)
	}
	if _, ok := m.Files["db/notes.txt"]; ok {
		t.Errorf("manifest must cover only .parquet files: %v", m.Files)
	}
}

// TestValidateLocalFile covers the verified, corrupt, no-manifest, and
// not-listed outcomes.
func TestValidateLocalFile(t *testing.T) {
	snap := t.TempDir()
	good := filepath.Join(snap, "db", "orders.parquet")
	writeFileT(t, good, []byte("the original baseline bytes"))
	if err := WriteManifest(snap); err != nil {
		t.Fatal(err)
	}

	if err := ValidateLocalFile(good); err != nil {
		t.Errorf("clean file must validate, got %v", err)
	}

	writeFileT(t, good, []byte("the corrupted baseline bytes!")) // bit-rot
	if err := ValidateLocalFile(good); !errors.Is(err, ErrIntegrity) {
		t.Errorf("corrupt file must fail loud with ErrIntegrity, got %v", err)
	}

	// Legacy snapshot (no manifest) → not verifiable, not a failure.
	legacy := filepath.Join(t.TempDir(), "db", "orders.parquet")
	writeFileT(t, legacy, []byte("legacy, no manifest"))
	if err := ValidateLocalFile(legacy); err != nil {
		t.Errorf("a legacy snapshot with no manifest must be a no-op, got %v", err)
	}

	// A file present but absent from the manifest → skip, not a false positive.
	extra := filepath.Join(snap, "db", "added_later.parquet")
	writeFileT(t, extra, []byte("not listed in the manifest"))
	if err := ValidateLocalFile(extra); err != nil {
		t.Errorf("a file absent from the manifest must skip, got %v", err)
	}
}

// TestValidateLocalFile_corruptManifest: a rotted / unparseable _MANIFEST must
// degrade to a SKIP (warn), not hard-fail — "cannot verify" is not "data
// corrupt", and a sidecar bit-flip must never brick recovery of intact data.
func TestValidateLocalFile_corruptManifest(t *testing.T) {
	snap := t.TempDir()
	good := filepath.Join(snap, "db", "orders.parquet")
	writeFileT(t, good, []byte("perfectly intact baseline data"))
	if err := WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	// Rot the sidecar into invalid JSON.
	writeFileT(t, filepath.Join(snap, ManifestName), []byte("{not valid json at all"))

	if err := ValidateLocalFile(good); err != nil {
		t.Errorf("a corrupt manifest over intact data must degrade to skip, not fail; got %v", err)
	}
}

// TestValidateLocalFile_unrecognizedVersion: a manifest whose version/algo this
// binary doesn't recognize (a future format) degrades to a SKIP, never
// ErrIntegrity — so an old binary can't brick recovery of a newer-but-intact
// baseline by comparing its crc32c against a foreign digest.
func TestValidateLocalFile_unrecognizedVersion(t *testing.T) {
	snap := t.TempDir()
	good := filepath.Join(snap, "db", "orders.parquet")
	writeFileT(t, good, []byte("intact baseline data"))
	// A v99/"future-hash" manifest with a deliberately WRONG digest for the file:
	// without the version gate this would be an ErrIntegrity mismatch.
	manifest := `{"version":99,"algo":"future-hash","files":{"db/orders.parquet":"deadbeef"}}`
	writeFileT(t, filepath.Join(snap, ManifestName), []byte(manifest))

	if err := ValidateLocalFile(good); err != nil {
		t.Errorf("an unrecognized manifest version/algo must degrade to skip, got %v", err)
	}
}
