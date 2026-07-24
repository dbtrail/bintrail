package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// TestMaterializeBaselineLocal_validatesIntegrity pins the #636 read hook: the
// local baseline choke point fails LOUD on a corrupt file (before any reader
// trusts it) and passes a clean one through. No DuckDB needed — the local branch
// validates and returns. crc32c is over the raw bytes, so an arbitrary-bytes
// fixture is sufficient (the file need not be valid Parquet).
func TestMaterializeBaselineLocal_validatesIntegrity(t *testing.T) {
	snap := t.TempDir()
	p := filepath.Join(snap, "db", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("baseline parquet bytes — pretend this is a real file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baselineintegrity.WriteManifest(snap); err != nil {
		t.Fatal(err)
	}

	// Clean → passes through unchanged.
	got, cleanup, err := materializeBaselineLocal(context.Background(), p, duckdbutil.Tuning{})
	if err != nil {
		t.Fatalf("clean baseline must materialize, got %v", err)
	}
	cleanup()
	if got != p {
		t.Errorf("a local path should pass through, got %q want %q", got, p)
	}

	// Corrupt the bytes → fail loud with ErrIntegrity, before DuckDB ever sees it.
	if err := os.WriteFile(p, []byte("CORRUPTED bytes — pretend bit-rot flipped these here"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := materializeBaselineLocal(context.Background(), p, duckdbutil.Tuning{}); !errors.Is(err, baselineintegrity.ErrIntegrity) {
		t.Errorf("a corrupt baseline must fail loud with ErrIntegrity, got %v", err)
	}

	// A legacy snapshot (no manifest) must still materialize (not verifiable ≠ fail).
	legacy := filepath.Join(t.TempDir(), "db", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacy, []byte("legacy baseline, no manifest"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := materializeBaselineLocal(context.Background(), legacy, duckdbutil.Tuning{}); err != nil {
		t.Errorf("a legacy baseline with no manifest must still materialize, got %v", err)
	}
}
