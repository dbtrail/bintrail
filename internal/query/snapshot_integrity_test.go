package query

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/event"
)

// TestFetchSnapshot_validatesIntegrity covers the THIRD local baseline read path
// (#636): `query --include-snapshot` must fail loud on a corrupt baseline rather
// than read its rows as snapshot events. The corrupt file fails the CRC check
// before parquet_scan, so arbitrary bytes suffice (no real Parquet needed).
func TestFetchSnapshot_validatesIntegrity(t *testing.T) {
	snap := t.TempDir()
	p := filepath.Join(snap, "shop", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("baseline parquet bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baselineintegrity.WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("CORRUPTED parquet bytes"), 0o644); err != nil { // bit-rot
		t.Fatal(err)
	}

	et := event.EventSnapshot
	_, err := FetchSnapshot(context.Background(), p, Options{Schema: "shop", Table: "orders", EventType: &et})
	if !errors.Is(err, baselineintegrity.ErrIntegrity) {
		t.Errorf("query --include-snapshot must fail loud on a corrupt baseline, got %v", err)
	}
}
