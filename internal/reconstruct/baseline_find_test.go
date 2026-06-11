package reconstruct

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFindFixture(t *testing.T, dir string, parts ...string) {
	t.Helper()
	p := filepath.Join(append([]string{dir}, parts...)...)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

// captureWarns routes the default slog logger into a buffer for the test.
// Process-global (slog.SetDefault) — do not t.Parallel() tests using it.
func captureWarns(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// TestFindBaselineLocal_staleSnapshotWarns pins the #461 staleness signal:
// when the requested table is absent from the newest eligible snapshot and an
// older one is used, findBaselineLocal must warn — and must NOT warn when the
// chosen snapshot IS the newest. It also pins the selection invariants the
// warn depends on: newest-eligible wins, and snapshots after `at` are
// excluded from both selection and the newest-snapshot comparison.
func TestFindBaselineLocal_staleSnapshotWarns(t *testing.T) {
	dir := t.TempDir()
	writeFindFixture(t, dir, "2026-01-01T00-00-00Z", "shop", "orders.parquet")
	writeFindFixture(t, dir, "2026-02-01T00-00-00Z", "shop", "users.parquet")  // newest eligible, NO orders
	writeFindFixture(t, dir, "2026-03-01T00-00-00Z", "shop", "orders.parquet") // after `at` — must be invisible

	at := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)

	buf := captureWarns(t)
	path, ts, err := findBaselineLocal(dir, "shop", "orders", at)
	if err != nil {
		t.Fatal(err)
	}
	wantTS := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !ts.Equal(wantTS) || filepath.Base(filepath.Dir(filepath.Dir(path))) != "2026-01-01T00-00-00Z" {
		t.Fatalf("selected %s (%s), want the 2026-01-01 snapshot (newer eligible snapshot lacks the table; 2026-03-01 is after `at`)", path, ts)
	}
	if !bytes.Contains(buf.Bytes(), []byte("absent from the newest snapshot")) {
		t.Fatalf("want the staleness warn, got log output: %q", buf.String())
	}

	// Chosen snapshot IS the newest eligible → no warn.
	buf2 := captureWarns(t)
	at2 := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	if _, ts2, err := findBaselineLocal(dir, "shop", "orders", at2); err != nil || !ts2.Equal(wantTS) {
		t.Fatalf("selection at %s: ts=%s err=%v", at2, ts2, err)
	}
	if bytes.Contains(buf2.Bytes(), []byte("absent from the newest snapshot")) {
		t.Fatalf("spurious staleness warn when the chosen snapshot is the newest eligible: %q", buf2.String())
	}
}
