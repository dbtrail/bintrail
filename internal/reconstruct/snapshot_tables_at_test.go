package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeSnapFixture creates <dir>/<tsdir>/<schema>/<table>.parquet empty files.
func writeSnapFixture(t *testing.T, dir, tsdir string, tables ...string) {
	t.Helper()
	for _, st := range tables {
		p := filepath.Join(dir, tsdir, filepath.Dir(st), filepath.Base(st)+".parquet")
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSnapshotTablesAt pins the anchor selection a point-in-time restore
// depends on: the table list must come from the newest snapshot AT OR BEFORE
// the chosen instant — the snapshot FindBaseline will anchor on — never from
// the newest snapshot overall (whose table set may differ).
func TestSnapshotTablesAt(t *testing.T) {
	dir := t.TempDir()
	writeSnapFixture(t, dir, "2026-06-01T00-00-00Z", "shop/orders")
	writeSnapFixture(t, dir, "2026-06-10T12-00-00Z", "shop/orders", "shop/users")
	ctx := context.Background()

	// Between the two snapshots: the OLDER one's single table.
	got, err := SnapshotTablesAt(ctx, dir, time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "shop.orders" {
		t.Fatalf("mid: got %v, want [shop.orders]", got)
	}

	// After both: the newest snapshot's two tables.
	got, err = SnapshotTablesAt(ctx, dir, time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "shop.orders" || got[1] != "shop.users" {
		t.Fatalf("after: got %v, want both tables", got)
	}

	// Exactly at a snapshot's instant: that snapshot qualifies (at or before).
	got, err = SnapshotTablesAt(ctx, dir, time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("exact: got %v, want both tables", got)
	}

	// Before every snapshot: nothing to fold from.
	got, err = SnapshotTablesAt(ctx, dir, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("before-all: got %v, want nil", got)
	}
}
