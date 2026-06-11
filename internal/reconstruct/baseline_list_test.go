package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeListFixture(t *testing.T, dir string, parts ...string) {
	t.Helper()
	p := filepath.Join(append([]string{dir}, parts...)...)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestListBaselinesLocal: path-derived listing, newest snapshot first, stable
// schema/table order within a snapshot, layout strangers skipped.
func TestListBaselinesLocal(t *testing.T) {
	dir := t.TempDir()
	writeListFixture(t, dir, "2026-06-01T00-00-00Z", "shop", "orders.parquet")
	writeListFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "users.parquet")
	writeListFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeListFixture(t, dir, "2026-06-10T12-00-00Z", "billing", "invoices.parquet")
	writeListFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "notes.txt")                 // not parquet
	writeListFixture(t, dir, "not-a-timestamp", "shop", "junk.parquet")                   // bad snapshot dir
	if err := os.WriteFile(filepath.Join(dir, "stray.parquet"), nil, 0o644); err != nil { // file at top level
		t.Fatal(err)
	}

	files, err := ListBaselines(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 4 {
		t.Fatalf("len = %d (%+v), want 4", len(files), files)
	}

	wantNewest := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	wantOldest := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	wantOrder := []struct {
		ts     time.Time
		schema string
		table  string
	}{
		{wantNewest, "billing", "invoices"},
		{wantNewest, "shop", "orders"},
		{wantNewest, "shop", "users"},
		{wantOldest, "shop", "orders"},
	}
	for i, w := range wantOrder {
		f := files[i]
		if !f.SnapshotTime.Equal(w.ts) || f.Schema != w.schema || f.Table != w.table {
			t.Fatalf("files[%d] = %s %s.%s, want %s %s.%s", i,
				f.SnapshotTime.Format(time.RFC3339), f.Schema, f.Table,
				w.ts.Format(time.RFC3339), w.schema, w.table)
		}
		if _, err := os.Stat(f.Path); err != nil {
			t.Fatalf("files[%d].Path does not exist: %v", i, err)
		}
	}
}

func TestListBaselinesLocal_missingDir(t *testing.T) {
	if _, err := ListBaselines(context.Background(), filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("want an error for a missing baseline directory")
	}
}
