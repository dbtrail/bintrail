package query

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestResolveArchiveSourcesRouting pins the per-row routing decisions (#383):
// a local base is preferred only when it actually HOLDS parquet data; an
// existing-but-fileless local tree (post-cleanup: files pruned after S3
// upload, tree left behind) falls back to the S3 copy instead of shadowing
// it; and a registered source is NEVER omitted — when nothing usable
// remains, the unusable local base is returned anyway so the fetch (not
// silence) reports the problem under strict mode (#377).
func TestResolveArchiveSourcesRouting(t *testing.T) {
	dir := t.TempDir()

	// Base with real data — local wins.
	dataBase := filepath.Join(dir, "bintrail_id=with-data")
	if err := os.MkdirAll(filepath.Join(dataBase, "event_date=2026-06-05", "event_hour=10"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataBase, "event_date=2026-06-05", "event_hour=10", "events.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Base that exists but holds no parquet files — the shadow case.
	emptyBase := filepath.Join(dir, "bintrail_id=pruned")
	if err := os.MkdirAll(filepath.Join(emptyBase, "event_date=2026-06-05"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Same fileless shape, but with no S3 columns to fall back to.
	orphanBase := filepath.Join(dir, "bintrail_id=orphan")
	if err := os.MkdirAll(orphanBase, 0o755); err != nil {
		t.Fatal(err)
	}

	// Local path entirely gone (stat fails).
	goneBase := filepath.Join(dir, "bintrail_id=gone") // never created

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cols := []string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}
	mock.ExpectQuery("FROM archive_state").WillReturnRows(sqlmock.NewRows(cols).
		// (1) data present locally + S3 registered → local wins.
		AddRow("with-data", filepath.Join(dataBase, "events.parquet"), "bkt", "events/bintrail_id=with-data/f.parquet").
		// (2) local tree exists but fileless + S3 registered → S3.
		AddRow("pruned", filepath.Join(emptyBase, "events.parquet"), "bkt", "events/bintrail_id=pruned/f.parquet").
		// (3) fileless local, NO S3 → keep the local base (never omit).
		AddRow("orphan", filepath.Join(orphanBase, "events.parquet"), nil, nil).
		// (4) local gone entirely + S3 → S3 (pre-#383 behavior preserved).
		AddRow("gone", filepath.Join(goneBase, "events.parquet"), "bkt", "events/bintrail_id=gone/f.parquet").
		// (5) local gone entirely, NO S3 → keep the local base (NEW: was
		// silently omitted, leaving the planner-claimed coverage with
		// nothing to fail on).
		AddRow("gone-orphan", filepath.Join(dir, "bintrail_id=gone-orphan", "events.parquet"), nil, nil))

	got := ResolveArchiveSources(context.Background(), db)
	want := []string{
		dataBase,
		"s3://bkt/events/bintrail_id=pruned",
		orphanBase,
		"s3://bkt/events/bintrail_id=gone",
		filepath.Join(dir, "bintrail_id=gone-orphan"),
	}
	if len(got) != len(want) {
		t.Fatalf("got %d sources %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sources[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestLocalBaseHasParquet(t *testing.T) {
	dir := t.TempDir()

	// Empty dir → false, no root error.
	if found, rootErr := localBaseHasParquet(dir); found || rootErr != nil {
		t.Errorf("empty dir: found=%v rootErr=%v, want false/nil", found, rootErr)
	}
	// Non-parquet files only → false.
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if found, _ := localBaseHasParquet(dir); found {
		t.Error("dir with only non-parquet files: want false")
	}
	// Parquet directly under base (test-fixture layout) → true.
	if err := os.WriteFile(filepath.Join(dir, "events.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if found, rootErr := localBaseHasParquet(dir); !found || rootErr != nil {
		t.Errorf("parquet directly under base: found=%v rootErr=%v, want true/nil", found, rootErr)
	}

	// Parquet nested in the rotate layout → true.
	nested := t.TempDir()
	sub := filepath.Join(nested, "event_date=2026-06-05", "event_hour=10")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "events.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if found, _ := localBaseHasParquet(nested); !found {
		t.Error("nested parquet: want true")
	}

	// Nonexistent base → false with a root error (callers distinguish
	// "unreadable" from "legitimately pruned" — #383 review).
	if found, rootErr := localBaseHasParquet(filepath.Join(dir, "nope")); found || rootErr == nil {
		t.Errorf("nonexistent base: found=%v rootErr=%v, want false/non-nil", found, rootErr)
	}

	// Unreadable base (no permission bits) → false with a root error.
	// Skipped for root, who bypasses permissions.
	if os.Getuid() != 0 {
		locked := t.TempDir()
		lockedBase := filepath.Join(locked, "bintrail_id=locked")
		if err := os.MkdirAll(lockedBase, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(lockedBase, "events.parquet"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(lockedBase, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(lockedBase, 0o755) })
		found, rootErr := localBaseHasParquet(lockedBase)
		if found || rootErr == nil {
			t.Errorf("unreadable base: found=%v rootErr=%v, want false/non-nil", found, rootErr)
		}
	}
}

func TestExtractBasePath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "local path",
			path: "/data/archives/bintrail_id=abc-123/event_date=2026-01-10/event_hour=14/events.parquet",
			want: "/data/archives/bintrail_id=abc-123",
		},
		{
			name: "s3 key",
			path: "prefix/bintrail_id=abc-123/event_date=2026-01-10/event_hour=14/events.parquet",
			want: "prefix/bintrail_id=abc-123",
		},
		{
			name: "no prefix",
			path: "bintrail_id=abc-123/event_date=2026-01-10/events.parquet",
			want: "bintrail_id=abc-123",
		},
		{
			name: "no trailing slash",
			path: "bintrail_id=abc-123",
			want: "bintrail_id=abc-123",
		},
		{
			name: "no bintrail_id marker",
			path: "/data/archives/event_date=2026-01-10/events.parquet",
			want: "",
		},
		{
			name: "empty path",
			path: "",
			want: "",
		},
		{
			name: "deep nesting",
			path: "a/b/c/bintrail_id=97adaf56-fe9e-4c1b-9794-b042f7faf197/event_date=2026-03-05/event_hour=18/events.parquet",
			want: "a/b/c/bintrail_id=97adaf56-fe9e-4c1b-9794-b042f7faf197",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractBasePath(tt.path)
			if got != tt.want {
				t.Errorf("extractBasePath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}
