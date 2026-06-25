package baseline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestUploadWithOps_ordering pins the crash-safe S3 upload ordering (#524
// review): _INCOMPLETE is published before any data, _SUCCESS only after all
// data, and _INCOMPLETE is deleted last. The s3UploadOps seam records the call
// order without a live client, so a future refactor that reorders the steps (the
// original #467-on-upload bug) fails this test instead of silently shipping.
func TestUploadWithOps_ordering(t *testing.T) {
	outputDir := t.TempDir()
	snap := filepath.Join(outputDir, "2025-01-01T00-00-00Z")
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Two data files plus the local _SUCCESS marker (Run already removed the
	// local _INCOMPLETE before upload).
	for _, f := range []string{
		filepath.Join(snap, "shop", "orders.parquet"),
		filepath.Join(snap, "shop", "customers.parquet"),
		filepath.Join(snap, SuccessMarker),
	} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var calls []string
	ops := s3UploadOps{
		putEmpty:     func(_ context.Context, k string) error { calls = append(calls, "put "+k); return nil },
		uploadFile:   func(_ context.Context, _, k string) error { calls = append(calls, "upload "+k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, k string) error { calls = append(calls, "delete "+k); return nil },
	}

	n, err := uploadWithOps(context.Background(), outputDir, "p", false, ops)
	if err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	if n != 3 { // 2 data files + 1 _SUCCESS
		t.Fatalf("uploaded %d objects, want 3", n)
	}

	first := func(pred func(string) bool) int {
		for i, c := range calls {
			if pred(c) {
				return i
			}
		}
		return -1
	}
	last := func(pred func(string) bool) int {
		idx := -1
		for i, c := range calls {
			if pred(c) {
				idx = i
			}
		}
		return idx
	}
	isPutIncomplete := func(c string) bool {
		return strings.HasPrefix(c, "put ") && strings.Contains(c, IncompleteMarker)
	}
	isUploadSuccess := func(c string) bool {
		return strings.HasPrefix(c, "upload ") && strings.Contains(c, SuccessMarker)
	}
	isData := func(c string) bool {
		return strings.HasPrefix(c, "upload ") && !strings.Contains(c, SuccessMarker)
	}
	isDelIncomplete := func(c string) bool {
		return strings.HasPrefix(c, "delete ") && strings.Contains(c, IncompleteMarker)
	}

	putInc := first(isPutIncomplete)
	firstData := first(isData)
	lastData := last(isData)
	upSuccess := first(isUploadSuccess)
	delInc := first(isDelIncomplete)

	if putInc < 0 || firstData < 0 || upSuccess < 0 || delInc < 0 {
		t.Fatalf("missing expected calls in %v", calls)
	}
	if putInc > firstData {
		t.Errorf("_INCOMPLETE put (%d) must precede first data upload (%d): %v", putInc, firstData, calls)
	}
	if upSuccess < lastData {
		t.Errorf("_SUCCESS upload (%d) must follow all data uploads (last %d): %v", upSuccess, lastData, calls)
	}
	if delInc < upSuccess {
		t.Errorf("_INCOMPLETE delete (%d) must follow _SUCCESS upload (%d): %v", delInc, upSuccess, calls)
	}
}

// TestUploadWithOps_retrySkipsExisting: under --retry, a data object already
// present in S3 is not re-uploaded.
func TestUploadWithOps_retrySkipsExisting(t *testing.T) {
	outputDir := t.TempDir()
	snap := filepath.Join(outputDir, "2025-01-01T00-00-00Z")
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{
		filepath.Join(snap, "shop", "orders.parquet"),
		filepath.Join(snap, SuccessMarker),
	} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var uploaded []string
	ops := s3UploadOps{
		putEmpty:     func(_ context.Context, _ string) error { return nil },
		uploadFile:   func(_ context.Context, _, k string) error { uploaded = append(uploaded, k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return true, nil }, // everything already present
		deleteObject: func(_ context.Context, _ string) error { return nil },
	}

	// count is "objects processed" (incremented even when skipped); the
	// load-bearing assertion is that uploadFile was never actually invoked.
	if _, err := uploadWithOps(context.Background(), outputDir, "p", true, ops); err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	if len(uploaded) != 0 {
		t.Fatalf("--retry re-uploaded existing objects %v; want none", uploaded)
	}
}

// TestSnapshotDirsWithSuccess verifies the helper that drives the S3 upload's
// _INCOMPLETE-first ordering: it returns exactly the immediate child snapshot
// directories carrying a local _SUCCESS marker (completed snapshots), ignoring
// loose files, marker-less dirs, and nested dirs.
func TestSnapshotDirsWithSuccess(t *testing.T) {
	out := t.TempDir()

	mkSnap := func(name string, success bool) {
		t.Helper()
		dir := filepath.Join(out, name, "shop")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "orders.parquet"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		if success {
			if err := os.WriteFile(filepath.Join(out, name, SuccessMarker), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	mkSnap("2026-01-01T00-00-00Z", true)  // complete → included
	mkSnap("2026-02-01T00-00-00Z", true)  // complete → included
	mkSnap("2026-03-01T00-00-00Z", false) // no _SUCCESS → excluded
	// A loose file at the top level must not be mistaken for a snapshot dir.
	if err := os.WriteFile(filepath.Join(out, "stray.txt"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := snapshotDirsWithSuccess(out)
	if err != nil {
		t.Fatalf("snapshotDirsWithSuccess: %v", err)
	}
	want := map[string]bool{
		filepath.Join(out, "2026-01-01T00-00-00Z"): true,
		filepath.Join(out, "2026-02-01T00-00-00Z"): true,
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want the two _SUCCESS-marked snapshots %v", got, want)
	}
	for _, d := range got {
		if !want[d] {
			t.Errorf("unexpected snapshot dir %q (only _SUCCESS-marked dirs should be returned)", d)
		}
	}
}

// TestSnapshotDirsWithSuccess_missingDir verifies the helper surfaces a read
// error rather than silently returning an empty list (which would skip the
// _INCOMPLETE-first publish entirely).
func TestSnapshotDirsWithSuccess_missingDir(t *testing.T) {
	if _, err := snapshotDirsWithSuccess("/nonexistent/path-does-not-exist"); err == nil {
		t.Fatal("expected error for nonexistent output directory, got nil")
	}
}
