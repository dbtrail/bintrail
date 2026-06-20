package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// TestRunBaselineUpload_ordering pins the crash-safe S3 upload ordering (#524
// review): _INCOMPLETE is published before any data, _SUCCESS only after all
// data, and _INCOMPLETE is deleted last. The s3ops seam records the call order
// without a live client, so a future refactor that reorders the steps (the
// original #467-on-upload bug) fails this test instead of silently shipping.
func TestRunBaselineUpload_ordering(t *testing.T) {
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
		filepath.Join(snap, baseline.SuccessMarker),
	} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var calls []string
	ops := s3ops{
		putEmpty:     func(_ context.Context, k string) error { calls = append(calls, "put "+k); return nil },
		uploadFile:   func(_ context.Context, _, k string) error { calls = append(calls, "upload "+k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, k string) error { calls = append(calls, "delete "+k); return nil },
	}

	n, err := runBaselineUpload(context.Background(), outputDir, "p", false, ops)
	if err != nil {
		t.Fatalf("runBaselineUpload: %v", err)
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
		return strings.HasPrefix(c, "put ") && strings.Contains(c, baseline.IncompleteMarker)
	}
	isUploadSuccess := func(c string) bool {
		return strings.HasPrefix(c, "upload ") && strings.Contains(c, baseline.SuccessMarker)
	}
	isData := func(c string) bool {
		return strings.HasPrefix(c, "upload ") && !strings.Contains(c, baseline.SuccessMarker)
	}
	isDelIncomplete := func(c string) bool {
		return strings.HasPrefix(c, "delete ") && strings.Contains(c, baseline.IncompleteMarker)
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

// TestRunBaselineUpload_retrySkipsExisting: under --retry, a data object already
// present in S3 is not re-uploaded.
func TestRunBaselineUpload_retrySkipsExisting(t *testing.T) {
	outputDir := t.TempDir()
	snap := filepath.Join(outputDir, "2025-01-01T00-00-00Z")
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{
		filepath.Join(snap, "shop", "orders.parquet"),
		filepath.Join(snap, baseline.SuccessMarker),
	} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var uploaded []string
	ops := s3ops{
		putEmpty:     func(_ context.Context, _ string) error { return nil },
		uploadFile:   func(_ context.Context, _, k string) error { uploaded = append(uploaded, k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return true, nil }, // everything already present
		deleteObject: func(_ context.Context, _ string) error { return nil },
	}

	// count is "objects processed" (incremented even when skipped); the
	// load-bearing assertion is that uploadFile was never actually invoked.
	if _, err := runBaselineUpload(context.Background(), outputDir, "p", true, ops); err != nil {
		t.Fatalf("runBaselineUpload: %v", err)
	}
	if len(uploaded) != 0 {
		t.Fatalf("--retry re-uploaded existing objects %v; want none", uploaded)
	}
}
