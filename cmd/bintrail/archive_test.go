package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/archive"
)

// TestReconcileDryRunErrFlagsDeepUnverified pins #469: a --deep dry-run with
// a zero-drift report but failed footer probes must still exit non-zero, so a
// "green" cron run can't hide objects it was asked to verify.
func TestReconcileDryRunErrFlagsDeepUnverified(t *testing.T) {
	// Zero-drift report: in sync, no actions. Before #469 this returned nil
	// even though some S3 objects could not be deep-verified.
	rep := &archive.Report{InSync: 3}

	if err := reconcileDryRunErr(rep, 0); err != nil {
		t.Fatalf("clean report with no footer failures should exit zero, got: %v", err)
	}

	err := reconcileDryRunErr(rep, 2)
	if err == nil {
		t.Fatal("dry-run must exit non-zero when --deep footer probes failed, got nil")
	}
	if !strings.Contains(err.Error(), "could not be deep-verified") {
		t.Fatalf("error must name the deep-verify failure, got: %v", err)
	}
}

// TestReconcileReportJSONIncludesDeepUnverified pins #469: the footer-probe
// failure count appears in --format json so a cron consumer can see it.
func TestReconcileReportJSONIncludesDeepUnverified(t *testing.T) {
	rep := &archive.Report{InSync: 1}

	var buf bytes.Buffer
	if err := writeReconcileReport(&buf, "json", rep, 4, 0, nil, false, false); err != nil {
		t.Fatalf("writeReconcileReport: %v", err)
	}

	var got reconcileReportJSON
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal report JSON: %v\noutput: %s", err, buf.String())
	}
	if got.DeepUnverified != 4 {
		t.Fatalf("deep_unverified = %d, want 4 (raw: %s)", got.DeepUnverified, buf.String())
	}
	// The field must be present (not omitted) even when zero — stable shape
	// for the cron consumer.
	if !strings.Contains(buf.String(), `"deep_unverified"`) {
		t.Fatalf("JSON output missing deep_unverified field: %s", buf.String())
	}
}

// TestReconcileReportTextSurfacesDeepUnverified pins the text-mode surface of
// #469: a non-zero count produces a WARNING line; zero stays quiet.
func TestReconcileReportTextSurfacesDeepUnverified(t *testing.T) {
	rep := &archive.Report{InSync: 1}

	var loud bytes.Buffer
	if err := writeReconcileReport(&loud, "text", rep, 3, 0, nil, false, false); err != nil {
		t.Fatalf("writeReconcileReport: %v", err)
	}
	if !strings.Contains(loud.String(), "could not be deep-verified") {
		t.Fatalf("text output must warn on deep-unverified files: %s", loud.String())
	}

	var quiet bytes.Buffer
	if err := writeReconcileReport(&quiet, "text", rep, 0, 0, nil, false, false); err != nil {
		t.Fatalf("writeReconcileReport: %v", err)
	}
	if strings.Contains(quiet.String(), "deep-verified") {
		t.Fatalf("healthy run must not mention deep-verify: %s", quiet.String())
	}
}
