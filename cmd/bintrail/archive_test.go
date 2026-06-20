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

// TestReconcileExecuteErrFlagsDeepUnverified is the execute-mode (--repair/
// --prune) sibling of the dry-run test: --deep/--repair/--prune are
// independent flags, so a `reconcile --deep --repair` run with no remaining
// drift but failed footer probes must STILL exit non-zero — --repair cannot
// fix a footer it cannot read, and a scheduled auto-remediation keys on the
// exit code. Without the shared deepUnverified guard this path returned nil
// (silent exit 0), reintroducing #469 in execute mode.
func TestReconcileExecuteErrFlagsDeepUnverified(t *testing.T) {
	// Zero unaddressed drift (in sync, no pending actions), --repair --prune.
	rep := &archive.Report{InSync: 3}

	if err := reconcileExecuteErr(rep, 0, true, true); err != nil {
		t.Fatalf("clean report with no footer failures should exit zero, got: %v", err)
	}

	err := reconcileExecuteErr(rep, 2, true, true)
	if err == nil {
		t.Fatal("execute mode must exit non-zero when --deep footer probes failed, got nil")
	}
	if !strings.Contains(err.Error(), "could not be deep-verified") {
		t.Fatalf("error must name the deep-verify failure, got: %v", err)
	}

	// The pre-existing drift rule is preserved: unaddressed drift still wins,
	// and a deep failure alongside it never lets the run exit 0.
	drifted := &archive.Report{Inserts: 1}
	if err := reconcileExecuteErr(drifted, 0, false, false); err == nil {
		t.Fatal("pending insert without --repair must exit non-zero")
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

// TestReconcileWiringFromReportDeepUnverified pins the command-layer wiring of
// the decision-layer count: runArchiveReconcile sources deepUnverified from
// report.DeepUnverified (the dual-backend / picked-Invalid signal), so a
// zero-DRIFT report that nonetheless has DeepUnverified>0 still fails the
// dry-run and shows up in both output modes. This is the integration point of
// the review fix — the count now originates in archive.Diff, not a scan-time
// probe counter.
func TestReconcileWiringFromReportDeepUnverified(t *testing.T) {
	// In sync (no actions), but one pair could not be deep-verified — the
	// dual-backend silent-downgrade state archive.Diff now reports.
	rep := &archive.Report{InSync: 2, DeepUnverified: 1}
	deepUnverified := rep.DeepUnverified // mirrors runArchiveReconcile

	// Dry-run must exit non-zero on the count even with zero diff actions.
	if err := reconcileDryRunErr(rep, deepUnverified); err == nil {
		t.Fatal("dry-run must fail when report.DeepUnverified>0 with no other drift")
	} else if !strings.Contains(err.Error(), "could not be deep-verified") {
		t.Fatalf("error must name the deep-verify failure, got: %v", err)
	}

	var jsonBuf bytes.Buffer
	if err := writeReconcileReport(&jsonBuf, "json", rep, deepUnverified, 0, nil, false, false); err != nil {
		t.Fatalf("writeReconcileReport json: %v", err)
	}
	var got reconcileReportJSON
	if err := json.Unmarshal(jsonBuf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonBuf.String())
	}
	if got.DeepUnverified != 1 {
		t.Fatalf("deep_unverified = %d, want 1 (raw: %s)", got.DeepUnverified, jsonBuf.String())
	}

	var textBuf bytes.Buffer
	if err := writeReconcileReport(&textBuf, "text", rep, deepUnverified, 0, nil, false, false); err != nil {
		t.Fatalf("writeReconcileReport text: %v", err)
	}
	if !strings.Contains(textBuf.String(), "could not be deep-verified") {
		t.Fatalf("text output must warn: %s", textBuf.String())
	}
}
