package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestStartUpRotation_armsRotateGlobals is the guard-arming regression test:
// the entire data-loss safety of the built-in rotation reduces to the rot*
// fan-out in startUpRotation. If rotProtectUnarchived stopped being set, `up`
// would silently drop unarchived partitions by default and the integration
// tests (which arm the guard themselves) would stay green. Mirrors
// TestPopulateStreamFlags for the stream fan-out.
func TestStartUpRotation_armsRotateGlobals(t *testing.T) {
	saved := struct {
		retain, archiveDir, archiveS3, bintrailID, format string
		addFuture                                         int
		noReplace, retry, protect                         bool
	}{rotRetain, rotArchiveDir, rotArchiveS3, rotBintrailID, rotFormat,
		rotAddFuture, rotNoReplace, rotRetry, rotProtectUnarchived}
	t.Cleanup(func() {
		rotRetain, rotArchiveDir, rotArchiveS3, rotBintrailID, rotFormat =
			saved.retain, saved.archiveDir, saved.archiveS3, saved.bintrailID, saved.format
		rotAddFuture, rotNoReplace, rotRetry, rotProtectUnarchived =
			saved.addFuture, saved.noReplace, saved.retry, saved.protect
	})

	// Poison every global so the assertions prove startUpRotation overwrote
	// them rather than inheriting a lucky zero value.
	rotProtectUnarchived = false
	rotNoReplace = true
	rotArchiveDir = "/poison"
	rotArchiveS3 = "s3://poison"
	rotBintrailID = "poison"
	rotRetry = true
	rotFormat = "text"
	rotAddFuture = 99
	rotRetain = "poison"

	s, err := parseUpRotation("30d", "1h", 3)
	if err != nil {
		t.Fatalf("parseUpRotation: %v", err)
	}
	// Cancelled ctx + empty DSN provider: the immediate first cycle dedupes
	// to nothing (no DB touched), then the loop exits on ctx.Done.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	startUpRotation(ctx, s, func() []string { return nil })

	if !rotProtectUnarchived {
		t.Error("rotProtectUnarchived must be armed — without it the built-in rotation drops unarchived data")
	}
	if rotNoReplace {
		t.Error("rotNoReplace must be false (dropped partitions are replaced)")
	}
	if rotArchiveDir != "" || rotArchiveS3 != "" || rotBintrailID != "" {
		t.Errorf("archive fields must be empty (built-in rotation never archives): dir=%q s3=%q id=%q",
			rotArchiveDir, rotArchiveS3, rotBintrailID)
	}
	if rotRetry {
		t.Error("rotRetry must be false")
	}
	if rotFormat != "json" {
		t.Errorf("rotFormat = %q, want json (suppresses per-partition stdout chatter)", rotFormat)
	}
	if rotAddFuture != 3 {
		t.Errorf("rotAddFuture = %d, want 3", rotAddFuture)
	}
	if rotRetain != "30d" {
		t.Errorf("rotRetain = %q, want 30d", rotRetain)
	}
}

// TestStartUpRotation_disabledIsInert verifies the disabled path starts no
// loop and never consults the DSN provider.
func TestStartUpRotation_disabledIsInert(t *testing.T) {
	called := false
	startUpRotation(context.Background(), upRotationSettings{}, func() []string {
		called = true
		return nil
	})
	if called {
		t.Error("disabled rotation must not invoke the DSN provider")
	}
}

// TestUpRotateFlagsRegistered pins the flag names to the envBindings strings:
// bindCommandEnv silently skips bindings whose flag doesn't exist on the
// command, so a renamed flag would make BINTRAIL_ROTATE_RETAIN=off silently
// fail to disable rotation.
func TestUpRotateFlagsRegistered(t *testing.T) {
	for _, name := range []string{"rotate-retain", "rotate-interval", "rotate-add-future"} {
		if upCmd.Flags().Lookup(name) == nil {
			t.Errorf("upCmd is missing --%s — its BINTRAIL_* env binding would silently no-op", name)
		}
	}
}

func TestParseUpRotation_enabled(t *testing.T) {
	s, err := parseUpRotation("30d", "1h", 3)
	if err != nil {
		t.Fatalf("parseUpRotation: %v", err)
	}
	if !s.enabled {
		t.Fatal("expected enabled")
	}
	if s.retain != 30*24*time.Hour {
		t.Errorf("retain = %v, want 720h", s.retain)
	}
	if s.interval != time.Hour {
		t.Errorf("interval = %v, want 1h", s.interval)
	}
	if s.addFuture != 3 {
		t.Errorf("addFuture = %d, want 3", s.addFuture)
	}
	if s.retainRaw != "30d" {
		t.Errorf("retainRaw = %q, want 30d", s.retainRaw)
	}
}

func TestParseUpRotation_disabledForms(t *testing.T) {
	for _, retain := range []string{"off", "0", ""} {
		s, err := parseUpRotation(retain, "1h", 3)
		if err != nil {
			t.Errorf("parseUpRotation(%q): unexpected error %v", retain, err)
			continue
		}
		if s.enabled {
			t.Errorf("parseUpRotation(%q): expected disabled", retain)
		}
	}
}

func TestParseUpRotation_invalidRetain(t *testing.T) {
	_, err := parseUpRotation("1x", "1h", 3)
	if err == nil {
		t.Fatal("expected error for invalid retain unit")
	}
	if !strings.Contains(err.Error(), "off") {
		t.Errorf("error should mention the \"off\" escape hatch, got: %v", err)
	}
}

func TestParseUpRotation_invalidInterval(t *testing.T) {
	if _, err := parseUpRotation("7d", "soon", 3); err == nil {
		t.Fatal("expected error for unparseable interval")
	}
	if _, err := parseUpRotation("7d", "-1h", 3); err == nil {
		t.Fatal("expected error for non-positive interval")
	}
}

func TestParseUpRotation_negativeAddFuture(t *testing.T) {
	if _, err := parseUpRotation("7d", "1h", -1); err == nil {
		t.Fatal("expected error for negative add-future")
	}
}

func TestDedupeDSNs(t *testing.T) {
	in := []string{"a", "", "b", "a", "c", "b"}
	got := dedupeDSNs(in)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("dedupeDSNs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dedupeDSNs = %v, want %v", got, want)
		}
	}
}
