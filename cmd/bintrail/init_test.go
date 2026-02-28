package main

import (
	"strings"
	"testing"
	"time"
)

// ─── Cobra command wiring ─────────────────────────────────────────────────────

func TestInitCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "init" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'init' command to be registered under rootCmd")
	}
}

func TestInitCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{"index-dsn", "partitions", "s3-bucket", "s3-region"} {
		if initCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on initCmd", name)
		}
	}
}

func TestInitCmd_indexDSNRequired(t *testing.T) {
	f := initCmd.Flag("index-dsn")
	if f == nil {
		t.Fatal("flag --index-dsn not registered")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("--index-dsn should be marked required")
	}
}

func TestInitCmd_s3BucketOptional(t *testing.T) {
	f := initCmd.Flag("s3-bucket")
	if f == nil {
		t.Fatal("flag --s3-bucket not registered")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] != nil {
		t.Error("--s3-bucket should not be marked required")
	}
}

func TestInitCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"partitions", "48"},
		{"s3-region", "us-east-1"},
		{"s3-bucket", ""},
	}
	for _, tc := range cases {
		f := initCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s default: expected %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

// ─── s3Instructions ───────────────────────────────────────────────────────────

// ─── buildPartitionDefs ───────────────────────────────────────────────────────

func TestBuildPartitionDefs_countAndFuture(t *testing.T) {
	now := time.Date(2026, 2, 28, 14, 30, 0, 0, time.UTC)
	parts := buildPartitionDefs(now, 48)

	// 48 named hourly partitions + p_future = 49
	if len(parts) != 49 {
		t.Fatalf("expected 49 partition defs, got %d", len(parts))
	}
	if !strings.Contains(parts[48], "p_future") {
		t.Errorf("last def should be p_future, got %q", parts[48])
	}
}

func TestBuildPartitionDefs_spansFromPastToCurrentHour(t *testing.T) {
	// now truncated to 14:00; with 48 partitions: start = 14:00 - 47h = 2026-02-26 15:00 UTC
	now := time.Date(2026, 2, 28, 14, 30, 0, 0, time.UTC)
	parts := buildPartitionDefs(now, 48)

	// First partition starts 47 hours before the current hour
	if !strings.Contains(parts[0], "p_2026022615") {
		t.Errorf("expected first partition p_2026022615 (47h ago), got %q", parts[0])
	}
	// Last named partition covers the current hour (14:00 UTC today)
	if !strings.Contains(parts[47], "p_2026022814") {
		t.Errorf("expected last named partition p_2026022814 (current hour), got %q", parts[47])
	}
}

func TestBuildPartitionDefs_boundaryValues(t *testing.T) {
	now := time.Date(2026, 2, 28, 14, 30, 0, 0, time.UTC)
	parts := buildPartitionDefs(now, 3)

	// 3 partitions: p_2026022812, p_2026022813, p_2026022814, p_future
	if !strings.Contains(parts[0], "p_2026022812") {
		t.Errorf("expected p_2026022812 (2h ago), got %q", parts[0])
	}
	if !strings.Contains(parts[1], "p_2026022813") {
		t.Errorf("expected p_2026022813, got %q", parts[1])
	}
	if !strings.Contains(parts[2], "p_2026022814") {
		t.Errorf("expected p_2026022814, got %q", parts[2])
	}
	// p_2026022812 boundary: LESS THAN TO_SECONDS('2026-02-28 13:00:00')
	if !strings.Contains(parts[0], "2026-02-28 13:00:00") {
		t.Errorf("expected boundary at 2026-02-28 13:00:00, got %q", parts[0])
	}
}

func TestBuildPartitionDefs_singlePartition(t *testing.T) {
	now := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	parts := buildPartitionDefs(now, 1)

	// 1 named + p_future = 2
	if len(parts) != 2 {
		t.Fatalf("expected 2 defs, got %d", len(parts))
	}
	if !strings.Contains(parts[0], "p_2026022810") {
		t.Errorf("expected p_2026022810 (current hour), got %q", parts[0])
	}
}

// ─── s3Instructions ───────────────────────────────────────────────────────────

func TestS3Instructions_usEast1_noLocationConstraint(t *testing.T) {
	out := s3Instructions("my-bucket", "us-east-1")

	if !strings.Contains(out, "create-bucket") {
		t.Error("expected create-bucket command in output")
	}
	if strings.Contains(out, "LocationConstraint") {
		t.Error("us-east-1 should not include LocationConstraint")
	}
	if !strings.Contains(out, "my-bucket") {
		t.Error("expected bucket name in output")
	}
	if !strings.Contains(out, "us-east-1") {
		t.Error("expected region in output")
	}
}

func TestS3Instructions_otherRegion_hasLocationConstraint(t *testing.T) {
	out := s3Instructions("acme-bintrail", "eu-west-1")

	if !strings.Contains(out, "LocationConstraint=eu-west-1") {
		t.Error("expected LocationConstraint=eu-west-1 in output")
	}
	if !strings.Contains(out, "acme-bintrail") {
		t.Error("expected bucket name in output")
	}
}

func TestS3Instructions_containsPublicAccessBlock(t *testing.T) {
	out := s3Instructions("test-bucket", "us-east-1")

	if !strings.Contains(out, "put-public-access-block") {
		t.Error("expected put-public-access-block command in output")
	}
	if !strings.Contains(out, "BlockPublicAcls=true") {
		t.Error("expected BlockPublicAcls=true in output")
	}
}

func TestS3Instructions_containsLifecyclePolicy(t *testing.T) {
	out := s3Instructions("test-bucket", "us-east-1")

	if !strings.Contains(out, "put-bucket-lifecycle-configuration") {
		t.Error("expected lifecycle configuration command in output")
	}
	if !strings.Contains(out, "365") {
		t.Error("expected 365 days in lifecycle output")
	}
	if !strings.Contains(out, "bintrail-1yr-expiry") {
		t.Error("expected rule ID in lifecycle output")
	}
}

func TestS3Instructions_containsConsoleURL(t *testing.T) {
	out := s3Instructions("test-bucket", "ap-southeast-1")

	if !strings.Contains(out, "s3.console.aws.amazon.com") {
		t.Error("expected AWS Console URL in output")
	}
	if !strings.Contains(out, "365 days") {
		t.Error("expected 365 days mention in console instructions")
	}
}
