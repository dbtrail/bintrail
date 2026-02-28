package main

import (
	"strings"
	"testing"
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
