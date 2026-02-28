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
	for _, name := range []string{"index-dsn", "partitions", "s3-bucket", "s3-region", "s3-arn"} {
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

func TestInitCmd_s3ARNOptional(t *testing.T) {
	f := initCmd.Flag("s3-arn")
	if f == nil {
		t.Fatal("flag --s3-arn not registered")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] != nil {
		t.Error("--s3-arn should not be marked required")
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
		{"s3-arn", ""},
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

// ─── parseS3ARN ───────────────────────────────────────────────────────────────

func TestParseS3ARN_validARNs(t *testing.T) {
	cases := []struct {
		arn    string
		bucket string
	}{
		{"arn:aws:s3:::my-bucket", "my-bucket"},
		{"arn:aws:s3:::acme-bintrail-archives", "acme-bintrail-archives"},
		{"arn:aws-cn:s3:::china-bucket", "china-bucket"},
		{"arn:aws-us-gov:s3:::gov-bucket", "gov-bucket"},
	}
	for _, tc := range cases {
		got, err := parseS3ARN(tc.arn)
		if err != nil {
			t.Errorf("parseS3ARN(%q) unexpected error: %v", tc.arn, err)
			continue
		}
		if got != tc.bucket {
			t.Errorf("parseS3ARN(%q) = %q, want %q", tc.arn, got, tc.bucket)
		}
	}
}

func TestParseS3ARN_invalidARNs(t *testing.T) {
	cases := []struct {
		arn     string
		wantErr string
	}{
		{"not-an-arn", `expected 6 colon-separated fields`},
		{"xyz:aws:s3:::my-bucket", `must start with "arn:"`},
		{"arn:aws:ec2:::my-bucket", `service must be "s3"`},
		{"arn:aws:s3:::", `bucket name is empty`},
	}
	for _, tc := range cases {
		_, err := parseS3ARN(tc.arn)
		if err == nil {
			t.Errorf("parseS3ARN(%q) expected error containing %q, got nil", tc.arn, tc.wantErr)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("parseS3ARN(%q) error = %q, want it to contain %q", tc.arn, err.Error(), tc.wantErr)
		}
	}
}

// ─── s3IAMInstructions ────────────────────────────────────────────────────────

func TestS3IAMInstructions_containsBucketName(t *testing.T) {
	out := s3IAMInstructions("my-archive-bucket")
	if !strings.Contains(out, "my-archive-bucket") {
		t.Error("expected bucket name in IAM instructions output")
	}
	if !strings.Contains(out, "s3:PutObject") {
		t.Error("expected s3:PutObject in IAM instructions output")
	}
	if !strings.Contains(out, "s3:GetObject") {
		t.Error("expected s3:GetObject in IAM instructions output")
	}
	if !strings.Contains(out, "s3:ListBucket") {
		t.Error("expected s3:ListBucket in IAM instructions output")
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
