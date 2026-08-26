package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	smithy "github.com/aws/smithy-go"
)

// GetBucketLocation sits outside bintrail's documented minimal IAM policy, so a
// denial is the EXPECTED shape and must stay a debug line with a usable
// fallback. Every other failure is worth a warning. Misclassifying either way
// is a support cost: a warning on every read for a policy we ourselves
// recommend, or a silenced SCP deny that looks like a benign one.
func TestIsBucketLocationAccessDenied(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"AccessDenied code", &smithy.GenericAPIError{Code: "AccessDenied", Message: "not authorized"}, true},
		{"AccessDeniedException code", &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized"}, true},
		{"wrapped AccessDenied", fmt.Errorf("get bucket location: %w", &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}), true},
		{"NoSuchBucket code", &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "not found"}, false},
		{"non-API error", errors.New("connection reset"), false},
		{"nil error", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isBucketLocationAccessDenied(tc.err); got != tc.want {
				t.Errorf("isBucketLocationAccessDenied(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestDetectBucketRegion_failureIsReportedAsSuchExists because the fallback is
// the COMMON path (s3:GetBucketLocation is outside bintrail's documented
// minimal IAM policy) and it returns a plausible, confident, WRONG region. The
// only thing separating it from a detection is the second return value, and a
// caller that publishes the answer relies on it entirely.
//
// A cancelled context makes the call fail without a network or an AWS account,
// so this is hermetic.
func TestDetectBucketRegion_failureIsReportedAsSuch(t *testing.T) {
	// NOT us-east-1: that is what the SUCCESS path substitutes for an empty
	// LocationConstraint, so an ambient us-east-1 would let a mutation that
	// returns the literal survive this assertion.
	t.Setenv("AWS_REGION", "eu-west-2")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "testdummykey")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")

	cfg, err := LoadAWSConfig(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	region, detected := DetectBucketRegion(ctx, cfg, "some-bucket")
	if detected {
		t.Error("a failed lookup reported itself as a detection; the console would pin this guess into a downloadable file")
	}
	// The read path still needs a usable value, which is why the failure is not
	// an error: it is about to read, and a wrong guess fails there, loudly.
	if region != "eu-west-2" {
		t.Errorf("region = %q, want the resolved default for the read path to fall back on", region)
	}
}
