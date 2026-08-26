package storage

import (
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
