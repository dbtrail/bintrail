package storage

import (
	"strings"
	"testing"
)

func TestParseS3URL(t *testing.T) {
	cases := []struct {
		input      string
		wantBucket string
		wantPrefix string
		wantErr    bool
	}{
		{"s3://my-bucket", "my-bucket", "", false},
		{"s3://my-bucket/", "my-bucket", "", false},
		{"s3://my-bucket/baselines/", "my-bucket", "baselines/", false},
		{"s3://my-bucket/prefix/sub", "my-bucket", "prefix/sub", false},
		{"http://my-bucket/prefix", "", "", true},
		{"s3://", "", "", true},
	}
	for _, tc := range cases {
		bucket, prefix, err := ParseS3URL(tc.input)
		if (err != nil) != tc.wantErr {
			t.Errorf("ParseS3URL(%q) error = %v, wantErr %v", tc.input, err, tc.wantErr)
			continue
		}
		if !tc.wantErr {
			if bucket != tc.wantBucket {
				t.Errorf("ParseS3URL(%q) bucket = %q, want %q", tc.input, bucket, tc.wantBucket)
			}
			if prefix != tc.wantPrefix {
				t.Errorf("ParseS3URL(%q) prefix = %q, want %q", tc.input, prefix, tc.wantPrefix)
			}
		}
	}
}

// TestParseS3URL_emptyBucketWithPath verifies that s3:///path (three slashes,
// empty bucket name) is rejected — strings.Cut on "/" gives bucket="" which
// hits the "bucket name is empty" guard.
func TestParseS3URL_emptyBucketWithPath(t *testing.T) {
	_, _, err := ParseS3URL("s3:///some/path")
	if err == nil {
		t.Fatal("expected error for s3:///some/path (empty bucket), got nil")
	}
	if !strings.Contains(err.Error(), "bucket") {
		t.Errorf("expected 'bucket' in error, got: %v", err)
	}
}

// TestParseS3URL_prefixRetainsTrailingSlash verifies that a prefix that ends
// with "/" is returned as-is (the function does not strip it — callers like
// uploadBaselineToS3 handle normalisation with TrimSuffix).
func TestParseS3URL_prefixRetainsTrailingSlash(t *testing.T) {
	_, prefix, err := ParseS3URL("s3://my-bucket/baselines/2026/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if prefix != "baselines/2026/" {
		t.Errorf("expected prefix %q, got %q", "baselines/2026/", prefix)
	}
}
