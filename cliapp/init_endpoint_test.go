package cliapp

import (
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// TestSetupS3Bucket_endpointConfigIsNotABucketProblem (#1453): a malformed
// BINTRAIL_S3_ENDPOINT must surface as the configuration fault it is. The
// caller degrades bucket failures to a warning plus a "create it by hand"
// recipe, and no amount of manual bucket creation fixes a typo in a URL.
func TestSetupS3Bucket_endpointConfigIsNotABucketProblem(t *testing.T) {
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "minio:9000") // no scheme

	err := setupS3Bucket(t.Context(), "some-bucket", "us-east-1")
	if err == nil {
		t.Fatal("a malformed endpoint was accepted")
	}
	if !errors.Is(err, storage.ErrS3EndpointConfig) {
		t.Fatalf("error is not recognizable as a config fault: %v", err)
	}
	if err := verifyS3Bucket(t.Context(), "some-bucket", "us-east-1"); !errors.Is(err, storage.ErrS3EndpointConfig) {
		t.Fatalf("verify: error is not recognizable as a config fault: %v", err)
	}
}
