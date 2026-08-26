package storage

import (
	"context"
	"errors"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
)

// DetectBucketRegion resolves a bucket's ACTUAL region, falling back to the
// resolved default when it cannot. Best-effort by contract: it never returns an
// error, because every caller has a usable answer without one and none of them
// should fail a read over a region hint.
//
// Why it is not just cfg.Region: a bucket outside the configured region answers
// 301 PermanentRedirect, and GetBucketLocation is the call that prevents it.
// That call must itself be made from us-east-1.
//
// Lives here rather than beside its first caller because a second one now needs
// the SAME answer for a different reason: the console names archive roots in a
// downloadable views.sql, and a file whose region disagrees with the daemon's
// own reads sends the recipient somewhere the daemon never went.
func DetectBucketRegion(ctx context.Context, cfg aws.Config, bucket string) string {
	locClient := NewS3ClientFromConfig(cfg, func(o *s3.Options) {
		o.Region = "us-east-1"
	})
	loc, err := locClient.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: &bucket})
	if err != nil {
		if isBucketLocationAccessDenied(err) {
			// Expected: GetBucketLocation is outside the minimal IAM policy.
			// Still logs err so the rarer non-benign case sharing this error
			// code (an SCP or VPC-endpoint-policy deny, a cross-account
			// restriction) stays diagnosable at --log-level debug.
			slog.Debug("skipping S3 bucket region auto-detection: GetBucketLocation denied (expected under the minimal IAM policy); using resolved default region",
				"bucket", bucket, "region", cfg.Region, "error", err)
		} else {
			slog.Warn("could not detect S3 bucket region, using default", "bucket", bucket, "error", err)
		}
		return cfg.Region
	}
	r := string(loc.LocationConstraint)
	if r == "" {
		r = "us-east-1" // GetBucketLocation returns empty for us-east-1
	}
	if r != cfg.Region {
		slog.Debug("S3 bucket in different region, switching", "bucket", bucket, "bucket_region", r, "default_region", cfg.Region)
	}
	return r
}

func isBucketLocationAccessDenied(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	code := apiErr.ErrorCode()
	return code == "AccessDenied" || code == "AccessDeniedException"
}
