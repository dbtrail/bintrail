package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// ParseS3URL parses an S3 URL of the form s3://bucket or s3://bucket/prefix
// and returns the bucket name and prefix (without leading slash).
func ParseS3URL(u string) (bucket, prefix string, err error) {
	if !strings.HasPrefix(u, "s3://") {
		return "", "", fmt.Errorf("must start with s3://, got %q", u)
	}
	rest := strings.TrimPrefix(u, "s3://")
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("bucket name is empty in %q", u)
	}
	return bucket, prefix, nil
}

// NewS3Client creates an S3 client using the default AWS credential chain.
// region is optional — if empty, the SDK resolves it from AWS_REGION env var
// or ~/.aws/config.
func NewS3Client(ctx context.Context, region string) (*s3.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{}
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return s3.NewFromConfig(awsCfg), nil
}

// BuildS3Key constructs the S3 object key for a file by computing its path
// relative to baseDir and prepending prefix (if non-empty). This is the
// shared key-building logic used by both the baseline upload and the rotate
// archive loop.
func BuildS3Key(baseDir, filePath, prefix string) (string, error) {
	rel, err := filepath.Rel(baseDir, filePath)
	if err != nil {
		return "", err
	}
	key := filepath.ToSlash(rel)
	if prefix != "" {
		key = strings.TrimSuffix(prefix, "/") + "/" + key
	}
	return key, nil
}

// S3ObjectExists checks whether an object already exists in S3 by issuing a
// HeadObject request. Returns true when the object is found, false on 404,
// and an error for any other failure.
func S3ObjectExists(ctx context.Context, client *s3.Client, bucket, key string) (bool, error) {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// The SDK wraps NotFound as a modeled error type.
		var nf *types.NotFound
		if errors.As(err, &nf) {
			return false, nil
		}
		// HeadObject also surfaces 404 as a generic HTTP 404 response error
		// when the bucket itself has no matching key (some S3-compatible
		// backends use this path).
		var re *smithyhttp.ResponseError
		if errors.As(err, &re) && re.Response.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("head s3://%s/%s: %w", bucket, key, err)
	}
	return true, nil
}

// UploadFile opens a single local file and uploads it to S3. It is a separate
// function so that defer f.Close() runs when UploadFile returns — not when a
// WalkDir callback returns — preventing file descriptor accumulation over
// large directory trees.
func UploadFile(ctx context.Context, client *s3.Client, path, bucket, key string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   f,
	}); err != nil {
		return fmt.Errorf("upload %s → s3://%s/%s: %w", path, bucket, key, err)
	}
	return nil
}
