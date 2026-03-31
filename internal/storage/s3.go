package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// s3API is the subset of the S3 client interface used by S3Backend.
// Defined as an interface for testability.
type s3API interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, input *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	HeadBucket(ctx context.Context, input *s3.HeadBucketInput, opts ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// S3Config holds the configuration for an S3 storage backend.
type S3Config struct {
	// Bucket is the S3 bucket name (required).
	Bucket string

	// Region is the AWS region. If empty, the SDK resolves it from the
	// standard AWS configuration chain (environment, shared config,
	// instance metadata).
	Region string

	// Prefix is an optional key prefix applied to all operations.
	// For example, "bintrail/" causes Put("foo.parquet", r) to write
	// to "bintrail/foo.parquet" in the bucket.
	Prefix string

	// Endpoint is an optional custom S3 endpoint URL for S3-compatible
	// services (MinIO, LocalStack). Leave empty for standard AWS S3.
	Endpoint string
}

// S3Backend implements Backend using AWS S3 (or any S3-compatible service).
type S3Backend struct {
	client s3API
	bucket string
	prefix string
}

// NewS3Backend creates an S3Backend and validates that the credentials and
// bucket are accessible by issuing a HeadBucket request. Returns an error
// if the bucket does not exist or credentials are invalid.
func NewS3Backend(ctx context.Context, cfg S3Config) (*S3Backend, error) {
	client, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return newS3BackendFromClient(ctx, client, cfg)
}

// newS3Client creates an S3 client from config.
func newS3Client(ctx context.Context, cfg S3Config) (*s3.Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("storage: load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true // required for MinIO / LocalStack
		})
	}

	return s3.NewFromConfig(awsCfg, s3Opts...), nil
}

// newS3BackendFromClient creates an S3Backend from an existing s3API client.
// Used by NewS3Backend and tests.
func newS3BackendFromClient(ctx context.Context, client s3API, cfg S3Config) (*S3Backend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("storage: S3 bucket name is required")
	}

	// Validate credentials and bucket access.
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	}); err != nil {
		return nil, fmt.Errorf("storage: validate bucket %q: %w", cfg.Bucket, err)
	}

	prefix := cfg.Prefix
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}

	return &S3Backend{
		client: client,
		bucket: cfg.Bucket,
		prefix: prefix,
	}, nil
}

// validateKey rejects empty keys and keys with a leading slash.
func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("storage: key must not be empty")
	}
	if strings.HasPrefix(key, "/") {
		return fmt.Errorf("storage: key must not start with /")
	}
	return nil
}

// fullKey returns the full S3 key by prepending the configured prefix.
func (b *S3Backend) fullKey(key string) string {
	return b.prefix + key
}

// relKey strips the configured prefix from a full S3 key.
func (b *S3Backend) relKey(fullKey string) string {
	return strings.TrimPrefix(fullKey, b.prefix)
}

// Put uploads the content from r to the given key.
func (b *S3Backend) Put(ctx context.Context, key string, r io.Reader) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if _, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.fullKey(key)),
		Body:   r,
	}); err != nil {
		return fmt.Errorf("storage: put %q: %w", key, err)
	}
	return nil
}

// Get returns a reader for the content at the given key.
func (b *S3Backend) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	resp, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.fullKey(key)),
	})
	if err != nil {
		return nil, fmt.Errorf("storage: get %q: %w", key, err)
	}
	return resp.Body, nil
}

// List returns all keys under the given prefix.
func (b *S3Backend) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := b.fullKey(prefix)
	var keys []string

	var continuationToken *string
	for {
		resp, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("storage: list %q (after %d keys): %w", prefix, len(keys), err)
		}
		for _, obj := range resp.Contents {
			if obj.Key != nil {
				keys = append(keys, b.relKey(*obj.Key))
			}
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	return keys, nil
}

// Delete removes the object at the given key.
func (b *S3Backend) Delete(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if _, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.fullKey(key)),
	}); err != nil {
		return fmt.Errorf("storage: delete %q: %w", key, err)
	}
	return nil
}

// Exists checks whether an object exists at the given key.
func (b *S3Backend) Exists(ctx context.Context, key string) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	_, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.fullKey(key)),
	})
	if err != nil {
		var nf *types.NotFound
		if errors.As(err, &nf) {
			return false, nil
		}
		// Some S3-compatible backends (Ceph, Wasabi) return NoSuchKey
		// from HeadObject instead of NotFound.
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return false, nil
		}
		// Some S3-compatible backends surface 404 as a generic HTTP error.
		var re *smithyhttp.ResponseError
		if errors.As(err, &re) && re.Response.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("storage: exists %q: %w", key, err)
	}
	return true, nil
}
