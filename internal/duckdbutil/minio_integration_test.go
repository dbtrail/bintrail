//go:build integration

package duckdbutil_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// TestS3Compat_MinIO is the leg #1453/#1454 asked for: against a real
// S3-compatible store, the SDK half WRITES through BINTRAIL_S3_ENDPOINT and
// the DuckDB half READS the same object back through the endpoint-carrying
// secret. Before the fix the write went to MinIO and the read to
// s3.amazonaws.com, which is the trap both issues describe. Each half was
// mutated to check this leg sees red: drop the secret's ENDPOINT clause and
// the read goes to bintrail-it.s3.us-east-1.amazonaws.com (403); drop
// BaseEndpoint and the client is not routed at all; force UsePathStyle off and
// the round trip fails too. What this leg does not cover is the
// BINTRAIL_S3_PATH_STYLE knob itself, which
// TestLoadAWSConfig_customEndpoint pins in both directions.
//
// Skips without
// BINTRAIL_TEST_MINIO_ENDPOINT (CI starts the container; locally:
// `docker run -d -p 9000:9000 -e MINIO_ROOT_USER=bintrail
// -e MINIO_ROOT_PASSWORD=bintrail-it-secret minio/minio server /data`).
func TestS3Compat_MinIO(t *testing.T) {
	endpoint := os.Getenv("BINTRAIL_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("BINTRAIL_TEST_MINIO_ENDPOINT not set")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", envOr("BINTRAIL_TEST_MINIO_ACCESS_KEY", "bintrail"))
	t.Setenv("AWS_SECRET_ACCESS_KEY", envOr("BINTRAIL_TEST_MINIO_SECRET_KEY", "bintrail-it-secret"))
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/aws-config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, endpoint)
	ctx := context.Background()

	// SDK half: create the bucket (idempotent) and upload a Parquet file that
	// DuckDB itself wrote, so no other package's writer is a dependency here.
	client, err := storage.NewS3Client(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	// Compare against the NORMALIZED value: a trailing slash is accepted and
	// trimmed, so comparing to the raw variable would fail on a correct setup.
	wantEndpoint, err := storage.S3EndpointFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if got := client.Options(); got.BaseEndpoint == nil || *got.BaseEndpoint != wantEndpoint.URL || !got.UsePathStyle {
		t.Fatalf("client not routed to MinIO: endpoint=%v pathStyle=%v", got.BaseEndpoint, got.UsePathStyle)
	}
	const bucket = "bintrail-it"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &owned) && !errors.As(err, &exists) {
			t.Fatalf("create bucket on MinIO: %v", err)
		}
	}
	local := filepath.Join(t.TempDir(), "t.parquet")
	gen, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gen.ExecContext(ctx, "COPY (SELECT 1 AS id, 'one' AS name UNION ALL SELECT 2, 'two') TO '"+local+"' (FORMAT PARQUET)"); err != nil {
		t.Fatal(err)
	}
	gen.Close()
	const key = "s3compat/t.parquet"
	if err := storage.UploadFile(ctx, client, local, bucket, key); err != nil {
		t.Fatalf("upload through the SDK half: %v", err)
	}
	if ok, err := storage.S3ObjectExists(ctx, client, bucket, key); err != nil || !ok {
		t.Fatalf("object not visible after upload: ok=%v err=%v", ok, err)
	}

	// DuckDB half: the endpoint-carrying secret makes read_parquet reach the
	// same store.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		t.Fatalf("httpfs: %v", err)
	}
	duckdbutil.EnableS3CredentialChain(ctx, db)
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM read_parquet('s3://"+bucket+"/"+key+"')").Scan(&n); err != nil {
		t.Fatalf("DuckDB read through the custom endpoint: %v", err)
	}
	if n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}

	// The same read with the aws extension switched off. Routing lives in the
	// session settings, not the secret, so the escape hatch (and an air-gapped
	// host that cannot install the extension, and a chain that resolves
	// nothing) must not silently redirect the read to AWS.
	t.Run("without the aws extension", func(t *testing.T) {
		t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "1")
		bare, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer bare.Close()
		if err := duckdbutil.LoadHTTPFS(ctx, bare); err != nil {
			t.Fatalf("httpfs: %v", err)
		}
		if err := duckdbutil.EnableS3CredentialChain(ctx, bare); err != nil {
			t.Fatal(err)
		}
		var m int
		if err := bare.QueryRowContext(ctx, "SELECT count(*) FROM read_parquet('s3://"+bucket+"/"+key+"')").Scan(&m); err != nil {
			t.Fatalf("read without the aws extension: %v", err)
		}
		if m != 2 {
			t.Fatalf("rows = %d, want 2", m)
		}
	})
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
