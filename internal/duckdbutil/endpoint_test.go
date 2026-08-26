package duckdbutil

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

func TestS3SecretClauses(t *testing.T) {
	cases := []struct {
		name   string
		region string
		ep     storage.S3Endpoint
		want   string
	}{
		{"aws, no region", "", storage.S3Endpoint{}, ""},
		{"aws, region pinned", "us-west-2", storage.S3Endpoint{}, ", REGION 'us-west-2'"},
		{"minio, path style, plain http", "", storage.S3Endpoint{URL: "http://minio:9000", PathStyle: true},
			", ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false"},
		{"wasabi, vhost, https, region", "us-east-1", storage.S3Endpoint{URL: "https://s3.wasabisys.com", PathStyle: false},
			", REGION 'us-east-1', ENDPOINT 's3.wasabisys.com', URL_STYLE 'vhost', USE_SSL true"},
		{"quotes are doubled", "it's", storage.S3Endpoint{}, ", REGION 'it''s'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := S3SecretClauses(tc.region, tc.ep); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestEnableS3CredentialChain_customEndpoint pins the DuckDB half of #1454:
// with BINTRAIL_S3_ENDPOINT set, the session's secret names that endpoint, so
// an httpfs read goes to the same store the SDK writes to instead of
// s3.amazonaws.com. Dummy env creds keep the chain resolvable offline.
func TestEnableS3CredentialChain_customEndpoint(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "testdummykey")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "http://minio:9000")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	EnableS3CredentialChain(context.Background(), db)

	var loaded bool
	if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil || !loaded {
		t.Skip("aws extension unavailable (offline host)")
	}
	var desc string
	if err := db.QueryRow("SELECT secret_string FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&desc); err != nil {
		t.Fatalf("chain secret missing: %v", err)
	}
	for _, want := range []string{"minio:9000", "path"} {
		if !strings.Contains(desc, want) {
			t.Errorf("secret does not carry %q: %s", want, desc)
		}
	}
}

// An invalid endpoint creates NO secret: the read then fails on credentials,
// loudly, instead of quietly reading an AWS bucket of the same name.
func TestEnableS3CredentialChain_invalidEndpointCreatesNoSecret(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "testdummykey")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "minio:9000")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	EnableS3CredentialChain(context.Background(), db)

	var loaded bool
	if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil || !loaded {
		t.Skip("aws extension unavailable (offline host)")
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("a secret was created over an invalid endpoint (n=%d, err=%v): reads would silently target AWS", n, err)
	}
}
