package duckdbutil

import (
	"context"
	"database/sql"
	"errors"
	"slices"
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

// TestS3SettingStatements pins the statements that decide WHERE this instance's
// s3:// requests go. Both this process and the downloadable views.sql render
// from this list, so it is the one place the two can be compared.
func TestS3SettingStatements(t *testing.T) {
	minio := storage.S3Endpoint{URL: "http://minio:9000", PathStyle: true}
	cases := []struct {
		name   string
		region string
		ep     storage.S3Endpoint
		want   []string
	}{
		// AWS: httpfs's own defaults are already right, and rerouting them
		// would be a new failure mode for every existing user. Region included:
		// with no endpoint there is nothing to reroute, and the secret carries
		// the region on its own.
		{"aws stays untouched", "us-west-2", storage.S3Endpoint{}, nil},
		{"endpoint, no region", "", minio, []string{
			"SET GLOBAL s3_endpoint='minio:9000'",
			"SET GLOBAL s3_url_style='path'",
			"SET GLOBAL s3_use_ssl=false",
		}},
		// The region belongs here and not only in the secret: three documented
		// branches skip the secret entirely, and a request signed for the wrong
		// region is rejected by an S3-compatible store that checks it.
		{"endpoint and region", "eu-central-1", minio, []string{
			"SET GLOBAL s3_endpoint='minio:9000'",
			"SET GLOBAL s3_url_style='path'",
			"SET GLOBAL s3_use_ssl=false",
			"SET GLOBAL s3_region='eu-central-1'",
		}},
		{"vhost over https", "", storage.S3Endpoint{URL: "https://s3.wasabisys.com"}, []string{
			"SET GLOBAL s3_endpoint='s3.wasabisys.com'",
			"SET GLOBAL s3_url_style='vhost'",
			"SET GLOBAL s3_use_ssl=true",
		}},
		{"quotes are doubled", "it's", minio, []string{
			"SET GLOBAL s3_endpoint='minio:9000'",
			"SET GLOBAL s3_url_style='path'",
			"SET GLOBAL s3_use_ssl=false",
			"SET GLOBAL s3_region='it''s'",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := S3SettingStatements(tc.region, tc.ep)
			if !slices.Equal(got, tc.want) {
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
	if err := EnableS3CredentialChain(context.Background(), db); err != nil {
		t.Fatal(err)
	}

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

// TestEnableS3CredentialChain_routesWithoutAWSExtension is the regression
// guard for the hole this feature would otherwise have (#1454): the endpoint
// used to live only in the credential-chain secret, and three branches skip
// that secret — the documented escape hatch, an aws extension that will not
// install on an air-gapped host, and a chain that resolves nothing. Every one
// of those is a plausible S3-compatible-store deployment, and an unrouted
// session does not fail: it succeeds against AWS with the ambient credentials.
func TestEnableS3CredentialChain_routesWithoutAWSExtension(t *testing.T) {
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "1") // the branch that skips the secret
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "https://s3.wasabisys.com")

	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := LoadHTTPFS(ctx, db); err != nil {
		t.Skip("httpfs unavailable (offline host)")
	}
	// Hold one connection open ACROSS the call, so the pool must hand the
	// configure step a different one. Reading back through the held
	// connection then proves the routing reached the whole pool: a plain SET
	// binds to the connection that ran it, and every other one goes to AWS.
	// (An idle pool reuses its single connection, which makes the obvious
	// version of this test pass either way.)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := EnableS3CredentialChain(ctx, db); err != nil {
		t.Fatal(err)
	}
	for setting, want := range map[string]string{
		"s3_endpoint":  "s3.wasabisys.com",
		"s3_url_style": "path",
		"s3_use_ssl":   "true",
	} {
		var got string
		if err := conn.QueryRowContext(ctx, "SELECT current_setting('"+setting+"')").Scan(&got); err != nil {
			t.Fatalf("read %s: %v", setting, err)
		}
		if got != want {
			t.Errorf("%s = %q, want %q: reads would go to AWS", setting, got, want)
		}
	}
	var n int
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("the escape hatch created a secret anyway (n=%d, err=%v)", n, err)
	}
}

// With no endpoint configured the session is left exactly as it was: httpfs's
// own defaults are correct for AWS, and touching them would be a new failure
// mode for every existing user.
func TestEnableS3CredentialChain_noEndpointTouchesNothing(t *testing.T) {
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "1")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_ENDPOINT_URL", "")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := LoadHTTPFS(context.Background(), db); err != nil {
		t.Skip("httpfs unavailable (offline host)")
	}
	if err := EnableS3CredentialChain(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	// NULL when the session never set it, which is the point: untouched.
	var got sql.NullString
	if err := db.QueryRow("SELECT current_setting('s3_endpoint')").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got.Valid && got.String != "" {
		t.Errorf("s3_endpoint = %q, want the httpfs default for AWS", got.String)
	}
}

// An invalid endpoint is returned as an error, not logged and walked past: on
// the baseline read paths there is no SDK call to fail first, so a log line
// would let the read proceed against AWS.
func TestEnableS3CredentialChain_invalidEndpointIsAnError(t *testing.T) {
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
	err = EnableS3CredentialChain(context.Background(), db)
	if err == nil {
		t.Fatal("an invalid endpoint was accepted: the read would target AWS")
	}
	if !errors.Is(err, storage.ErrS3EndpointConfig) {
		t.Errorf("error does not wrap ErrS3EndpointConfig: %v", err)
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("a secret was created over an invalid endpoint (n=%d, err=%v)", n, err)
	}
}

// TestEnableS3CredentialChainRegion_regionReachesTheSessionWithoutSecret runs
// the region statement against a real DuckDB, which nothing else does: the
// only other region test pins no endpoint, so its statement list is empty and
// the SET never executes. That matters twice over. In this process an invalid
// statement is a returned error, and in the downloadable views.sql it is worse:
// `duckdb -init views.sql` ABORTS the file at the first failure, so a
// mis-rendered SET would take the view definitions down with it.
//
// The escape hatch is on, so the secret is skipped and the settings are the
// only thing carrying the region. That is the deployment this setting exists
// for, and the one where a store that validates the signing region rejects
// what an unpinned session sends.
func TestEnableS3CredentialChainRegion_regionReachesTheSessionWithoutSecret(t *testing.T) {
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "1")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "http://minio:9000")

	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := LoadHTTPFS(ctx, db); err != nil {
		t.Skip("httpfs unavailable (offline host)")
	}
	if err := EnableS3CredentialChainRegion(ctx, db, "eu-central-1"); err != nil {
		t.Fatalf("the region statement was rejected by DuckDB: %v", err)
	}

	var got string
	if err := db.QueryRowContext(ctx, "SELECT current_setting('s3_region')").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "eu-central-1" {
		t.Errorf("s3_region = %q: requests would be signed for the wrong region", got)
	}
	// Positive evidence that the secret really was skipped, so the assertion
	// above is about the settings and not about a secret quietly doing the work.
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("a secret exists, so this proves nothing about the settings (n=%d, err=%v)", n, err)
	}
}
