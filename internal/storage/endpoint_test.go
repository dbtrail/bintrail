package storage

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// isolateAWSEnv keeps the operator's real ~/.aws files and env out of a test
// that reasons about "no region configured anywhere".
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/aws-config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "testdummykey")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv(EnvS3Endpoint, "")
	t.Setenv(EnvS3PathStyle, "")
}

func TestS3EndpointFromEnv(t *testing.T) {
	cases := []struct {
		name      string
		env       map[string]string
		want      S3Endpoint
		wantHost  string
		wantSSL   bool
		wantError string
	}{
		{name: "unset means AWS", want: S3Endpoint{}},
		{
			name:     "MinIO over http, path style by default",
			env:      map[string]string{EnvS3Endpoint: "http://minio:9000"},
			want:     S3Endpoint{URL: "http://minio:9000", PathStyle: true, Source: EnvS3Endpoint},
			wantHost: "minio:9000",
		},
		{
			name:     "Wasabi over https, virtual-hosted on request",
			env:      map[string]string{EnvS3Endpoint: "https://s3.wasabisys.com", EnvS3PathStyle: "false"},
			want:     S3Endpoint{URL: "https://s3.wasabisys.com", PathStyle: false, Source: EnvS3Endpoint, pathStyleExplicit: true},
			wantHost: "s3.wasabisys.com",
			wantSSL:  true,
		},
		{
			// Mirrored to DuckDB so reads follow writes, but left virtual-hosted:
			// that is how the SDK addresses it, and the two halves must agree.
			name:     "the SDK's own variable is mirrored, with the SDK's addressing",
			env:      map[string]string{"AWS_ENDPOINT_URL_S3": "http://localstack:4566"},
			want:     S3Endpoint{URL: "http://localstack:4566", Source: "AWS_ENDPOINT_URL_S3"},
			wantHost: "localstack:4566",
		},
		{
			name:     "an explicit path style applies to an SDK-resolved endpoint too",
			env:      map[string]string{"AWS_ENDPOINT_URL": "http://minio:9000", EnvS3PathStyle: "true"},
			want:     S3Endpoint{URL: "http://minio:9000", PathStyle: true, Source: "AWS_ENDPOINT_URL", pathStyleExplicit: true},
			wantHost: "minio:9000",
		},
		{
			// The standard SDK pattern: a generic endpoint plus a
			// service-specific override. The service-specific one wins there,
			// so it must win here or the two halves split.
			name:     "the service-specific AWS variable wins over the generic one",
			env:      map[string]string{"AWS_ENDPOINT_URL": "http://generic:1", "AWS_ENDPOINT_URL_S3": "http://s3-specific:2"},
			want:     S3Endpoint{URL: "http://s3-specific:2", Source: "AWS_ENDPOINT_URL_S3"},
			wantHost: "s3-specific:2",
		},
		{
			name:     "BINTRAIL_S3_ENDPOINT wins over the SDK variable",
			env:      map[string]string{EnvS3Endpoint: "http://minio:9000", "AWS_ENDPOINT_URL_S3": "http://other:1"},
			want:     S3Endpoint{URL: "http://minio:9000", PathStyle: true, Source: EnvS3Endpoint},
			wantHost: "minio:9000",
		},
		{
			name:     "a trailing slash is not a path",
			env:      map[string]string{EnvS3Endpoint: "http://minio:9000/"},
			want:     S3Endpoint{URL: "http://minio:9000", PathStyle: true, Source: EnvS3Endpoint},
			wantHost: "minio:9000",
		},
		{
			// The SDK accepts shapes this package cannot render into DuckDB.
			// Failing every command over one would break a working setup on
			// upgrade, so it is dropped from the mirror, not made fatal.
			name: "an SDK variable this package cannot parse is not fatal",
			env:  map[string]string{"AWS_ENDPOINT_URL": "https://gw.corp/aws"},
			want: S3Endpoint{},
		},
		{name: "no scheme", env: map[string]string{EnvS3Endpoint: "minio:9000"}, wantError: "scheme"},
		{name: "a path is not supported", env: map[string]string{EnvS3Endpoint: "http://host/s3"}, wantError: "no path"},
		{name: "credentials in the URL are refused", env: map[string]string{EnvS3Endpoint: "http://u:p@host:9000"}, wantError: "credentials"},
		{name: "bad path-style value", env: map[string]string{EnvS3Endpoint: "http://minio:9000", EnvS3PathStyle: "maybe"}, wantError: "true or false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolateAWSEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			got, err := S3EndpointFromEnv()
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("err = %v, want one mentioning %q", err, tc.wantError)
				}
				// A caller that degrades on read failures needs to tell a
				// configuration fault apart from a storage one.
				if !errors.Is(err, ErrS3EndpointConfig) {
					t.Errorf("err does not wrap ErrS3EndpointConfig: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
			if got.Host() != tc.wantHost || got.UseSSL() != tc.wantSSL {
				t.Fatalf("Host()=%q UseSSL()=%v, want %q/%v", got.Host(), got.UseSSL(), tc.wantHost, tc.wantSSL)
			}
		})
	}
}

// TestLoadAWSConfig_customEndpoint pins the SDK half of #1453: the endpoint
// lands in the config every client is built from, the region defaults so
// signing works with no AWS region anywhere, and the client carries the path
// style, which no SDK environment variable can set.
func TestLoadAWSConfig_customEndpoint(t *testing.T) {
	isolateAWSEnv(t)
	// Check the premise this test rests on rather than assuming it: if the
	// environment leaked a region in, the us-east-1 assertion below would pass
	// for the wrong reason (or fail for one).
	if base, err := LoadAWSConfig(context.Background(), ""); err != nil || base.Region != "" {
		t.Fatalf("isolation failed: region = %q, err = %v; the assertions below would be meaningless", base.Region, err)
	}
	t.Setenv(EnvS3Endpoint, "http://minio:9000")

	cfg, err := LoadAWSConfig(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BaseEndpoint == nil || *cfg.BaseEndpoint != "http://minio:9000" {
		t.Fatalf("BaseEndpoint = %v, want the custom endpoint", cfg.BaseEndpoint)
	}
	if cfg.Region != "us-east-1" {
		t.Fatalf("Region = %q, want the us-east-1 default for a custom endpoint", cfg.Region)
	}
	if opts := NewS3ClientFromConfig(cfg).Options(); !opts.UsePathStyle {
		t.Fatal("client is not path-style: MinIO would be reached at a virtual-hosted URL")
	}

	t.Setenv(EnvS3PathStyle, "false")
	if opts := NewS3ClientFromConfig(cfg).Options(); opts.UsePathStyle {
		t.Fatal("BINTRAIL_S3_PATH_STYLE=false did not turn path style off")
	}

	t.Setenv(EnvS3PathStyle, "")
	t.Setenv(EnvS3Endpoint, "not-a-url")
	if _, err := LoadAWSConfig(context.Background(), ""); err == nil {
		t.Fatal("an invalid endpoint must fail the load, never fall back to AWS")
	}
}

// TestLoadAWSConfig_sdkEndpointKeepsSDKSemantics: an operator who already
// configured the SDK's own endpoint variable keeps the SDK's behavior. bintrail
// mirrors the value to its DuckDB half but must not force path style (the SDK
// addresses virtual-hosted), must not fail on a value the SDK accepts, and must
// not suppress the IMDS region fallback — doing that would sign an EC2 role's
// requests for us-east-1 instead of the instance's region.
func TestLoadAWSConfig_sdkEndpointKeepsSDKSemantics(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://localstack:4566")

	cfg, err := LoadAWSConfig(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Region == "us-east-1" {
		t.Error("bintrail defaulted the region for an SDK-owned endpoint; the IMDS fallback is the SDK user's to keep")
	}
	if opts := NewS3ClientFromConfig(cfg).Options(); opts.UsePathStyle {
		t.Error("path style forced on an SDK-resolved endpoint: this changes addressing for a setup that worked before")
	}

	// The SDK routes AWS_ENDPOINT_URL_S3 itself, at client build time rather
	// than through cfg.BaseEndpoint.
	if opts := NewS3ClientFromConfig(cfg).Options(); opts.BaseEndpoint == nil || *opts.BaseEndpoint != "http://localstack:4566" {
		t.Errorf("client endpoint = %v, want the SDK's own routing preserved", opts.BaseEndpoint)
	}

	// The operator can still ask for path style explicitly.
	t.Setenv(EnvS3PathStyle, "true")
	cfg2, err := LoadAWSConfig(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if opts := NewS3ClientFromConfig(cfg2).Options(); !opts.UsePathStyle {
		t.Error("BINTRAIL_S3_PATH_STYLE=true did not apply to an SDK-resolved endpoint")
	}
}

// TestS3Endpoint_bintrailWinsOnBothHalves is the split-brain guard. The SDK's
// service-specific AWS_ENDPOINT_URL_S3 overrides cfg.BaseEndpoint at client
// build time, so with both variables set the SDK would write to one store
// while DuckDB (which follows BINTRAIL_S3_ENDPOINT) reads the other. Whichever
// variable wins, both halves must name the SAME store.
func TestS3Endpoint_bintrailWinsOnBothHalves(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv(EnvS3Endpoint, "http://bintrail-store:9000")
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://sdk-store:1")

	ep, err := S3EndpointFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadAWSConfig(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	opts := NewS3ClientFromConfig(cfg).Options()
	if opts.BaseEndpoint == nil {
		t.Fatal("client has no endpoint")
	}
	// The DuckDB half reads ep.URL; the SDK half reads the client endpoint.
	if *opts.BaseEndpoint != ep.URL {
		t.Fatalf("halves disagree: SDK writes to %q, DuckDB reads %q", *opts.BaseEndpoint, ep.URL)
	}
	if *opts.BaseEndpoint != "http://bintrail-store:9000" {
		t.Fatalf("client endpoint = %q, want bintrail's own variable to win", *opts.BaseEndpoint)
	}
}

// TestS3ClientOptions_pathStyleWithoutOurEndpoint: the AWS SDK has no
// shared-config setting for addressing style, so BINTRAIL_S3_PATH_STYLE is the
// only lever for an operator who configured endpoint_url in ~/.aws/config.
// Dropping it there sends uploads to bucket.host and fails on DNS, with a
// diagnostic that points at DNS rather than at the ignored variable.
func TestS3ClientOptions_pathStyleWithoutOurEndpoint(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv(EnvS3PathStyle, "true")

	cfg, err := LoadAWSConfig(context.Background(), "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	if opts := NewS3ClientFromConfig(cfg).Options(); !opts.UsePathStyle {
		t.Fatal("BINTRAIL_S3_PATH_STYLE=true was dropped for an endpoint bintrail did not resolve itself")
	}
}

// Without an endpoint nothing changes: no BaseEndpoint, no path style, and an
// explicit region is kept.
func TestLoadAWSConfig_awsUnchanged(t *testing.T) {
	isolateAWSEnv(t)
	cfg, err := LoadAWSConfig(context.Background(), "eu-west-1")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BaseEndpoint != nil {
		t.Fatalf("BaseEndpoint = %q, want none for AWS", *cfg.BaseEndpoint)
	}
	if cfg.Region != "eu-west-1" {
		t.Fatalf("Region = %q, want eu-west-1", cfg.Region)
	}
	if opts := NewS3ClientFromConfig(cfg).Options(); opts.UsePathStyle {
		t.Fatal("path style must stay off for AWS")
	}
}

// TestNewS3ClientFromConfig_warnsOnUnmirroredEndpoint: the SDK resolves a
// service-specific endpoint when the CLIENT is built, so cfg.BaseEndpoint is
// nil for exactly the shape that most needs the warning. Reading the client's
// own endpoint is what makes the check see it.
func TestNewS3ClientFromConfig_warnsOnUnmirroredEndpoint(t *testing.T) {
	isolateAWSEnv(t)
	// Written straight into the config: the point is that a client carrying an
	// endpoint bintrail did not resolve is detected, whatever put it there.
	cfg, err := LoadAWSConfig(context.Background(), "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	var logged strings.Builder
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(restore)
	warnUnmirroredOnce = sync.Once{}

	// A URL carrying credentials is REACHABLE here, and is why the value is
	// redacted: bintrail refuses that shape, which is what leaves the endpoint
	// unmirrored and lands execution in this very branch.
	NewS3ClientFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://accesskey:supersecret@minio.svc:9000")
	})

	out := logged.String()
	if !strings.Contains(out, "minio.svc:9000") {
		t.Errorf("no warning for an endpoint DuckDB will not follow: %s", out)
	}
	if strings.Contains(out, "supersecret") || strings.Contains(out, "accesskey") {
		t.Errorf("the warning echoes credentials from the endpoint: %s", out)
	}
}

// An explicit S3Config.Endpoint still wins over the environment's, for the
// callers that name the store themselves.
func TestNewS3Client_explicitEndpointWins(t *testing.T) {
	isolateAWSEnv(t)
	// BINTRAIL_S3_PATH_STYLE=false makes this discriminating: the explicit
	// block sets path style ON, so if the shared options were applied LAST
	// they would turn it back off. Without this the two orders coincide and
	// the "extra options win" contract is prose.
	t.Setenv(EnvS3Endpoint, "http://env-store:9000")
	t.Setenv(EnvS3PathStyle, "false")
	client, err := newS3Client(context.Background(), S3Config{Bucket: "b", Endpoint: "http://explicit:9000"})
	if err != nil {
		t.Fatal(err)
	}
	opts := client.Options()
	if opts.BaseEndpoint == nil || *opts.BaseEndpoint != "http://explicit:9000" || !opts.UsePathStyle {
		t.Fatalf("explicit endpoint not applied last: endpoint=%v pathStyle=%v", opts.BaseEndpoint, opts.UsePathStyle)
	}
}

// TestS3EndpointFromEnv_warnsOncePerProcess covers the one signal a user gets
// when an AWS SDK endpoint variable cannot be mirrored to DuckDB: the SDK will
// still honour it for writes, so the halves diverge silently and the reads land
// on AWS. Two properties, and the second is why this test exists at all:
//
//   - it fires, naming the variable, so the operator can act on it
//   - it fires ONCE, because S3EndpointFromEnv runs three times per client
//     construction and a per-call warning turns one typo into a repeating
//     stanza in the log of a long-running daemon
func TestS3EndpointFromEnv_warnsOncePerProcess(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_ENDPOINT_URL_S3", "minio.svc:9000") // no scheme: unusable for DuckDB

	var logged strings.Builder
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(restore)
	warnUnparseableOnce = sync.Once{}
	t.Cleanup(func() { warnUnparseableOnce = sync.Once{} })

	for range 3 {
		if _, err := S3EndpointFromEnv(); err != nil {
			// Lenient on the SDK's own variables: bintrail did not ask for
			// them, and failing here would break an upgrade for anyone whose
			// AWS_ENDPOINT_URL carries a path.
			t.Fatalf("an AWS variable was treated as fatal: %v", err)
		}
	}

	out := logged.String()
	if !strings.Contains(out, "AWS_ENDPOINT_URL_S3") {
		t.Errorf("no warning naming the variable DuckDB cannot follow: %s", out)
	}
	if n := strings.Count(out, "AWS_ENDPOINT_URL_S3"); n != 1 {
		t.Errorf("warned %d times for one static misconfiguration: %s", n, out)
	}
}
