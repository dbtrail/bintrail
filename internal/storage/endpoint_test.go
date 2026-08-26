package storage

import (
	"context"
	"strings"
	"testing"
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
			want:     S3Endpoint{URL: "http://minio:9000", PathStyle: true},
			wantHost: "minio:9000",
		},
		{
			name:     "Wasabi over https, virtual-hosted on request",
			env:      map[string]string{EnvS3Endpoint: "https://s3.wasabisys.com", EnvS3PathStyle: "false"},
			want:     S3Endpoint{URL: "https://s3.wasabisys.com", PathStyle: false},
			wantHost: "s3.wasabisys.com",
			wantSSL:  true,
		},
		{
			name:     "the SDK's own variable is honored as a fallback",
			env:      map[string]string{"AWS_ENDPOINT_URL_S3": "http://localstack:4566"},
			want:     S3Endpoint{URL: "http://localstack:4566", PathStyle: true},
			wantHost: "localstack:4566",
		},
		{
			name:     "BINTRAIL_S3_ENDPOINT wins over the SDK variable",
			env:      map[string]string{EnvS3Endpoint: "http://minio:9000", "AWS_ENDPOINT_URL_S3": "http://other:1"},
			want:     S3Endpoint{URL: "http://minio:9000", PathStyle: true},
			wantHost: "minio:9000",
		},
		{
			name:     "a trailing slash is not a path",
			env:      map[string]string{EnvS3Endpoint: "http://minio:9000/"},
			want:     S3Endpoint{URL: "http://minio:9000", PathStyle: true},
			wantHost: "minio:9000",
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

// An explicit S3Config.Endpoint still wins over the environment's, for the
// callers that name the store themselves.
func TestNewS3Client_explicitEndpointWins(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv(EnvS3Endpoint, "http://env-store:9000")
	client, err := newS3Client(context.Background(), S3Config{Bucket: "b", Endpoint: "http://explicit:9000"})
	if err != nil {
		t.Fatal(err)
	}
	opts := client.Options()
	if opts.BaseEndpoint == nil || *opts.BaseEndpoint != "http://explicit:9000" || !opts.UsePathStyle {
		t.Fatalf("explicit endpoint not applied: %+v", opts.BaseEndpoint)
	}
}
