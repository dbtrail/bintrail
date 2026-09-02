package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func newStorageServer(t *testing.T) *Server {
	t.Helper()
	s := &Server{token: "t", cm: newConnManager(nil, false)}
	s.mux = s.buildHandler()
	return s
}

// TestStorageAPI_reportsPresenceNeverValues: the endpoint reports which
// credential signals are set (plus the two non-secret names) and must never
// serialize the key material itself.
func TestStorageAPI_reportsPresenceNeverValues(t *testing.T) {
	const secretKey = "AKIAEXAMPLESECRETID"
	tmp := t.TempDir()
	credPath := filepath.Join(tmp, "credentials")
	if err := os.WriteFile(credPath, []byte("[default]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", secretKey)
	t.Setenv("AWS_PROFILE", "prod-admin")
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credPath)
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(tmp, "missing"))
	t.Setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")

	srv := newStorageServer(t)
	rec, body := doServersReq(t, srv, "GET", "/api/storage", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if strings.Contains(string(body), secretKey) {
		t.Fatalf("response leaked the access key value: %s", body)
	}
	var got storageInfoResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.AWS.AccessKeyEnv || got.AWS.Profile != "prod-admin" || got.AWS.RegionEnv != "eu-west-1" {
		t.Fatalf("aws = %+v, want access_key_env + profile + region from env", got.AWS)
	}
	if !got.AWS.SharedConfig {
		t.Fatal("shared_config = false, want true (credentials file exists)")
	}
	if got.AWS.ContainerCreds || got.AWS.WebIdentity {
		t.Fatalf("aws = %+v, want no container/web-identity signals", got.AWS)
	}
}

// TestStorageAPI_webIdentityIsProbedNotAsserted pins the #1534 shapes: the
// variable alone used to render "Using an IAM role" while a typo'd path, an
// unmounted projected-token volume, or a missing AWS_ROLE_ARN meant nothing
// signs — on the page an operator opens precisely because S3 is not working.
func TestStorageAPI_webIdentityIsProbedNotAsserted(t *testing.T) {
	tmp := t.TempDir()
	token := filepath.Join(tmp, "token")
	if err := os.WriteFile(token, []byte("jwt"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, env := range []string{"AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"} {
		t.Setenv(env, "")
	}
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(tmp, "missing"))
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(tmp, "missing2"))

	srv := newStorageServer(t)
	read := func() awsCredsDTO {
		t.Helper()
		rec, body := doServersReq(t, srv, "GET", "/api/storage", "")
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s", rec.Code, body)
		}
		var got storageInfoResponse
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		return got.AWS
	}

	// The healthy shape: token readable, role named.
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/app")
	if aws := read(); !aws.WebIdentity || !aws.WebIdentityTokenReadable || !aws.WebIdentityRoleArn {
		t.Fatalf("healthy IRSA = %+v, want all three signals", aws)
	}

	// The typo'd/unmounted path: variable set, file absent. This is the shape
	// that used to read as "Using an IAM role".
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", filepath.Join(tmp, "nope"))
	if aws := read(); !aws.WebIdentity || aws.WebIdentityTokenReadable {
		t.Fatalf("missing token = %+v, want web_identity set and token NOT readable", aws)
	}

	// A token without a role: the provider needs both.
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
	t.Setenv("AWS_ROLE_ARN", "")
	if aws := read(); !aws.WebIdentityTokenReadable || aws.WebIdentityRoleArn {
		t.Fatalf("no role arn = %+v, want readable token and role_arn false", aws)
	}

	// The projected-volume mount pointed at instead of the token inside it.
	// os.Open succeeds on a directory, so a probe built on Open alone reports
	// this as readable while the SDK's own read fails with EISDIR — the same
	// unearned claim #1534 exists to retire.
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tmp)
	if aws := read(); !aws.WebIdentity || aws.WebIdentityTokenReadable {
		t.Fatalf("directory = %+v, want web_identity set and token NOT readable", aws)
	}

	// A token the daemon may not read. It stats fine (stat comes from the
	// directory's execute bit), so this is the case the OPEN half of the probe
	// exists for: without it the function reduces to a stat and every other
	// case here still passes.
	locked := filepath.Join(tmp, "locked-token")
	if err := os.WriteFile(locked, []byte("jwt"), 0o000); err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() == 0 {
		t.Log("running as root: skipping the unreadable-mode case (root opens a mode-000 file)")
	} else {
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", locked)
		if aws := read(); !aws.WebIdentity || aws.WebIdentityTokenReadable {
			t.Fatalf("mode-000 token = %+v, want web_identity set and token NOT readable", aws)
		}
	}

	// A Kubernetes projected token is a symlink farm (token -> ..data/token),
	// so the probe must follow symlinks: Lstat here would report a healthy
	// IRSA mount as unreadable.
	link := filepath.Join(tmp, "linked-token")
	if err := os.Symlink(token, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", link)
	if aws := read(); !aws.WebIdentityTokenReadable {
		t.Fatalf("symlinked token = %+v, want readable (projected tokens are symlinks)", aws)
	}
}

// TestStorageAPI_webIdentityFifoDoesNotBlock: os.Open on a FIFO with no writer
// blocks forever, which would strand this handler's goroutine (and its
// response) on every GET /api/storage. The probe stats first, so a FIFO is
// answered immediately. Asserted with a deadline because the regression is a
// HANG, not a wrong answer — a bare call would wedge the test binary instead
// of failing it.
func TestStorageAPI_webIdentityFifoDoesNotBlock(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo unsupported here: %v", err)
	}
	done := make(chan bool, 1)
	go func() { done <- webIdentityTokenReadable(fifo) }()
	select {
	case got := <-done:
		if got {
			t.Fatal("a FIFO is not a readable token file")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("webIdentityTokenReadable blocked on a FIFO with no writer")
	}
}

func TestStorageAPI_nothingSet(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(tmp, "nope"))
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(tmp, "also-nope"))
	t.Setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")

	srv := newStorageServer(t)
	rec, body := doServersReq(t, srv, "GET", "/api/storage", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got storageInfoResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.AWS.AccessKeyEnv || got.AWS.Profile != "" || got.AWS.RegionEnv != "" || got.AWS.SharedConfig {
		t.Fatalf("aws = %+v, want all signals absent", got.AWS)
	}
}

// TestStorageAPI_stagingShare (#1448): a daemon that can build .sql backups
// reports what its staging holds, with the total and each build's server
// name resolved from the registry; a console without the exporter reports
// no staging at all (there is none).
func TestStorageAPI_stagingShare(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	stub := &stubSQLExporter{}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())
	stub.staged = SQLExportStagingInfo{Dir: "/staging/sql-export", TTL: 4 * time.Hour, Builds: []SQLExportStagedBuild{
		{ServerID: id, State: "succeeded", At: "2026-06-10T11:00:00Z", ExpiresAt: "2026-06-10T16:00:00Z", Bytes: 1500, BytesKnown: true},
		{ServerID: "gone", State: "running", Bytes: 500, BytesKnown: true},
		{ServerID: "stuck", State: "failed", Bytes: 7, BytesKnown: false, StagingError: "could not remove the staged files (build failed): permission denied"},
	}}

	rec, body := doServersReq(t, srv, "GET", "/api/storage", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got storageInfoResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Staging == nil {
		t.Fatalf("staging absent with an exporter wired: %s", body)
	}
	if got.Staging.Dir != "/staging/sql-export" || got.Staging.TTLHours != 4 || got.Staging.Bytes != 2000 {
		t.Fatalf("staging = %+v, want the dir, 4h and the 2000-byte total", got.Staging)
	}
	if len(got.Staging.Builds) != 3 {
		t.Fatalf("builds = %+v, want all three", got.Staging.Builds)
	}
	// An unmeasurable build is excluded from the total (2000, not 2007) and
	// says so, never "0 B".
	if b := got.Staging.Builds[2]; b.BytesKnown || b.StagingError == "" || b.State != "failed" {
		t.Fatalf("build[2] = %+v, want bytes_known=false with its staging error and state", b)
	}
	if !strings.Contains(string(body), `"bytes_known":false`) {
		t.Fatalf("bytes_known must be serialized even when false: %s", body)
	}
	if b := got.Staging.Builds[0]; b.ServerName != "wp" || b.State != "succeeded" || b.ExpiresAt != "2026-06-10T16:00:00Z" || b.Bytes != 1500 {
		t.Fatalf("build[0] = %+v, want the registry name, state, deadline and size", b)
	}
	if b := got.Staging.Builds[1]; b.ServerName != "" || b.ServerID != "gone" {
		t.Fatalf("build[1] = %+v: a deleted server keeps its id and no name", b)
	}

	// No exporter: the key is absent, not an empty object the card would
	// render as "nothing staged".
	off := newStorageServer(t)
	rec, body = doServersReq(t, off, "GET", "/api/storage", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if strings.Contains(string(body), `"staging"`) {
		t.Fatalf("staging reported without an exporter: %s", body)
	}
}
