package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
