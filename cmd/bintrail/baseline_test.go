package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestBaselineCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "baseline" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'baseline' command to be registered under rootCmd")
	}
}

func TestBaselineCmd_requiredFlags(t *testing.T) {
	for _, name := range []string{"input", "output"} {
		flag := baselineCmd.Flag(name)
		if flag == nil {
			t.Fatalf("flag --%s not registered", name)
		}
		if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("flag --%s is not marked required", name)
		}
	}
}

func TestBaselineCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"input", "output", "timestamp", "tables",
		"compression", "row-group-size", "upload", "upload-region",
		"retry", "encrypt", "encrypt-key",
	} {
		if baselineCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on baselineCmd", name)
		}
	}
}

func TestBaselineCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"compression", "zstd"},
		{"row-group-size", "500000"},
	}
	for _, tc := range cases {
		f := baselineCmd.Flag(tc.flag)
		if f == nil {
			t.Fatalf("flag --%s not registered", tc.flag)
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

func TestBaselineCmd_emptyStringDefaults(t *testing.T) {
	for _, name := range []string{"timestamp", "tables", "upload", "upload-region"} {
		f := baselineCmd.Flag(name)
		if f == nil {
			t.Errorf("flag --%s not registered", name)
			continue
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

func TestBaselineCmd_retryDefaultFalse(t *testing.T) {
	f := baselineCmd.Flag("retry")
	if f == nil {
		t.Fatal("flag --retry not registered")
	}
	if f.DefValue != "false" {
		t.Errorf("expected default retry=false, got %q", f.DefValue)
	}
}

// ─── parseTableFilter ─────────────────────────────────────────────────────────

func TestParseTableFilter(t *testing.T) {
	cases := []struct {
		input string
		want  []string
	}{
		{"", nil},
		{"mydb.orders", []string{"mydb.orders"}},
		{"mydb.orders, mydb.users", []string{"mydb.orders", "mydb.users"}},
		{"  mydb.orders  ,  ", []string{"mydb.orders"}},
	}
	for _, tc := range cases {
		got := parseTableFilter(tc.input)
		if len(got) != len(tc.want) {
			t.Errorf("parseTableFilter(%q) = %v, want %v", tc.input, got, tc.want)
			continue
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Errorf("parseTableFilter(%q)[%d] = %q, want %q", tc.input, i, got[i], tc.want[i])
			}
		}
	}
}

func TestRunBaselineTimestampParsing(t *testing.T) {
	origInput, origOutput, origTS := bslInput, bslOutput, bslTimestamp
	t.Cleanup(func() {
		bslInput = origInput
		bslOutput = origOutput
		bslTimestamp = origTS
	})

	// Use real directories so Run gets past DiscoverTables with 0 tables found,
	// avoiding any interaction with the filesystem beyond what's needed.
	bslInput = t.TempDir()
	bslOutput = t.TempDir()

	// Invalid format must return the "expected ISO 8601" error before calling Run.
	bslTimestamp = "not-a-timestamp"
	if err := runBaseline(baselineCmd, nil); err == nil || !strings.Contains(err.Error(), "expected ISO 8601") {
		t.Errorf("invalid timestamp: want ISO 8601 error, got: %v", err)
	}

	// Valid formats: each should parse without the ISO 8601 error.
	// With an empty input dir, DiscoverTables returns 0 tables → Run returns
	// Stats{} with no error, so runBaseline succeeds overall.
	validCases := []struct {
		name string
		ts   string
	}{
		{"RFC3339", "2025-02-28T00:00:00Z"},
		{"T-no-TZ", "2025-02-28T00:00:00"},
		{"space-fmt", "2025-02-28 00:00:00"},
	}
	for _, tc := range validCases {
		bslTimestamp = tc.ts
		err := runBaseline(baselineCmd, nil)
		if err != nil && strings.Contains(err.Error(), "expected ISO 8601") {
			t.Errorf("%s: timestamp should parse without ISO 8601 error, got: %v", tc.name, err)
		}
	}
}

func TestParseS3URL(t *testing.T) {
	cases := []struct {
		input      string
		wantBucket string
		wantPrefix string
		wantErr    bool
	}{
		{"s3://my-bucket", "my-bucket", "", false},
		{"s3://my-bucket/", "my-bucket", "", false},
		{"s3://my-bucket/baselines/", "my-bucket", "baselines/", false},
		{"s3://my-bucket/prefix/sub", "my-bucket", "prefix/sub", false},
		{"http://my-bucket/prefix", "", "", true},
		{"s3://", "", "", true},
	}
	for _, tc := range cases {
		bucket, prefix, err := parseS3URL(tc.input)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseS3URL(%q) error = %v, wantErr %v", tc.input, err, tc.wantErr)
			continue
		}
		if !tc.wantErr {
			if bucket != tc.wantBucket {
				t.Errorf("parseS3URL(%q) bucket = %q, want %q", tc.input, bucket, tc.wantBucket)
			}
			if prefix != tc.wantPrefix {
				t.Errorf("parseS3URL(%q) prefix = %q, want %q", tc.input, prefix, tc.wantPrefix)
			}
		}
	}
}

// TestParseTableFilter_onlyCommasAndSpaces verifies that a string containing
// only commas and whitespace (no actual table names) returns nil — the same
// result as an empty string, but via a different code path (SplitSeq iterates
// but every trimmed part is "").
func TestParseTableFilter_onlyCommasAndSpaces(t *testing.T) {
	for _, input := range []string{",", "  ,  ", ", , ,", " , "} {
		if got := parseTableFilter(input); got != nil {
			t.Errorf("parseTableFilter(%q) = %v, want nil", input, got)
		}
	}
}

// TestParseTableFilter_threeEntries verifies the split loop handles n>2 tables.
func TestParseTableFilter_threeEntries(t *testing.T) {
	got := parseTableFilter("db.a, db.b, db.c")
	want := []string{"db.a", "db.b", "db.c"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] expected %q, got %q", i, want[i], got[i])
		}
	}
}

// TestParseTableFilter_trailingComma verifies that a trailing comma does not
// produce an empty string entry — the TrimSpace+empty-check drops it.
func TestParseTableFilter_trailingComma(t *testing.T) {
	got := parseTableFilter("db.orders,")
	if len(got) != 1 || got[0] != "db.orders" {
		t.Errorf("expected [db.orders], got %v", got)
	}
}

// TestParseS3URL_emptyBucketWithPath verifies that s3:///path (three slashes,
// empty bucket name) is rejected — strings.Cut on "/" gives bucket="" which
// hits the "bucket name is empty" guard.
func TestParseS3URL_emptyBucketWithPath(t *testing.T) {
	_, _, err := parseS3URL("s3:///some/path")
	if err == nil {
		t.Fatal("expected error for s3:///some/path (empty bucket), got nil")
	}
	if !strings.Contains(err.Error(), "bucket") {
		t.Errorf("expected 'bucket' in error, got: %v", err)
	}
}

// TestParseS3URL_prefixRetainsTrailingSlash verifies that a prefix that ends
// with "/" is returned as-is (the function does not strip it — callers like
// uploadBaselineToS3 handle normalisation with TrimSuffix).
func TestParseS3URL_prefixRetainsTrailingSlash(t *testing.T) {
	_, prefix, err := parseS3URL("s3://my-bucket/baselines/2026/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if prefix != "baselines/2026/" {
		t.Errorf("expected prefix %q, got %q", "baselines/2026/", prefix)
	}
}

func TestRunBaselineMissingInput(t *testing.T) {
	origInput, origOutput, origTS := bslInput, bslOutput, bslTimestamp
	t.Cleanup(func() {
		bslInput = origInput
		bslOutput = origOutput
		bslTimestamp = origTS
	})

	// Non-existent input directory should produce an error about reading the dir.
	bslInput = "/nonexistent/path-does-not-exist"
	bslOutput = t.TempDir()
	bslTimestamp = "2025-01-01T00:00:00Z" // valid timestamp, skips metadata parsing

	if err := runBaseline(baselineCmd, nil); err == nil {
		t.Error("expected error for nonexistent input directory, got nil")
	}
}

// TestRunBaseline_invalidUploadURL verifies that an invalid --upload value
// (not starting with s3://) is caught by parseS3URL inside uploadBaselineToS3
// and surfaces as an "S3 upload" error — without requiring AWS credentials.
// baseline.Run with an empty input dir and a valid timestamp succeeds (0 tables),
// so execution reaches the upload block before the URL validation fires.
func TestRunBaseline_invalidUploadURL(t *testing.T) {
	origInput, origOutput, origTS, origUpload :=
		bslInput, bslOutput, bslTimestamp, bslUpload
	t.Cleanup(func() {
		bslInput = origInput
		bslOutput = origOutput
		bslTimestamp = origTS
		bslUpload = origUpload
	})

	bslInput = t.TempDir() // empty dir → 0 tables → baseline.Run succeeds
	bslOutput = t.TempDir()
	bslTimestamp = "2025-02-28T00:00:00Z"
	bslUpload = "http://not-s3.example.com/bucket" // invalid: not s3://

	err := runBaseline(baselineCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --upload URL, got nil")
	}
	if !strings.Contains(err.Error(), "S3 upload") {
		t.Errorf("expected 'S3 upload' in error, got: %v", err)
	}
}

// TestRunBaseline_emptyTimestamp verifies that when --timestamp is omitted,
// runBaseline delegates timestamp resolution to baseline.Run → ParseMetadata.
// A temp dir with no metadata file causes a "parse mydumper metadata" error —
// proving the empty-timestamp code path is reached.
func TestRunBaseline_emptyTimestamp(t *testing.T) {
	origInput, origOutput, origTS := bslInput, bslOutput, bslTimestamp
	t.Cleanup(func() {
		bslInput = origInput
		bslOutput = origOutput
		bslTimestamp = origTS
	})

	bslInput = t.TempDir() // valid dir but no metadata file
	bslOutput = t.TempDir()
	bslTimestamp = "" // triggers ParseMetadata inside baseline.Run

	err := runBaseline(baselineCmd, nil)
	if err == nil {
		t.Fatal("expected error when metadata file is absent, got nil")
	}
	if !strings.Contains(err.Error(), "metadata") {
		t.Errorf("expected 'metadata' in error, got: %v", err)
	}
}

// ─── encryption ───────────────────────────────────────────────────────────────

func TestBaselineCmd_encryptDefaultFalse(t *testing.T) {
	f := baselineCmd.Flag("encrypt")
	if f == nil {
		t.Fatal("flag --encrypt not registered")
	}
	if f.DefValue != "false" {
		t.Errorf("expected default encrypt=false, got %q", f.DefValue)
	}
}

func TestDecryptDumpFiles_noEncFiles(t *testing.T) {
	dir := t.TempDir()
	// Create a non-.enc file to verify it's left alone.
	os.WriteFile(filepath.Join(dir, "test.sql"), []byte("data"), 0o644)

	keyPath := filepath.Join(dir, "test.key")
	os.WriteFile(keyPath, []byte("testkey"), 0o600)

	cleanup, err := decryptDumpFiles(dir, keyPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup()
	// Should log a warning but not fail.
}

func TestDecryptDumpFiles_roundTrip(t *testing.T) {
	// Skip if openssl is not available.
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl not available on $PATH")
	}

	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	os.WriteFile(keyPath, []byte("testpassphrase"), 0o600)

	// Encrypt a file using the same openssl command that dump would use.
	plaintext := "CREATE TABLE test (id INT PRIMARY KEY);\n"
	plainFile := filepath.Join(dir, "mydb.test-schema.sql")
	os.WriteFile(plainFile, []byte(plaintext), 0o644)

	encFile := plainFile + ".enc"
	cmd := exec.Command("openssl", "enc", "-aes-256-cbc", "-pbkdf2",
		"-pass", "file:"+keyPath, "-in", plainFile, "-out", encFile)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("encrypt failed: %v\n%s", err, out)
	}

	// Remove the plain file to simulate what mydumper would produce.
	os.Remove(plainFile)

	// Decrypt.
	cleanup, err := decryptDumpFiles(dir, keyPath)
	if err != nil {
		t.Fatalf("decryptDumpFiles failed: %v", err)
	}

	// Verify decrypted file exists and has correct content.
	data, err := os.ReadFile(plainFile)
	if err != nil {
		t.Fatalf("decrypted file not found: %v", err)
	}
	if string(data) != plaintext {
		t.Errorf("decrypted content mismatch: got %q, want %q", string(data), plaintext)
	}

	// Cleanup should remove the decrypted file.
	cleanup()
	if _, err := os.Stat(plainFile); !os.IsNotExist(err) {
		t.Error("cleanup should have removed the decrypted file")
	}
}
