package main

import (
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
