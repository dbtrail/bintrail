package main

import (
	"os"
	"strings"
	"testing"
)

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestUploadCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "upload" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'upload' command to be registered under rootCmd")
	}
}

func TestUploadCmd_requiredFlags(t *testing.T) {
	for _, name := range []string{"source", "destination"} {
		flag := uploadCmd.Flag(name)
		if flag == nil {
			t.Fatalf("flag --%s not registered", name)
		}
		if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("flag --%s is not marked required", name)
		}
	}
}

func TestUploadCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"source", "destination", "region", "index-dsn",
		"format", "retry",
	} {
		if uploadCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on uploadCmd", name)
		}
	}
}

func TestUploadCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"format", "text"},
		{"retry", "false"},
		{"region", ""},
		{"index-dsn", ""},
	}
	for _, tc := range cases {
		f := uploadCmd.Flag(tc.flag)
		if f == nil {
			t.Fatalf("flag --%s not registered", tc.flag)
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

// ─── runUpload validation ─────────────────────────────────────────────────────

func TestRunUpload_invalidFormat(t *testing.T) {
	orig := uplFormat
	t.Cleanup(func() { uplFormat = orig })

	uplFormat = "xml"
	err := runUpload(uploadCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "invalid --format") {
		t.Errorf("expected 'invalid --format' error, got: %v", err)
	}
}

func TestRunUpload_invalidDestination(t *testing.T) {
	origFmt, origDst := uplFormat, uplDestination
	t.Cleanup(func() {
		uplFormat = origFmt
		uplDestination = origDst
	})

	uplFormat = "text"
	uplDestination = "http://not-s3/bucket"
	err := runUpload(uploadCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "invalid --destination") {
		t.Errorf("expected 'invalid --destination' error, got: %v", err)
	}
}

func TestRunUpload_missingSourceDir(t *testing.T) {
	origFmt, origDst, origSrc := uplFormat, uplDestination, uplSource
	t.Cleanup(func() {
		uplFormat = origFmt
		uplDestination = origDst
		uplSource = origSrc
	})

	uplFormat = "text"
	uplDestination = "s3://my-bucket/prefix/"
	uplSource = "/nonexistent/path-does-not-exist"
	err := runUpload(uploadCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "--source") {
		t.Errorf("expected '--source' error, got: %v", err)
	}
}

func TestRunUpload_sourceNotDir(t *testing.T) {
	origFmt, origDst, origSrc := uplFormat, uplDestination, uplSource
	t.Cleanup(func() {
		uplFormat = origFmt
		uplDestination = origDst
		uplSource = origSrc
	})

	// Create a temp file (not a directory) to use as source.
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/not-a-dir.txt"
	if err := writeTestFile(tmpFile, "hello"); err != nil {
		t.Fatal(err)
	}

	uplFormat = "text"
	uplDestination = "s3://my-bucket/prefix/"
	uplSource = tmpFile
	err := runUpload(uploadCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Errorf("expected 'not a directory' error, got: %v", err)
	}
}

// ─── parseArchivePath ─────────────────────────────────────────────────────────

func TestParseArchivePath(t *testing.T) {
	cases := []struct {
		name       string
		path       string
		wantID     string
		wantPart   string
	}{
		{
			name:     "valid hive path",
			path:     "/data/archives/bintrail_id=3e11fa47-1234-5678-9abc-def012345678/event_date=2026-03-01/event_hour=14/events.parquet",
			wantID:   "3e11fa47-1234-5678-9abc-def012345678",
			wantPart: "p_2026030114",
		},
		{
			name:     "hour 00",
			path:     "/data/bintrail_id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/event_date=2026-01-15/event_hour=00/events.parquet",
			wantID:   "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
			wantPart: "p_2026011500",
		},
		{
			name:     "hour 23",
			path:     "bintrail_id=11111111-2222-3333-4444-555555555555/event_date=2026-12-31/event_hour=23/events.parquet",
			wantID:   "11111111-2222-3333-4444-555555555555",
			wantPart: "p_2026123123",
		},
		{
			name:     "no match - baseline path",
			path:     "/data/baselines/2026-03-01T00-00-00Z/mydb/orders.parquet",
			wantID:   "",
			wantPart: "",
		},
		{
			name:     "no match - non-parquet",
			path:     "/data/bintrail_id=3e11fa47-1234-5678-9abc-def012345678/event_date=2026-03-01/event_hour=14/readme.txt",
			wantID:   "",
			wantPart: "",
		},
		{
			name:     "no match - missing bintrail_id",
			path:     "/data/event_date=2026-03-01/event_hour=14/events.parquet",
			wantID:   "",
			wantPart: "",
		},
		{
			name:     "no match - invalid hour 25",
			path:     "/data/bintrail_id=3e11fa47-1234-5678-9abc-def012345678/event_date=2026-03-01/event_hour=25/events.parquet",
			wantID:   "",
			wantPart: "",
		},
		{
			name:     "no match - invalid hour 99",
			path:     "/data/bintrail_id=3e11fa47-1234-5678-9abc-def012345678/event_date=2026-03-01/event_hour=99/events.parquet",
			wantID:   "",
			wantPart: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotID, gotPart := parseArchivePath(tc.path)
			if gotID != tc.wantID {
				t.Errorf("bintrailID = %q, want %q", gotID, tc.wantID)
			}
			if gotPart != tc.wantPart {
				t.Errorf("partName = %q, want %q", gotPart, tc.wantPart)
			}
		})
	}
}

// writeTestFile is a helper for creating test files.
func writeTestFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
