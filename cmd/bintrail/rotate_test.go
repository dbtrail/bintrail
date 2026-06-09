package main

import (
	"strings"
	"testing"
)

// ─── cobra command wiring ────────────────────────────────────────────────────

func TestRotateCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "rotate" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'rotate' command to be registered under rootCmd")
	}
}

func TestRotateCmd_indexDSN_required(t *testing.T) {
	flag := rotateCmd.Flag("index-dsn")
	if flag == nil {
		t.Fatal("flag --index-dsn not registered on rotateCmd")
	}
	if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("flag --index-dsn is not marked required on rotateCmd")
	}
}

func TestRotateCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"index-dsn", "retain", "add-future", "no-replace",
		"archive-dir", "archive-compression", "archive-s3", "archive-s3-region",
		"retry",
	} {
		if rotateCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on rotateCmd", name)
		}
	}
}

func TestRotateCmd_retryDefaultFalse(t *testing.T) {
	f := rotateCmd.Flag("retry")
	if f == nil {
		t.Fatal("flag --retry not registered")
	}
	if f.DefValue != "false" {
		t.Errorf("expected default retry=false, got %q", f.DefValue)
	}
}

// ─── runRotate validation (no DB required) ──────────────────────────────────

func TestRunRotate_archiveS3RequiresArchiveDir(t *testing.T) {
	savedRetain, savedAdd, savedDSN, savedArchiveDir, savedArchiveS3 :=
		rotRetain, rotAddFuture, rotIndexDSN, rotArchiveDir, rotArchiveS3
	t.Cleanup(func() {
		rotRetain = savedRetain
		rotAddFuture = savedAdd
		rotIndexDSN = savedDSN
		rotArchiveDir = savedArchiveDir
		rotArchiveS3 = savedArchiveS3
	})

	rotRetain = "7d"
	rotAddFuture = 0
	rotIndexDSN = "user:pass@tcp(localhost:3306)/binlog_index"
	rotArchiveDir = "" // not set
	rotArchiveS3 = "s3://my-bucket/archives/"

	err := runRotate(rotateCmd, nil)
	if err == nil {
		t.Fatal("expected error when --archive-s3 is set without --archive-dir, got nil")
	}
	if !strings.Contains(err.Error(), "--archive-s3 requires --archive-dir") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunRotate_noFlagsError(t *testing.T) {
	savedRetain, savedAdd, savedNoReplace := rotRetain, rotAddFuture, rotNoReplace
	t.Cleanup(func() { rotRetain = savedRetain; rotAddFuture = savedAdd; rotNoReplace = savedNoReplace })

	rotRetain = ""
	rotAddFuture = 0

	err := runRotate(rotateCmd, nil)
	if err == nil {
		t.Fatal("expected error when neither --retain nor --add-future is set, got nil")
	}
	if !strings.Contains(err.Error(), "--retain or --add-future") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunRotate_invalidRetain(t *testing.T) {
	savedRetain, savedAdd, savedNoReplace := rotRetain, rotAddFuture, rotNoReplace
	t.Cleanup(func() { rotRetain = savedRetain; rotAddFuture = savedAdd; rotNoReplace = savedNoReplace })

	rotRetain = "5weeks" // invalid unit
	rotAddFuture = 0

	err := runRotate(rotateCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --retain value, got nil")
	}
	if !strings.Contains(err.Error(), "--retain") {
		t.Errorf("expected '--retain' in error, got: %v", err)
	}
}

func TestRunRotate_missingDBName(t *testing.T) {
	savedRetain, savedAdd, savedDSN := rotRetain, rotAddFuture, rotIndexDSN
	t.Cleanup(func() { rotRetain = savedRetain; rotAddFuture = savedAdd; rotIndexDSN = savedDSN })

	rotRetain = ""
	rotAddFuture = 5
	rotIndexDSN = "user:pass@tcp(localhost:3306)/" // valid DSN syntax but no database name

	err := runRotate(rotateCmd, nil)
	if err == nil {
		t.Fatal("expected error when DSN has no database name, got nil")
	}
	if !strings.Contains(err.Error(), "--index-dsn must include a database name") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ─── cobra flag defaults ────────────────────────────────────────────────────

func TestRotateCmd_defaults(t *testing.T) {
	cases := []struct{ flag, want string }{
		{"add-future", "0"},
		{"no-replace", "false"},
		{"archive-compression", "zstd"},
	}
	for _, tc := range cases {
		f := rotateCmd.Flag(tc.flag)
		if f == nil {
			t.Fatalf("flag --%s not registered", tc.flag)
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

func TestRotateCmd_emptyStringDefaults(t *testing.T) {
	for _, name := range []string{"retain", "archive-dir", "archive-s3", "archive-s3-region"} {
		f := rotateCmd.Flag(name)
		if f == nil {
			t.Fatalf("flag --%s not registered", name)
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

// ─── runRotate positive-path guard tests ────────────────────────────────────────

// TestRunRotate_addFutureAlonePassesFirstGuard verifies that providing only
// --add-future (no --retain) passes the "at least one of" guard.
func TestRunRotate_addFutureAlonePassesFirstGuard(t *testing.T) {
	savedRetain, savedAdd := rotRetain, rotAddFuture
	t.Cleanup(func() { rotRetain = savedRetain; rotAddFuture = savedAdd })

	rotRetain = ""
	rotAddFuture = 5

	err := runRotate(rotateCmd, nil) // fails later at DSN parse or config.Connect
	if err != nil && strings.Contains(err.Error(), "--retain or --add-future") {
		t.Errorf("first guard should not fire when --add-future is set, got: %v", err)
	}
}

// TestRunRotate_retainAlonePassesFirstGuard verifies that providing only
// --retain (no --add-future) passes both the "at least one of" guard and
// the retain-parse check.
func TestRunRotate_retainAlonePassesFirstGuard(t *testing.T) {
	savedRetain, savedAdd, savedDSN := rotRetain, rotAddFuture, rotIndexDSN
	t.Cleanup(func() { rotRetain = savedRetain; rotAddFuture = savedAdd; rotIndexDSN = savedDSN })

	rotRetain = "7d"
	rotAddFuture = 0
	rotIndexDSN = "user:pass@tcp(localhost:3306)/binlog_index"

	err := runRotate(rotateCmd, nil) // fails later at config.Connect — that's fine
	if err != nil && strings.Contains(err.Error(), "--retain or --add-future") {
		t.Errorf("first guard should not fire when --retain is set, got: %v", err)
	}
	if err != nil && strings.Contains(err.Error(), "--retain:") {
		t.Errorf("retain parse should accept '7d', got: %v", err)
	}
}

// ─── daemon flag wiring ─────────────────────────────────────────────────────────

func TestRotateCmd_daemonFlagRegistered(t *testing.T) {
	f := rotateCmd.Flag("daemon")
	if f == nil {
		t.Fatal("flag --daemon not registered on rotateCmd")
	}
	if f.DefValue != "false" {
		t.Errorf("expected default false, got %q", f.DefValue)
	}
}

func TestRotateCmd_intervalDefault(t *testing.T) {
	f := rotateCmd.Flag("interval")
	if f == nil {
		t.Fatal("flag --interval not registered on rotateCmd")
	}
	if f.DefValue != "1h" {
		t.Errorf("expected default 1h, got %q", f.DefValue)
	}
}

func TestRunRotate_daemonInvalidInterval(t *testing.T) {
	savedRetain, savedAdd, savedDSN, savedDaemon, savedInterval :=
		rotRetain, rotAddFuture, rotIndexDSN, rotDaemon, rotInterval
	t.Cleanup(func() {
		rotRetain = savedRetain
		rotAddFuture = savedAdd
		rotIndexDSN = savedDSN
		rotDaemon = savedDaemon
		rotInterval = savedInterval
	})

	rotRetain = "7d"
	rotAddFuture = 0
	rotIndexDSN = "user:pass@tcp(localhost:3306)/binlog_index"
	rotDaemon = true
	rotInterval = "notaduration"

	err := runRotate(rotateCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --interval, got nil")
	}
	if !strings.Contains(err.Error(), "--interval") {
		t.Errorf("expected '--interval' in error, got: %v", err)
	}
}
