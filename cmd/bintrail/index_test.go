package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestIndexCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "index" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'index' command to be registered under rootCmd")
	}
}

func TestIndexCmd_requiredFlags(t *testing.T) {
	for _, name := range []string{"index-dsn", "binlog-dir"} {
		flag := indexCmd.Flag(name)
		if flag == nil {
			t.Fatalf("flag --%s not registered", name)
		}
		if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("flag --%s is not marked required", name)
		}
	}
}

func TestIndexCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"index-dsn", "source-dsn", "binlog-dir", "files", "all", "batch-size", "schemas", "tables",
		"skip-source-validation",
	} {
		if indexCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on indexCmd", name)
		}
	}
}

func TestIndexCmd_defaults(t *testing.T) {
	cases := []struct{ flag, want string }{
		{"batch-size", "1000"},
		{"all", "false"},
	}
	for _, tc := range cases {
		f := indexCmd.Flag(tc.flag)
		if f == nil {
			t.Fatalf("flag --%s not registered", tc.flag)
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

// ─── runIndex validation (no DB required) ─────────────────────────────────────

func TestRunIndex_noFilesOrAll(t *testing.T) {
	savedFiles, savedAll := idxFiles, idxAll
	t.Cleanup(func() { idxFiles = savedFiles; idxAll = savedAll })

	idxFiles = ""
	idxAll = false

	err := runIndex(indexCmd, nil)
	if err == nil {
		t.Fatal("expected error when neither --files nor --all is set, got nil")
	}
	if !strings.Contains(err.Error(), "--files or --all") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestRunIndex_allSetPassesFirstGuard verifies that --all bypasses the
// "either --files or --all" guard; execution then fails at config.Connect.
func TestRunIndex_allSetPassesFirstGuard(t *testing.T) {
	savedFiles, savedAll := idxFiles, idxAll
	t.Cleanup(func() { idxFiles = savedFiles; idxAll = savedAll })

	idxFiles = ""
	idxAll = true

	err := runIndex(indexCmd, nil)
	if err != nil && strings.Contains(err.Error(), "--files or --all") {
		t.Errorf("first guard should not fire when --all is set, got: %v", err)
	}
}

// TestRunIndex_filesSetPassesFirstGuard verifies that --files bypasses the
// first guard even without --all.
func TestRunIndex_filesSetPassesFirstGuard(t *testing.T) {
	savedFiles, savedAll := idxFiles, idxAll
	savedSkip := idxSkipSourceCheck
	t.Cleanup(func() { idxFiles = savedFiles; idxAll = savedAll; idxSkipSourceCheck = savedSkip })

	idxFiles = "binlog.000001"
	idxAll = false
	// Clear the source guard (#493) so execution again reaches config.Connect —
	// the first guard under test, not the later --source-dsn requirement.
	idxSkipSourceCheck = true

	err := runIndex(indexCmd, nil) // fails later at config.Connect
	if err != nil && strings.Contains(err.Error(), "--files or --all") {
		t.Errorf("first guard should not fire when --files is set, got: %v", err)
	}
}

// TestRunIndex_requiresSourceOrSkip verifies #493 part 2: without --source-dsn
// the source pre-flight cannot run, so indexing must fail with an actionable
// error instead of silently skipping validation. Passing
// --skip-source-validation is the sanctioned opt-out (for offline binlogs);
// it then fails later at the index DB connect, not at this guard.
func TestRunIndex_requiresSourceOrSkip(t *testing.T) {
	savedFiles, savedAll := idxFiles, idxAll
	savedSource, savedSkip := idxSourceDSN, idxSkipSourceCheck
	savedIndex := idxIndexDSN
	t.Cleanup(func() {
		idxFiles, idxAll = savedFiles, savedAll
		idxSourceDSN, idxSkipSourceCheck = savedSource, savedSkip
		idxIndexDSN = savedIndex
	})

	idxFiles = "binlog.000001"
	idxAll = false
	idxIndexDSN = "user:pass@tcp(127.0.0.1:1)/idx" // unreachable on purpose
	idxSourceDSN = ""

	// Neither --source-dsn nor --skip-source-validation → fail at the new guard.
	idxSkipSourceCheck = false
	err := runIndex(indexCmd, nil)
	if err == nil {
		t.Fatal("expected error when neither --source-dsn nor --skip-source-validation is set")
	}
	if !strings.Contains(err.Error(), "--source-dsn is required") ||
		!strings.Contains(err.Error(), "--skip-source-validation") {
		t.Errorf("guard error should name both flags, got: %v", err)
	}

	// --skip-source-validation set → past the guard; fails later at index connect.
	idxSkipSourceCheck = true
	err = runIndex(indexCmd, nil)
	if err != nil && strings.Contains(err.Error(), "--source-dsn is required") {
		t.Errorf("--skip-source-validation should pass the source guard, got: %v", err)
	}
}

// ─── binlogFileRe ────────────────────────────────────────────────────────────

func TestBinlogFileRe(t *testing.T) {
	cases := []struct {
		name  string
		match bool
	}{
		{"binlog.000001", true},
		{"mysql-bin.000042", true},
		{"binlog.999999", true},
		{"binlog.0000001", true},  // 7 digits — still matches (6+)
		{"binlog.00000000", true}, // 8 digits
		{"notes.txt", false},
		{"binlog.index", false},
		{"binlog.00001", false}, // only 5 digits
		{"binlog.abc123", false},
		{".000001", true}, // degenerate but matches the regex
	}
	for _, tc := range cases {
		got := binlogFileRe.MatchString(tc.name)
		if got != tc.match {
			t.Errorf("binlogFileRe.MatchString(%q) = %v, want %v", tc.name, got, tc.match)
		}
	}
}

// ─── resolveFiles ────────────────────────────────────────────────────────────

func TestResolveFiles_explicit(t *testing.T) {
	files, err := resolveFiles("/tmp", "binlog.000001,binlog.000002", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 2 || files[0] != "binlog.000001" || files[1] != "binlog.000002" {
		t.Errorf("expected [binlog.000001 binlog.000002], got %v", files)
	}
}

func TestResolveFiles_trimming(t *testing.T) {
	files, err := resolveFiles("/tmp", " binlog.000001 , binlog.000002 ", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 2 || files[0] != "binlog.000001" || files[1] != "binlog.000002" {
		t.Errorf("expected trimmed filenames, got %v", files)
	}
}

func TestResolveFiles_dropsEmpty(t *testing.T) {
	files, err := resolveFiles("/tmp", "binlog.000001,,binlog.000002,", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files with empties dropped, got %v", files)
	}
}

func TestResolveFiles_emptyError(t *testing.T) {
	_, err := resolveFiles("/tmp", "", false)
	if err == nil {
		t.Error("expected error for empty --files, got nil")
	}
}

func TestResolveFiles_allEmptyError(t *testing.T) {
	_, err := resolveFiles("/tmp", ",,,", false)
	if err == nil {
		t.Error("expected error when all entries are empty, got nil")
	}
}

// ─── findBinlogFiles ─────────────────────────────────────────────────────────

func TestFindBinlogFiles_matchesPattern(t *testing.T) {
	dir := t.TempDir()
	// Create some files: binlog pattern + non-matching
	for _, name := range []string{"binlog.000001", "binlog.000003", "binlog.000002", "notes.txt", "binlog.index"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte{}, 0644); err != nil {
			t.Fatal(err)
		}
	}

	files, err := findBinlogFiles(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("expected 3 binlog files, got %d: %v", len(files), files)
	}
	// Must be sorted ascending
	if files[0] != "binlog.000001" || files[1] != "binlog.000002" || files[2] != "binlog.000003" {
		t.Errorf("expected sorted [binlog.000001 binlog.000002 binlog.000003], got %v", files)
	}
}

// TestFindBinlogFiles_nonexistentDir verifies that a nonexistent directory
// surfaces the "failed to read binlog directory" error from os.ReadDir.
func TestFindBinlogFiles_nonexistentDir(t *testing.T) {
	_, err := findBinlogFiles("/nonexistent/bintrail-test-path-xyz")
	if err == nil {
		t.Fatal("expected error for nonexistent directory, got nil")
	}
	if !strings.Contains(err.Error(), "failed to read binlog directory") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestFindBinlogFiles_emptyDir(t *testing.T) {
	dir := t.TempDir()
	_, err := findBinlogFiles(dir)
	if err == nil {
		t.Error("expected error for empty directory, got nil")
	}
}

func TestFindBinlogFiles_noMatches(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "notes.txt"), []byte{}, 0644)
	_, err := findBinlogFiles(dir)
	if err == nil {
		t.Error("expected error when no binlog files match, got nil")
	}
}

func TestFindBinlogFiles_skipsDirectories(t *testing.T) {
	dir := t.TempDir()
	// Create a directory that matches the pattern — should be skipped
	os.MkdirAll(filepath.Join(dir, "binlog.000001"), 0755)
	os.WriteFile(filepath.Join(dir, "binlog.000002"), []byte{}, 0644)

	files, err := findBinlogFiles(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 1 || files[0] != "binlog.000002" {
		t.Errorf("expected [binlog.000002] (directory skipped), got %v", files)
	}
}

// ─── binlogFileRe end-anchor ──────────────────────────────────────────────────

// TestBinlogFileRe_trailingSuffix verifies that the $ anchor in binlogFileRe
// prevents matching filenames where the digit sequence is not at the end.
func TestBinlogFileRe_trailingSuffix(t *testing.T) {
	cases := []struct {
		name  string
		match bool
	}{
		{"binlog.000001.bak", false},   // digits not at end
		{"binlog.000001-relay", false}, // digits not at end
		{"binlog.000001", true},        // digits at end — baseline check
		{"mysql-bin.000001.gz", false}, // compressed backup
	}
	for _, tc := range cases {
		got := binlogFileRe.MatchString(tc.name)
		if got != tc.match {
			t.Errorf("binlogFileRe.MatchString(%q) = %v, want %v", tc.name, got, tc.match)
		}
	}
}

// ─── resolveFiles with all=true ───────────────────────────────────────────────

// TestResolveFiles_allTrue verifies that resolveFiles delegates to findBinlogFiles
// when all=true, returning only the matching files from the directory.
func TestResolveFiles_allTrue(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"binlog.000001", "binlog.000002", "notes.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte{}, 0644); err != nil {
			t.Fatal(err)
		}
	}

	files, err := resolveFiles(dir, "", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 binlog files, got %d: %v", len(files), files)
	}
	if files[0] != "binlog.000001" || files[1] != "binlog.000002" {
		t.Errorf("expected [binlog.000001 binlog.000002], got %v", files)
	}
}
