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
	t.Cleanup(func() { idxFiles = savedFiles; idxAll = savedAll })

	idxFiles = "binlog.000001"
	idxAll = false

	err := runIndex(indexCmd, nil) // fails later at config.Connect
	if err != nil && strings.Contains(err.Error(), "--files or --all") {
		t.Errorf("first guard should not fire when --files is set, got: %v", err)
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

// ─── buildIndexFilters ───────────────────────────────────────────────────────

func TestBuildIndexFilters_empty(t *testing.T) {
	f := buildIndexFilters("", "")
	if f.Schemas != nil {
		t.Errorf("expected nil Schemas map, got %v", f.Schemas)
	}
	if f.Tables != nil {
		t.Errorf("expected nil Tables map, got %v", f.Tables)
	}
}

func TestBuildIndexFilters_schemasOnly(t *testing.T) {
	f := buildIndexFilters("mydb,other", "")
	if f.Schemas == nil || !f.Schemas["mydb"] || !f.Schemas["other"] {
		t.Errorf("expected Schemas {mydb:true, other:true}, got %v", f.Schemas)
	}
	if f.Tables != nil {
		t.Errorf("expected nil Tables, got %v", f.Tables)
	}
}

func TestBuildIndexFilters_tablesOnly(t *testing.T) {
	f := buildIndexFilters("", "mydb.orders,mydb.items")
	if f.Schemas != nil {
		t.Errorf("expected nil Schemas, got %v", f.Schemas)
	}
	if f.Tables == nil || !f.Tables["mydb.orders"] || !f.Tables["mydb.items"] {
		t.Errorf("expected Tables {mydb.orders:true, mydb.items:true}, got %v", f.Tables)
	}
}

func TestBuildIndexFilters_both(t *testing.T) {
	f := buildIndexFilters("mydb", "mydb.orders")
	if f.Schemas == nil || !f.Schemas["mydb"] {
		t.Error("expected Schemas with mydb")
	}
	if f.Tables == nil || !f.Tables["mydb.orders"] {
		t.Error("expected Tables with mydb.orders")
	}
}

func TestBuildIndexFilters_trimming(t *testing.T) {
	f := buildIndexFilters(" mydb , other ", " mydb.orders , mydb.items ")
	if !f.Schemas["mydb"] || !f.Schemas["other"] {
		t.Errorf("expected trimmed schemas, got %v", f.Schemas)
	}
	if !f.Tables["mydb.orders"] || !f.Tables["mydb.items"] {
		t.Errorf("expected trimmed tables, got %v", f.Tables)
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
		{"binlog.000001.bak", false},    // digits not at end
		{"binlog.000001-relay", false},  // digits not at end
		{"binlog.000001", true},         // digits at end — baseline check
		{"mysql-bin.000001.gz", false},  // compressed backup
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

// ─── nullOrStringVal ─────────────────────────────────────────────────────────

func TestNullOrStringVal_empty(t *testing.T) {
	got := nullOrStringVal("")
	if got != nil {
		t.Errorf("expected nil for empty string, got %v", got)
	}
}

func TestNullOrStringVal_nonEmpty(t *testing.T) {
	got := nullOrStringVal("uuid:42")
	if got != "uuid:42" {
		t.Errorf("expected 'uuid:42', got %v", got)
	}
}
