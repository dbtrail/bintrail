package main

import (
	"os"
	"path/filepath"
	"testing"
)

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
