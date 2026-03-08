package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunConfigInit(t *testing.T) {
	t.Run("creates .bintrail.env in current directory", func(t *testing.T) {
		dir := t.TempDir()
		// Save and restore working directory.
		orig, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		cfgGlobal = false
		if err := runConfigInit(configInitCmd, nil); err != nil {
			t.Fatal(err)
		}

		path := filepath.Join(dir, ".bintrail.env")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("expected file at %s: %v", path, err)
		}

		content := string(data)
		if !strings.Contains(content, "BINTRAIL_INDEX_DSN") {
			t.Error("expected BINTRAIL_INDEX_DSN in template")
		}
		if !strings.Contains(content, "BINTRAIL_SOURCE_DSN") {
			t.Error("expected BINTRAIL_SOURCE_DSN in template")
		}
		if !strings.Contains(content, "# Bintrail configuration") {
			t.Error("expected header comment")
		}

		// Verify file permissions (owner-only).
		info, _ := os.Stat(path)
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Errorf("file permissions = %o, want 0600", perm)
		}
	})

	t.Run("creates global config", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv("HOME", dir)

		cfgGlobal = true
		if err := runConfigInit(configInitCmd, nil); err != nil {
			t.Fatal(err)
		}

		path := filepath.Join(dir, ".config", "bintrail", "config.env")
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected file at %s: %v", path, err)
		}
	})

	t.Run("error when file exists", func(t *testing.T) {
		dir := t.TempDir()
		orig, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		// Create the file first.
		os.WriteFile(filepath.Join(dir, ".bintrail.env"), []byte("existing"), 0o644)

		cfgGlobal = false
		err = runConfigInit(configInitCmd, nil)
		if err == nil {
			t.Fatal("expected error when file exists")
		}
		if !strings.Contains(err.Error(), "already exists") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestGenerateEnvTemplate(t *testing.T) {
	t.Run("pre-fills from environment", func(t *testing.T) {
		t.Setenv("BINTRAIL_INDEX_DSN", "test-dsn-value")

		content := generateEnvTemplate()

		// The env var should appear uncommented with its value.
		if !strings.Contains(content, "BINTRAIL_INDEX_DSN=test-dsn-value") {
			t.Error("expected BINTRAIL_INDEX_DSN=test-dsn-value (uncommented)")
		}
		// Other vars should be commented out.
		if !strings.Contains(content, "# BINTRAIL_SOURCE_DSN=") {
			t.Error("expected BINTRAIL_SOURCE_DSN to be commented out")
		}
	})

	t.Run("all env vars present", func(t *testing.T) {
		content := generateEnvTemplate()
		for _, b := range envBindings {
			if !strings.Contains(content, b.EnvVar) {
				t.Errorf("missing %s in template", b.EnvVar)
			}
		}
	})

	t.Run("contains section headers", func(t *testing.T) {
		content := generateEnvTemplate()
		for _, sec := range envSections {
			if !strings.Contains(content, sec.Header) {
				t.Errorf("missing section header %q", sec.Header)
			}
		}
	})
}
