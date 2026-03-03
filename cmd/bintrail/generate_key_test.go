package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateKeyCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "generate-key" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'generate-key' command to be registered under rootCmd")
	}
}

func TestGenerateKeyCmd_flagsRegistered(t *testing.T) {
	for _, name := range []string{"output", "format"} {
		if generateKeyCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on generateKeyCmd", name)
		}
	}
}

func TestRunGenerateKey_createsKeyFile(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")

	saved := gkOutput
	savedFmt := gkFormat
	t.Cleanup(func() {
		gkOutput = saved
		gkFormat = savedFmt
	})
	gkOutput = keyPath
	gkFormat = "text"

	if err := runGenerateKey(generateKeyCmd, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("failed to read key file: %v", err)
	}

	content := strings.TrimSpace(string(data))
	if len(content) == 0 {
		t.Error("key file is empty")
	}

	// Base64-encoded 32 bytes = 44 characters.
	if len(content) != 44 {
		t.Errorf("expected 44-char base64 key, got %d chars: %q", len(content), content)
	}

	// Verify file permissions.
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("failed to stat key file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("expected 0600 permissions, got %04o", perm)
	}
}

func TestRunGenerateKey_refusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	if err := os.WriteFile(keyPath, []byte("existing"), 0o600); err != nil {
		t.Fatalf("failed to create existing key file: %v", err)
	}

	saved := gkOutput
	savedFmt := gkFormat
	t.Cleanup(func() {
		gkOutput = saved
		gkFormat = savedFmt
	})
	gkOutput = keyPath
	gkFormat = "text"

	err := runGenerateKey(generateKeyCmd, nil)
	if err == nil {
		t.Fatal("expected error when key file already exists")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("expected 'already exists' in error, got: %v", err)
	}
}

func TestRunGenerateKey_createsDirectory(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "subdir", "nested", "test.key")

	saved := gkOutput
	savedFmt := gkFormat
	t.Cleanup(func() {
		gkOutput = saved
		gkFormat = savedFmt
	})
	gkOutput = keyPath
	gkFormat = "text"

	if err := runGenerateKey(generateKeyCmd, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := os.Stat(keyPath); err != nil {
		t.Errorf("key file not created at %s: %v", keyPath, err)
	}
}

func TestRunGenerateKey_invalidFormat(t *testing.T) {
	saved := gkFormat
	t.Cleanup(func() { gkFormat = saved })
	gkFormat = "xml"

	err := runGenerateKey(generateKeyCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
	if !strings.Contains(err.Error(), "invalid --format") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDefaultKeyPath(t *testing.T) {
	p := defaultKeyPath()
	if !strings.HasSuffix(p, filepath.Join(".config", "bintrail", "dump.key")) {
		t.Errorf("unexpected default key path: %s", p)
	}
}
