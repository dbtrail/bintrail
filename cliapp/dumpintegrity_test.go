package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTestEncDir creates a temp dump dir with one fake .enc file and a key
// file, returning (dir, encPath, keyPath, key bytes).
func writeTestEncDir(t *testing.T) (dir, encPath, keyPath string, key []byte) {
	t.Helper()
	dir = t.TempDir()
	encPath = filepath.Join(dir, "mydb.orders.00000.sql.enc")
	if err := os.WriteFile(encPath, []byte("fake-ciphertext-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	key = []byte("0123456789abcdef0123456789abcdef")
	keyPath = filepath.Join(dir, "dump.key")
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, encPath, keyPath, key
}

func TestHMACSidecarRoundTrip(t *testing.T) {
	dir, encPath, _, key := writeTestEncDir(t)

	n, err := writeDumpHMACSidecars(dir, key)
	if err != nil {
		t.Fatalf("writeDumpHMACSidecars: %v", err)
	}
	if n != 1 {
		t.Fatalf("wrote %d sidecars, want 1", n)
	}
	if _, err := os.Stat(encPath + hmacSidecarSuffix); err != nil {
		t.Fatalf("sidecar not written: %v", err)
	}

	hadSidecar, err := verifyEncFileHMAC(encPath, key)
	if err != nil {
		t.Fatalf("verifyEncFileHMAC on untampered file: %v", err)
	}
	if !hadSidecar {
		t.Fatal("hadSidecar = false, want true")
	}
}

func TestWriteDumpHMACSidecarsOnlyTargetsEncFiles(t *testing.T) {
	dir, encPath, _, key := writeTestEncDir(t)
	// A plaintext mydumper file and a pre-existing sidecar must both be
	// ignored — no .sql.hmac and no .enc.hmac.hmac.
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := writeDumpHMACSidecars(dir, key); err != nil {
		t.Fatal(err)
	}

	n, err := writeDumpHMACSidecars(dir, key) // second pass sees the sidecar
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("second pass wrote %d sidecars, want 1 (the .enc only)", n)
	}
	if _, err := os.Stat(encPath + hmacSidecarSuffix + hmacSidecarSuffix); err == nil {
		t.Fatal("a sidecar-of-a-sidecar was written")
	}
}

func TestVerifyEncFileHMACTampered(t *testing.T) {
	dir, encPath, _, key := writeTestEncDir(t)
	if _, err := writeDumpHMACSidecars(dir, key); err != nil {
		t.Fatal(err)
	}
	// Flip content after the sidecar was written.
	if err := os.WriteFile(encPath, []byte("Take-ciphertext-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	hadSidecar, err := verifyEncFileHMAC(encPath, key)
	if !hadSidecar {
		t.Fatal("hadSidecar = false, want true")
	}
	if err == nil || !strings.Contains(err.Error(), "HMAC-SHA256 mismatch") {
		t.Fatalf("err = %v, want HMAC-SHA256 mismatch", err)
	}
	if !strings.Contains(err.Error(), "mydb.orders.00000.sql.enc") {
		t.Fatalf("error does not name the file: %v", err)
	}
}

func TestVerifyEncFileHMACWrongKey(t *testing.T) {
	dir, encPath, _, key := writeTestEncDir(t)
	if _, err := writeDumpHMACSidecars(dir, key); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyEncFileHMAC(encPath, []byte("a different key")); err == nil ||
		!strings.Contains(err.Error(), "HMAC-SHA256 mismatch") {
		t.Fatalf("err = %v, want HMAC-SHA256 mismatch", err)
	}
}

func TestVerifyEncFileHMACMissingSidecarIsLegacy(t *testing.T) {
	_, encPath, _, key := writeTestEncDir(t)
	hadSidecar, err := verifyEncFileHMAC(encPath, key)
	if err != nil {
		t.Fatalf("missing sidecar must not error (legacy dump): %v", err)
	}
	if hadSidecar {
		t.Fatal("hadSidecar = true, want false")
	}
}

// TestDecryptDumpFilesRefusesTamperedFile drives the REAL production entry
// point (`bintrail baseline --encrypt` → decryptDumpFiles): a tampered .enc
// must be refused BEFORE openssl is ever invoked — which also means this test
// needs no openssl. If the verify call is removed from decryptDumpFiles, the
// error becomes an openssl failure (or "openssl not found") and the mismatch
// assertion goes red.
func TestDecryptDumpFilesRefusesTamperedFile(t *testing.T) {
	dir, encPath, keyPath, key := writeTestEncDir(t)
	if _, err := writeDumpHMACSidecars(dir, key); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(encPath, []byte("tampered!"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := decryptDumpFiles(dir, keyPath)
	if err == nil || !strings.Contains(err.Error(), "HMAC-SHA256 mismatch") {
		t.Fatalf("decryptDumpFiles err = %v, want HMAC-SHA256 mismatch", err)
	}
	// The tampered file must never have been decrypted.
	if _, statErr := os.Stat(strings.TrimSuffix(encPath, ".enc")); statErr == nil {
		t.Fatal("tampered file was decrypted despite the HMAC mismatch")
	}
}

// The .enc.hmac sidecars themselves must be invisible to the decrypt walk: a
// directory holding only a sidecar (no .enc) decrypts nothing and succeeds.
func TestDecryptDumpFilesSkipsSidecars(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mydb.orders.00000.sql.enc.hmac"), []byte("deadbeef\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(dir, "dump.key")
	if err := os.WriteFile(keyPath, []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}

	cleanup, err := decryptDumpFiles(dir, keyPath)
	if err != nil {
		t.Fatalf("decryptDumpFiles: %v", err)
	}
	cleanup()
}
