package cliapp

// Integrity authentication for encrypted dump files (#960).
//
// `openssl enc -aes-256-cbc` gives confidentiality but NO authentication: a
// tampered or bit-rotted .enc file decrypts "successfully" to garbled SQL that
// would flow silently into baseline conversion and everything downstream of it
// (reconstruct, recover). `openssl enc` does not support AEAD modes (GCM), so
// the fix is encrypt-then-MAC: after mydumper completes, every .enc file gets
// an HMAC-SHA256 sidecar (`<name>.enc.hmac`, lowercase hex digest) keyed with
// the raw bytes of the encryption key file. `bintrail baseline --encrypt`
// verifies the sidecar BEFORE decrypting; a mismatch is a hard error (the file
// is never decrypted), a missing sidecar (pre-fix legacy dump) is a warning
// and decryption proceeds for backward compatibility.
//
// The key bytes are read in-process only — they never appear on argv or in the
// environment.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// hmacSidecarSuffix is appended to the encrypted file name, so a sidecar for
// `mydb.orders.00000.sql.enc` is `mydb.orders.00000.sql.enc.hmac`. Sidecars do
// not end in ".enc", so the decrypt walk in decryptDumpFiles never tries to
// decrypt them, and the baseline converter's mydumper-pattern matching ignores
// them.
const hmacSidecarSuffix = ".hmac"

// readHMACKey reads the raw bytes of the encryption key file for use as the
// HMAC key. Kept as a helper so every call site gets the same error framing.
func readHMACKey(keyPath string) ([]byte, error) {
	key, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read encryption key for HMAC: %w", err)
	}
	if len(key) == 0 {
		return nil, fmt.Errorf("encryption key file %s is empty", keyPath)
	}
	return key, nil
}

// computeFileHMAC streams path through HMAC-SHA256 keyed with key and returns
// the lowercase hex digest.
func computeFileHMAC(path string, key []byte) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s for HMAC: %w", path, err)
	}
	defer f.Close()

	mac := hmac.New(sha256.New, key)
	if _, err := io.Copy(mac, f); err != nil {
		return "", fmt.Errorf("read %s for HMAC: %w", path, err)
	}
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// writeDumpHMACSidecars walks dir and writes a `<name>.enc.hmac` sidecar for
// every `.enc` file, returning how many were written. Called by `bintrail dump`
// after mydumper completes successfully with --encrypt.
func writeDumpHMACSidecars(dir string, key []byte) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("read dump directory: %w", err)
	}

	written := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".enc") {
			continue
		}
		encPath := filepath.Join(dir, e.Name())
		digest, err := computeFileHMAC(encPath, key)
		if err != nil {
			return written, err
		}
		sidecar := encPath + hmacSidecarSuffix
		if err := os.WriteFile(sidecar, []byte(digest+"\n"), 0o600); err != nil {
			return written, fmt.Errorf("write HMAC sidecar %s: %w", sidecar, err)
		}
		written++
	}
	return written, nil
}

// verifyEncFileHMAC checks encPath against its `.hmac` sidecar.
//
// Returns (false, nil) when no sidecar exists — a legacy dump written before
// sidecars existed; the caller warns and proceeds. Returns an error (and never
// asks the caller to decrypt) when the sidecar exists but the recomputed
// HMAC-SHA256 does not match.
func verifyEncFileHMAC(encPath string, key []byte) (hadSidecar bool, err error) {
	sidecar := encPath + hmacSidecarSuffix
	want, err := os.ReadFile(sidecar)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read HMAC sidecar %s: %w", sidecar, err)
	}

	got, err := computeFileHMAC(encPath, key)
	if err != nil {
		return true, err
	}
	if !hmac.Equal([]byte(got), []byte(strings.TrimSpace(string(want)))) {
		return true, fmt.Errorf("integrity check failed for %s: HMAC-SHA256 mismatch with its .hmac sidecar — the encrypted file was modified after the dump (or a different --encrypt-key is in use); refusing to decrypt it", filepath.Base(encPath))
	}
	return true, nil
}
