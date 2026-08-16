package cliapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// fakeMydumper0180 writes a fake mydumper that reports a version NEW ENOUGH to
// accept --sync-thread-lock-mode, and records the argv it was called with.
//
// The version string is the whole point. Every pre-existing runDump test uses a
// fake reporting 0.15.0, which sets supportsLockMode=false and makes the entire
// lock-mode region of runDump structurally unreachable — so none of it was
// covered, including the privilege preflight that exists to keep mydumper from
// segfaulting.
func fakeMydumper0180(t *testing.T, dir string) (bin, record string) {
	t.Helper()
	bin = filepath.Join(dir, "mydumper")
	record = filepath.Join(dir, "argv.txt")
	script := "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then echo \"mydumper 0.18.0 (built with foo)\"; exit 0; fi\n" +
		"echo \"$@\" > " + record + "\n" +
		"exit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake mydumper: %v", err)
	}
	return bin, record
}

// newDumpCmdForTest returns a command carrying a context. runDump passes
// cmd.Context() to the privilege preflight, and a cobra command built without
// one returns nil — the repo's recorded gotcha (a nil context hangs or panics
// rather than failing cleanly).
func newDumpCmdForTest(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{RunE: runDump}
	cmd.Flags().StringVar(&dmpLockMode, "lock-mode", "ftwrl", "")
	cmd.Flags().StringVar(&dmpMydumperPath, "mydumper-path", "mydumper", "")
	cmd.SetContext(context.Background())
	return cmd
}

// TestRunDumpDefaultModeChecksPrivilegesBeforeDumping asserts the ABSENCE of a
// side effect, not just an error: with the point-consistent default, mydumper
// must never be launched against a source whose privileges were not verified.
// Granting BACKUP_ADMIN without RELOAD makes the pinned build SEGFAULT rather
// than fail cleanly, so this guard is what keeps that crash unreachable — and
// #1377 made this the DEFAULT path of the surface most operators use.
func TestRunDumpDefaultModeChecksPrivilegesBeforeDumping(t *testing.T) {
	dir := t.TempDir()
	bin, record := fakeMydumper0180(t, dir)

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })

	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/" // nothing listens: the probe cannot pass
	dmpOutputDir = filepath.Join(dir, "out")
	dmpMydumperPath = bin
	dmpLockMode = "ftwrl"
	dmpFormat = "text"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	err := runDump(cmd, nil)
	if err == nil {
		t.Fatal("the default lock mode dumped without verifying privileges; the segfault path is reachable again")
	}
	if _, statErr := os.Stat(record); statErr == nil {
		t.Error("mydumper ran even though the privilege probe failed — the preflight must gate the launch, not just report")
	}
}

// TestRunDumpSafeNoLockSkipsPreflightAndCarriesTheMode covers the other half:
// a mode that needs no elevated privilege must not be gated by the probe, and
// the mode the operator chose must reach mydumper's argv. Passing no-lock here
// instead would prove nothing — a builder that ignored its argument and
// hardcoded NO_LOCK would produce identical argv.
func TestRunDumpSafeNoLockSkipsPreflightAndCarriesTheMode(t *testing.T) {
	dir := t.TempDir()
	bin, record := fakeMydumper0180(t, dir)

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })

	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = filepath.Join(dir, "out")
	dmpMydumperPath = bin
	dmpLockMode = "safe-no-lock"
	dmpFormat = "text"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Flags().Set("lock-mode", "safe-no-lock"); err != nil {
		t.Fatal(err)
	}
	if err := runDump(cmd, nil); err != nil {
		t.Fatalf("safe-no-lock was blocked by the privilege preflight it does not need: %v", err)
	}

	argv, readErr := os.ReadFile(record)
	if readErr != nil {
		t.Fatalf("mydumper never ran: %v", readErr)
	}
	if !strings.Contains(string(argv), "SAFE_NO_LOCK") {
		t.Errorf("argv = %q; the operator asked for safe-no-lock and mydumper was told something else", argv)
	}
}
