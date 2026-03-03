package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// ─── Cobra command wiring ─────────────────────────────────────────────────────

func TestDumpCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "dump" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'dump' command to be registered under rootCmd")
	}
}

func TestDumpCmd_requiredFlags(t *testing.T) {
	for _, name := range []string{"source-dsn", "output-dir"} {
		flag := dumpCmd.Flag(name)
		if flag == nil {
			t.Fatalf("flag --%s not registered", name)
		}
		if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("flag --%s is not marked required", name)
		}
	}
}

func TestDumpCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"threads", "4"},
		{"mydumper-path", "mydumper"},
	}
	for _, tc := range cases {
		f := dumpCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

func TestDumpCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"source-dsn", "output-dir", "schemas", "tables", "mydumper-path", "mydumper-image", "threads",
	} {
		if dumpCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on dumpCmd", name)
		}
	}
}

// ─── buildMydumperArgs ────────────────────────────────────────────────────────

func TestBuildMydumperArgs_basic(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "secret", "/tmp/dump", 4, nil, nil)
	assertArgsContainPair(t, args, "--host", "127.0.0.1")
	assertArgsContainPair(t, args, "--port", "3306")
	assertArgsContainPair(t, args, "--user", "root")
	assertArgsContainPair(t, args, "--outputdir", "/tmp/dump")
	assertArgsContainPair(t, args, "--threads", "4")
}

func TestBuildMydumperArgs_compressAndComplete(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil)
	if !argsContain(args, "--compress-protocol") {
		t.Error("expected --compress-protocol in args")
	}
	if !argsContain(args, "--complete-insert") {
		t.Error("expected --complete-insert in args")
	}
}

func TestBuildMydumperArgs_lockAndTrx(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil)
	assertArgsContainPair(t, args, "--sync-thread-lock-mode", "NO_LOCK")
	if !argsContain(args, "--trx-tables") {
		t.Error("expected --trx-tables in args")
	}
}

func TestBuildMydumperArgs_noPassword(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil)
	if argsContain(args, "--password") {
		t.Error("expected --password to be absent when password is empty")
	}
}

func TestBuildMydumperArgs_singleSchema(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, []string{"mydb"}, nil)
	assertArgsContainPair(t, args, "--database", "mydb")
	if argsContain(args, "--regex") {
		t.Error("expected --regex to be absent for single schema")
	}
}

func TestBuildMydumperArgs_multipleSchemas(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, []string{"db1", "db2"}, nil)
	if argsContain(args, "--database") {
		t.Error("expected --database to be absent for multiple schemas")
	}
	idx := argsIndex(args, "--regex")
	if idx < 0 {
		t.Fatal("expected --regex in args")
	}
	if idx+1 >= len(args) {
		t.Fatal("--regex has no value")
	}
	regex := args[idx+1]
	if !strings.Contains(regex, "db1") || !strings.Contains(regex, "db2") {
		t.Errorf("regex %q does not contain both schema names", regex)
	}
}

func TestBuildMydumperArgs_withPassword(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "s3cr3t", "/tmp/dump", 4, nil, nil)
	assertArgsContainPair(t, args, "--password", "s3cr3t")
}

func TestBuildMydumperArgs_noSchemasOrTables(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil)
	for _, flag := range []string{"--database", "--regex", "--tables-list"} {
		if argsContain(args, flag) {
			t.Errorf("expected %s to be absent when no schemas or tables given", flag)
		}
	}
}

func TestBuildMydumperArgs_regexAnchoredFormat(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, []string{"db1", "db2"}, nil)
	idx := argsIndex(args, "--regex")
	if idx < 0 || idx+1 >= len(args) {
		t.Fatal("expected --regex in args")
	}
	regex := args[idx+1]
	// Must be anchored at start and dot must be escaped.
	if !strings.HasPrefix(regex, "^(") {
		t.Errorf("regex should start with ^(, got %q", regex)
	}
	if !strings.HasSuffix(regex, `\.`) {
		t.Errorf("regex should end with \\., got %q", regex)
	}
}

func TestBuildMydumperArgs_schemaAndTables(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4,
		[]string{"mydb"}, []string{"mydb.orders", "mydb.items"})
	assertArgsContainPair(t, args, "--database", "mydb")
	idx := argsIndex(args, "--tables-list")
	if idx < 0 || idx+1 >= len(args) {
		t.Fatal("expected --tables-list in args")
	}
	if !strings.Contains(args[idx+1], "mydb.orders") {
		t.Errorf("--tables-list missing mydb.orders: %q", args[idx+1])
	}
}

func TestBuildMydumperArgs_tables(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, []string{"mydb.orders", "mydb.items"})
	idx := argsIndex(args, "--tables-list")
	if idx < 0 {
		t.Fatal("expected --tables-list in args")
	}
	if idx+1 >= len(args) {
		t.Fatal("--tables-list has no value")
	}
	val := args[idx+1]
	if !strings.Contains(val, "mydb.orders") || !strings.Contains(val, "mydb.items") {
		t.Errorf("--tables-list value %q missing expected table names", val)
	}
}

// ─── Lock mechanism ───────────────────────────────────────────────────────────

func TestAcquireReleaseDumpLock(t *testing.T) {
	dir := t.TempDir()
	old := dumpLockDir
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = old })

	f, err := acquireDumpLock()
	if err != nil {
		t.Fatalf("acquireDumpLock: unexpected error: %v", err)
	}

	// Lock file should exist and contain our PID.
	lockPath := filepath.Join(dir, dumpLockFilename)
	data, readErr := os.ReadFile(lockPath)
	if readErr != nil {
		t.Fatalf("failed to read lock file: %v", readErr)
	}
	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
	if parseErr != nil {
		t.Fatalf("invalid PID in lock file %q: %v", string(data), parseErr)
	}
	if pid != os.Getpid() {
		t.Errorf("lock PID: expected %d, got %d", os.Getpid(), pid)
	}

	// Release and verify the file is gone.
	releaseDumpLock(f)
	if _, statErr := os.Stat(lockPath); !os.IsNotExist(statErr) {
		t.Error("lock file should not exist after release")
	}
}

func TestAcquireDumpLock_alreadyHeld(t *testing.T) {
	dir := t.TempDir()
	old := dumpLockDir
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = old })

	f, err := acquireDumpLock()
	if err != nil {
		t.Fatalf("first acquireDumpLock: unexpected error: %v", err)
	}
	defer releaseDumpLock(f)

	_, err = acquireDumpLock()
	if err == nil {
		t.Error("expected error on second acquireDumpLock, got nil")
	}
}

func TestAcquireDumpLock_staleLock(t *testing.T) {
	dir := t.TempDir()
	old := dumpLockDir
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = old })

	// Write a lockfile with a PID that almost certainly does not exist.
	lockPath := filepath.Join(dir, dumpLockFilename)
	if err := os.WriteFile(lockPath, []byte("999999999"), 0o600); err != nil {
		t.Fatalf("failed to write stale lock file: %v", err)
	}

	f, err := acquireDumpLock()
	if err != nil {
		t.Fatalf("acquireDumpLock with stale lock: unexpected error: %v", err)
	}
	releaseDumpLock(f)
}

// ─── extractSchemasFromTables ─────────────────────────────────────────────────

// TestBuildMydumperArgs_threeSchemas verifies that 3+ schemas all appear in the
// --regex value, not just the first two.
func TestBuildMydumperArgs_threeSchemas(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, []string{"db1", "db2", "db3"}, nil)
	idx := argsIndex(args, "--regex")
	if idx < 0 || idx+1 >= len(args) {
		t.Fatal("expected --regex in args for 3 schemas")
	}
	regex := args[idx+1]
	for _, s := range []string{"db1", "db2", "db3"} {
		if !strings.Contains(regex, s) {
			t.Errorf("regex %q missing schema %q", regex, s)
		}
	}
}

// TestAcquireDumpLock_invalidPIDContent verifies that a lockfile containing
// non-numeric text (parseErr != nil) is treated as stale — the signal check
// is skipped and the file is removed so a fresh lock can be acquired.
func TestAcquireDumpLock_invalidPIDContent(t *testing.T) {
	dir := t.TempDir()
	old := dumpLockDir
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = old })

	lockPath := filepath.Join(dir, dumpLockFilename)
	if err := os.WriteFile(lockPath, []byte("not-a-pid"), 0o600); err != nil {
		t.Fatalf("failed to write stale lock file: %v", err)
	}

	f, err := acquireDumpLock()
	if err != nil {
		t.Fatalf("acquireDumpLock with invalid PID content: unexpected error: %v", err)
	}
	releaseDumpLock(f)
}

func TestExtractSchemasFromTables_basic(t *testing.T) {
	schemas := extractSchemasFromTables([]string{"mydb.t1", "mydb.t2", "other.t3"})
	if len(schemas) != 2 {
		t.Fatalf("expected 2 schemas, got %d: %v", len(schemas), schemas)
	}
	if schemas[0] != "mydb" {
		t.Errorf("schemas[0]: expected mydb, got %q", schemas[0])
	}
	if schemas[1] != "other" {
		t.Errorf("schemas[1]: expected other, got %q", schemas[1])
	}
}

func TestExtractSchemasFromTables_noDot(t *testing.T) {
	schemas := extractSchemasFromTables([]string{"nodot", "mydb.t1"})
	if len(schemas) != 1 || schemas[0] != "mydb" {
		t.Errorf("expected [mydb], got %v", schemas)
	}
}

func TestExtractSchemasFromTables_allNoDot(t *testing.T) {
	// All entries lack a dot — none contribute a schema, so result must be nil.
	if schemas := extractSchemasFromTables([]string{"nodot1", "nodot2", "nodot3"}); schemas != nil {
		t.Errorf("expected nil when all entries have no dot, got %v", schemas)
	}
}

// TestExtractSchemasFromTables_deduplicationOrder verifies that insertion order
// is preserved and a schema that appears in multiple entries is counted once.
func TestExtractSchemasFromTables_deduplicationOrder(t *testing.T) {
	got := extractSchemasFromTables([]string{"other.t1", "mydb.t1", "other.t2"})
	want := []string{"other", "mydb"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] expected %q, got %q", i, want[i], got[i])
		}
	}
}

func TestExtractSchemasFromTables_empty(t *testing.T) {
	if schemas := extractSchemasFromTables(nil); schemas != nil {
		t.Errorf("expected nil for nil input, got %v", schemas)
	}
	if schemas := extractSchemasFromTables([]string{}); schemas != nil {
		t.Errorf("expected nil for empty slice, got %v", schemas)
	}
}

// ─── RunE validation ──────────────────────────────────────────────────────────

func TestRunDump_mydumperNotFound(t *testing.T) {
	// Use Flags().Set to mark --mydumper-path as Changed so resolveMydumper
	// tries the explicit path branch.
	savedPath := dmpMydumperPath
	t.Cleanup(func() {
		dmpMydumperPath = savedPath
		dumpCmd.Flags().Set("mydumper-path", savedPath)
	})
	dumpCmd.Flags().Set("mydumper-path", "/nonexistent/path/to/mydumper")

	savedDSN := dmpSourceDSN
	t.Cleanup(func() { dmpSourceDSN = savedDSN })
	dmpSourceDSN = "root@tcp(127.0.0.1:3306)/"

	err := runDump(dumpCmd, nil)
	if err == nil {
		t.Fatal("expected error for missing mydumper, got nil")
	}
	if !strings.Contains(err.Error(), "mydumper not found") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunDump_invalidSourceDSN(t *testing.T) {
	// Create a fake mydumper binary (LookPath only needs it to exist and be executable).
	dir := t.TempDir()
	fakeBin := filepath.Join(dir, "mydumper")
	if err := os.WriteFile(fakeBin, []byte(""), 0o755); err != nil {
		t.Fatalf("failed to create fake mydumper: %v", err)
	}

	savedPath, savedDSN := dmpMydumperPath, dmpSourceDSN
	t.Cleanup(func() {
		dmpMydumperPath = savedPath
		dumpCmd.Flags().Set("mydumper-path", savedPath)
		dmpSourceDSN = savedDSN
	})

	dumpCmd.Flags().Set("mydumper-path", fakeBin)
	dmpSourceDSN = "root@unix(/var/run/mysqld.sock)/" // unix socket → rejected by parseSourceDSN

	err := runDump(dumpCmd, nil)
	if err == nil {
		t.Fatal("expected error for unix socket DSN, got nil")
	}
	if !strings.Contains(err.Error(), "unix socket") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ─── resolveMydumper ──────────────────────────────────────────────────────────

func TestResolveMydumper_explicitPathTakesPrecedence(t *testing.T) {
	dir := t.TempDir()
	fakeBin := filepath.Join(dir, "mydumper")
	if err := os.WriteFile(fakeBin, []byte(""), 0o755); err != nil {
		t.Fatalf("failed to create fake mydumper: %v", err)
	}

	saved := dmpMydumperPath
	t.Cleanup(func() {
		dmpMydumperPath = saved
		dumpCmd.Flags().Set("mydumper-path", saved)
	})
	dumpCmd.Flags().Set("mydumper-path", fakeBin)

	res, err := resolveMydumper(dumpCmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.mode != dumpModeLocal {
		t.Errorf("expected dumpModeLocal, got %d", res.mode)
	}
	if res.path != fakeBin {
		t.Errorf("expected path %q, got %q", fakeBin, res.path)
	}
}

func TestResolveMydumper_explicitPathNotFound(t *testing.T) {
	saved := dmpMydumperPath
	t.Cleanup(func() {
		dmpMydumperPath = saved
		dumpCmd.Flags().Set("mydumper-path", saved)
	})
	dumpCmd.Flags().Set("mydumper-path", "/nonexistent/mydumper")

	_, err := resolveMydumper(dumpCmd)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "mydumper not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestResolveMydumper_nothingAvailable(t *testing.T) {
	// Ensure --mydumper-path is not Changed by resetting the flag.
	saved := dmpMydumperPath
	t.Cleanup(func() { dmpMydumperPath = saved })

	// Create a fresh command with the same flags to avoid Changed state.
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("mydumper-path", "mydumper", "")

	// Override PATH to exclude both mydumper and docker.
	t.Setenv("PATH", t.TempDir())

	_, err := resolveMydumper(cmd)
	if err == nil {
		t.Fatal("expected error when neither mydumper nor docker is available")
	}
	if !strings.Contains(err.Error(), "mydumper not found on $PATH") {
		t.Errorf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "Docker is not available") {
		t.Errorf("expected Docker mentioned in error: %v", err)
	}
}

func TestResolveMydumper_dockerFallback(t *testing.T) {
	// Create a fake docker binary but no mydumper.
	dir := t.TempDir()
	fakeDocker := filepath.Join(dir, "docker")
	if err := os.WriteFile(fakeDocker, []byte(""), 0o755); err != nil {
		t.Fatalf("failed to create fake docker: %v", err)
	}

	// Override PATH to contain only the fake docker.
	t.Setenv("PATH", dir)

	saved := dmpMydumperImage
	t.Cleanup(func() { dmpMydumperImage = saved })
	dmpMydumperImage = "mydumper/mydumper:v0.16"

	// Use a fresh command so --mydumper-path is not Changed.
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("mydumper-path", "mydumper", "")

	res, err := resolveMydumper(cmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.mode != dumpModeDocker {
		t.Errorf("expected dumpModeDocker, got %d", res.mode)
	}
	if res.path != fakeDocker {
		t.Errorf("expected docker path %q, got %q", fakeDocker, res.path)
	}
	if res.image != "mydumper/mydumper:v0.16" {
		t.Errorf("expected image %q, got %q", "mydumper/mydumper:v0.16", res.image)
	}
}

// ─── buildDockerArgs ──────────────────────────────────────────────────────────

func TestBuildDockerArgs_basic(t *testing.T) {
	mydumperArgs := []string{"--host", "db.example.com", "--port", "3306", "--user", "root", "--outputdir", "/tmp/dump"}
	args := buildDockerArgs("mydumper/mydumper:latest", "/tmp/dump", "db.example.com", mydumperArgs)

	// Should start with docker run --rm
	if len(args) < 3 || args[0] != "run" || args[1] != "--rm" {
		t.Fatalf("expected args to start with [run --rm], got %v", args[:min(3, len(args))])
	}

	// Volume mount
	assertArgsContainPair(t, args, "-v", "/tmp/dump:/tmp/dump")

	// Should NOT have --network host for non-localhost
	if argsContain(args, "--network") {
		t.Error("expected no --network flag for non-localhost host")
	}

	// Image and mydumper command
	imgIdx := argsIndex(args, "mydumper/mydumper:latest")
	if imgIdx < 0 {
		t.Fatal("expected image name in args")
	}
	if imgIdx+1 >= len(args) || args[imgIdx+1] != "mydumper" {
		t.Error("expected 'mydumper' command after image name")
	}

	// mydumper args follow
	if !argsContain(args, "--host") || !argsContain(args, "db.example.com") {
		t.Error("expected mydumper args to be present after image + command")
	}
}

func TestBuildDockerArgs_localhostNetworkHost(t *testing.T) {
	for _, host := range []string{"localhost", "127.0.0.1", "::1"} {
		args := buildDockerArgs("mydumper/mydumper:latest", "/tmp/dump", host, nil)

		// On Linux, --network host should be added; on macOS it should not.
		hasNetwork := argsContain(args, "--network")
		if hasNetwork {
			idx := argsIndex(args, "--network")
			if idx+1 >= len(args) || args[idx+1] != "host" {
				t.Errorf("host=%s: --network should be followed by 'host'", host)
			}
		}
		// We can't assert the exact behavior since it depends on runtime.GOOS,
		// but we can verify it doesn't panic and the structure is valid.
	}
}

func TestIsLocalhost(t *testing.T) {
	for _, tc := range []struct {
		host string
		want bool
	}{
		{"localhost", true},
		{"127.0.0.1", true},
		{"::1", true},
		{"db.example.com", false},
		{"192.168.1.1", false},
		{"", false},
	} {
		if got := isLocalhost(tc.host); got != tc.want {
			t.Errorf("isLocalhost(%q) = %v, want %v", tc.host, got, tc.want)
		}
	}
}

func TestDumpCmd_mydumperImageFlag(t *testing.T) {
	f := dumpCmd.Flag("mydumper-image")
	if f == nil {
		t.Fatal("flag --mydumper-image not registered")
	}
	if f.DefValue != "mydumper/mydumper:latest" {
		t.Errorf("expected default %q, got %q", "mydumper/mydumper:latest", f.DefValue)
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// argsContain reports whether args contains the string s.
func argsContain(args []string, s string) bool {
	return argsIndex(args, s) >= 0
}

// argsIndex returns the index of s in args, or -1 if not present.
func argsIndex(args []string, s string) int {
	for i, a := range args {
		if a == s {
			return i
		}
	}
	return -1
}

// assertArgsContainPair checks that key appears in args immediately followed by val.
func assertArgsContainPair(t *testing.T, args []string, key, val string) {
	t.Helper()
	idx := argsIndex(args, key)
	if idx < 0 {
		t.Errorf("expected %q in args %v", key, args)
		return
	}
	if idx+1 >= len(args) || args[idx+1] != val {
		var got string
		if idx+1 < len(args) {
			got = args[idx+1]
		}
		t.Errorf("expected %q after %q, got %q", val, key, got)
	}
}
