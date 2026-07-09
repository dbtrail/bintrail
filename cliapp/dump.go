package cliapp

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
)

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Invoke mydumper to create a logical dump of the source MySQL instance",
	Long: `Invokes mydumper to create a logical dump of the source MySQL instance.
Only one dump may run at a time (enforced by a lockfile). Any existing output
directory is removed before the dump begins.`,
	RunE: runDump,
}

var (
	dmpSourceDSN     string
	dmpOutputDir     string
	dmpSchemas       string
	dmpTables        string
	dmpMydumperPath  string
	dmpMydumperImage string
	dmpThreads       int
	dmpFormat        string
	dmpEncrypt       bool
	dmpEncryptKey    string
)

// dumpLockDir is a function returning the directory for the dump lockfile.
// It is a variable so tests can override it with a temp directory.
var dumpLockDir = os.TempDir

func init() {
	dumpCmd.Flags().StringVar(&dmpSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	dumpCmd.Flags().StringVar(&dmpOutputDir, "output-dir", "", "Directory for mydumper output (required)")
	dumpCmd.Flags().StringVar(&dmpSchemas, "schemas", "", "Comma-separated schema filter (e.g. mydb,otherdb)")
	dumpCmd.Flags().StringVar(&dmpTables, "tables", "", "Comma-separated table filter (e.g. mydb.orders,mydb.items)")
	dumpCmd.Flags().StringVar(&dmpMydumperPath, "mydumper-path", "mydumper", "Path to the mydumper binary")
	dumpCmd.Flags().StringVar(&dmpMydumperImage, "mydumper-image", "mydumper/mydumper:latest", "Docker image for mydumper (used when no local binary is found)")
	dumpCmd.Flags().IntVar(&dmpThreads, "threads", 4, "Number of mydumper dump threads")
	dumpCmd.Flags().StringVar(&dmpFormat, "format", "text", "Output format: text or json")
	dumpCmd.Flags().BoolVar(&dmpEncrypt, "encrypt", false, "Encrypt dump files at rest using AES-256-CBC (requires openssl on $PATH)")
	dumpCmd.Flags().StringVar(&dmpEncryptKey, "encrypt-key", "", "Path to encryption key file (default: ~/.config/bintrail/dump.key; generate with 'bintrail generate-key')")
	_ = dumpCmd.MarkFlagRequired("source-dsn")
	_ = dumpCmd.MarkFlagRequired("output-dir")
	bindCommandEnv(dumpCmd)

	rootCmd.AddCommand(dumpCmd)
}

// dumpMode indicates how mydumper will be invoked.
type dumpMode int

const (
	dumpModeLocal  dumpMode = iota // local binary
	dumpModeDocker                 // docker run
)

// dumpResolution holds the result of resolving how to invoke mydumper.
type dumpResolution struct {
	mode  dumpMode
	path  string // binary path (local) or docker path (docker)
	image string // docker image (only for dumpModeDocker)
}

// resolveMydumper determines how to invoke mydumper based on flag state.
// Priority: explicit --mydumper-path → $PATH lookup → Docker → error.
func resolveMydumper(cmd *cobra.Command) (dumpResolution, error) {
	if cmd.Flags().Changed("mydumper-path") {
		path, err := exec.LookPath(dmpMydumperPath)
		if err != nil {
			return dumpResolution{}, fmt.Errorf("mydumper not found at %q: %w", dmpMydumperPath, err)
		}
		return dumpResolution{mode: dumpModeLocal, path: path}, nil
	}

	if path, err := exec.LookPath("mydumper"); err == nil {
		if isShellScript(path) {
			slog.Warn("found mydumper on $PATH but it appears to be a shell script wrapper; skipping in favor of Docker",
				"path", path)
		} else {
			return dumpResolution{mode: dumpModeLocal, path: path}, nil
		}
	}

	dockerPath, err := exec.LookPath("docker")
	if err == nil {
		return dumpResolution{mode: dumpModeDocker, path: dockerPath, image: dmpMydumperImage}, nil
	}

	return dumpResolution{}, fmt.Errorf("mydumper not found on $PATH and Docker is not available; " +
		"install mydumper (https://github.com/mydumper/mydumper), install Docker, or use --mydumper-path")
}

// buildDockerArgs constructs the full argument slice for invoking mydumper via
// docker run. The output directory is bind-mounted at the same absolute path so
// downstream tools need no path translation. When encryptKeyPath is non-empty,
// the key file is also bind-mounted into the container. When defaultsFile is
// non-empty it is likewise bind-mounted read-only at the same path so the
// container's mydumper can read the source password from it via --defaults-file
// (keeping the secret off argv and out of `docker inspect`, #811).
func buildDockerArgs(image, outputDir, host string, mydumperArgs []string, encryptKeyPath, defaultsFile string) []string {
	absOutput, err := filepath.Abs(outputDir)
	if err != nil {
		absOutput = outputDir
	}

	args := []string{
		"run", "--rm",
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-v", absOutput + ":" + absOutput,
	}

	if encryptKeyPath != "" {
		absKey, err := filepath.Abs(encryptKeyPath)
		if err != nil {
			absKey = encryptKeyPath
		}
		args = append(args, "-v", absKey+":"+absKey+":ro")
	}

	if defaultsFile != "" {
		absDefaults, err := filepath.Abs(defaultsFile)
		if err != nil {
			absDefaults = defaultsFile
		}
		args = append(args, "-v", absDefaults+":"+absDefaults+":ro")
	}

	if isLocalhost(host) {
		if runtime.GOOS == "linux" {
			args = append(args, "--network", "host")
		} else {
			slog.Warn("source host is localhost but --network host only works on Linux; "+
				"use the Docker host IP (e.g. host.docker.internal) or set --source-dsn accordingly",
				"host", host, "os", runtime.GOOS)
		}
	}

	args = append(args, image, "mydumper")
	args = append(args, mydumperArgs...)
	return args
}

// isShellScript reports whether the file at path starts with a shebang (#!),
// indicating it is a script rather than a compiled binary.
func isShellScript(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	var buf [2]byte
	n, err := f.Read(buf[:])
	return n == 2 && err == nil && buf[0] == '#' && buf[1] == '!'
}

// isLocalhost reports whether the host refers to the local machine.
func isLocalhost(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func runDump(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(dmpFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", dmpFormat)
	}

	// 0. Resolve encryption key path.
	var encryptKeyPath string
	if dmpEncrypt {
		var err error
		encryptKeyPath, err = resolveEncryptKey(dmpEncryptKey)
		if err != nil {
			return err
		}
	}

	// 1. Resolve how to invoke mydumper.
	res, err := resolveMydumper(cmd)
	if err != nil {
		return err
	}

	// 2. Parse source DSN.
	host, port, user, password, err := config.ParseSourceDSN(dmpSourceDSN)
	if err != nil {
		return err
	}

	// 3. Parse schema and table filters.
	schemas := cliutil.ParseSchemaList(dmpSchemas)
	tables := cliutil.ParseSchemaList(dmpTables)

	// 4. Acquire dump lock — only one dump at a time.
	lockFile, err := acquireDumpLock()
	if err != nil {
		return fmt.Errorf("another dump is already running: %w", err)
	}
	defer releaseDumpLock(lockFile)

	// 5. Remove existing output directory.
	if err := os.RemoveAll(dmpOutputDir); err != nil {
		return fmt.Errorf("failed to remove existing output directory %q: %w", dmpOutputDir, err)
	}

	// 6. Probe mydumper version and build args.
	// --sync-thread-lock-mode and --trx-tables require mydumper >= 0.18 (see
	// mydumperSupportsLockMode). Distro apt packages ship older builds —
	// Ubuntu 24.04 and Debian bookworm both package upstream 0.10.1, whose
	// binary self-reports 0.10.0 — so we must not pass the flags
	// unconditionally or the dump fails (#219, #460). Docker mode is NOT
	// version-probed: a pinned --mydumper-image older than 0.18 fails with
	// "unknown option", which is why the docs' pin examples stay >= 0.18.
	supportsLockMode := true
	if res.mode == dumpModeLocal {
		major, minor, patch, verErr := mydumperVersion(res.path)
		if verErr != nil {
			slog.Warn("could not determine mydumper version; omitting --sync-thread-lock-mode and --trx-tables for safety",
				"error", verErr)
			supportsLockMode = false
		} else if !mydumperSupportsLockMode(major, minor) {
			slog.Warn("mydumper version is older than 0.18; omitting --sync-thread-lock-mode and --trx-tables — the dump may hold heavier locks",
				"version", fmt.Sprintf("%d.%d.%d", major, minor, patch))
			supportsLockMode = false
		}
	}
	// The source password must never reach mydumper's argv (visible in
	// `ps aux` / /proc/<pid>/cmdline and, under Docker, in `docker inspect`).
	// Docker mode delivers it via a 0600 defaults-file bind-mounted read-only;
	// local mode delivers it via MYSQL_PWD in the child env below (#811).
	var defaultsFile string
	if password != "" && res.mode == dumpModeDocker {
		var cleanup func()
		var derr error
		defaultsFile, cleanup, derr = writeMydumperDefaultsFile(password)
		if derr != nil {
			return derr
		}
		defer cleanup()
	}
	mydumperArgs := buildMydumperArgs(host, port, user, defaultsFile, dmpOutputDir, dmpThreads, schemas, tables, encryptKeyPath, supportsLockMode)

	// 7. Build the final command depending on resolution mode.
	var c *exec.Cmd
	switch res.mode {
	case dumpModeDocker:
		dockerArgs := buildDockerArgs(res.image, dmpOutputDir, host, mydumperArgs, encryptKeyPath, defaultsFile)
		c = exec.CommandContext(cmd.Context(), res.path, dockerArgs...)
		slog.Info("starting dump via Docker", "image", res.image, "output_dir", dmpOutputDir)
	default:
		c = exec.CommandContext(cmd.Context(), res.path, mydumperArgs...)
		// Pass the password out of band so it stays off argv. MYSQL_PWD is
		// honored by the MySQL client library mydumper links against and sits
		// in the child's mode-0400 /proc/<pid>/environ, not world-readable
		// cmdline (#811).
		if password != "" {
			c.Env = append(os.Environ(), "MYSQL_PWD="+password)
		}
		slog.Info("starting dump", "path", res.path, "output_dir", dmpOutputDir)
	}

	// Always capture stderr into a buffer so the error message can include
	// the actual diagnostic — "exit status 1" by itself hides why mydumper
	// failed (auth plugin mismatch, missing privilege, full disk, etc.).
	// In text mode we ALSO passthrough to the user's terminal so they see
	// progress live; in JSON mode the caller is parsing stdout so we keep
	// stderr buffered only.
	var stderrBuf bytes.Buffer
	if dmpFormat != "json" {
		c.Stdout = os.Stdout
		c.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
	} else {
		c.Stderr = &stderrBuf
	}
	// Captured immediately before invoking mydumper so it approximates
	// mydumper's own dump-start instant, but as this process's UTC wall
	// clock rather than mydumper's (possibly non-UTC, possibly
	// containerized) local time. Recorded as a sidecar below so
	// `bintrail baseline` can anchor on it instead of re-parsing mydumper's
	// ambiguous "Started dump at" line (#768).
	dumpStartedAt := time.Now().UTC()
	if runErr := c.Run(); runErr != nil {
		if stderr := strings.TrimSpace(stderrBuf.String()); stderr != "" {
			return fmt.Errorf("mydumper failed: %w; stderr: %s", runErr, stderr)
		}
		return fmt.Errorf("mydumper failed: %w", runErr)
	}

	slog.Info("dump complete", "output_dir", dmpOutputDir)

	// Best-effort: a write failure here just means baseline conversion falls
	// back to mydumper's own (TZ-sensitive) "Started dump at" line rather
	// than failing the dump itself.
	if err := baseline.WriteStartedAtMarker(dmpOutputDir, dumpStartedAt); err != nil {
		slog.Warn("could not write dump-start marker; baseline conversion will fall back to mydumper's local-time 'Started dump at' line",
			"output_dir", dmpOutputDir, "error", err)
	}

	if dmpFormat == "json" {
		return cliutil.OutputJSON(struct {
			OutputDir string `json:"output_dir"`
		}{OutputDir: dmpOutputDir})
	}
	return nil
}

// mydumperVersion runs `<path> --version` and parses the version triple via
// parseMydumperVersion. Returns (0, 0, 0, err) on any failure — the caller
// should treat an unparseable version conservatively (assume oldest).
func mydumperVersion(path string) (major, minor, patch int, err error) {
	out, err := exec.Command(path, "--version").CombinedOutput()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("run %s --version: %w", path, err)
	}
	return parseMydumperVersion(string(out))
}

// parseMydumperVersion extracts the major.minor.patch triple from mydumper
// --version output (e.g. "mydumper 0.10.0, built against MySQL 8.0.36"
// → 0, 10, 0). Extracted from mydumperVersion so the parsing logic is
// directly unit-testable without shelling out to a real binary.
func parseMydumperVersion(output string) (major, minor, patch int, err error) {
	line := strings.SplitN(output, "\n", 2)[0]
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return 0, 0, 0, fmt.Errorf("unexpected --version output: %q", line)
	}
	ver := strings.TrimRight(parts[1], ",")
	n, scanErr := fmt.Sscanf(ver, "%d.%d.%d", &major, &minor, &patch)
	if scanErr != nil || n != 3 {
		return 0, 0, 0, fmt.Errorf("cannot parse version %q from %q", ver, line)
	}
	return major, minor, patch, nil
}

// mydumperSupportsLockMode reports whether a mydumper version understands
// --sync-thread-lock-mode and --trx-tables. The flags landed in mydumper
// 0.18.1 — NOT 0.11, whose light-locking options were --less-locking /
// --trx-consistency-only (which --trx-tables replaced; --no-locks survives
// in modern versions). The gate previously sat at 0.11, handing 0.11–0.17
// builds flags they reject with "unknown option" (#460). No 0.18.0 was ever
// released — the 0.18 series starts at 0.18.1 — so gating on (major, minor)
// alone is exact.
func mydumperSupportsLockMode(major, minor int) bool {
	return major > 0 || minor >= 18
}

// buildMydumperArgs constructs the argument slice for a mydumper invocation.
// --compress-protocol and --complete-insert are always included.
// When supportsLockMode is true (mydumper >= 0.18, see
// mydumperSupportsLockMode), --sync-thread-lock-mode and --trx-tables are
// included for lighter locking. When false (older builds, e.g. distro apt
// packages), they are omitted so the dump works without error (#219, #460).
// Schema filtering: single schema → --database; multiple → --regex.
// Table filtering: --tables-list with a comma-joined list.
// When encryptKeyPath is non-empty, --exec-per-thread and
// --exec-per-thread-extension are added for AES-256-CBC encryption.
//
// The source password is NEVER placed on argv — it would be world-readable via
// `ps aux` / /proc/<pid>/cmdline and, under Docker, in `docker inspect`. It is
// delivered out of band: locally via MYSQL_PWD in the child env, and under
// Docker via a 0600 defaults-file bind-mounted read-only. When defaultsFile is
// non-empty its path is referenced with --defaults-file so mydumper reads the
// password from it (#811).
func buildMydumperArgs(host string, port uint16, user, defaultsFile, outputDir string,
	threads int, schemas, tables []string, encryptKeyPath string, supportsLockMode bool) []string {

	args := []string{
		"--host", host,
		"--port", strconv.Itoa(int(port)),
		"--user", user,
		"--threads", strconv.Itoa(threads),
		"--compress-protocol",
		"--complete-insert",
	}

	if supportsLockMode {
		args = append(args, "--sync-thread-lock-mode", "NO_LOCK", "--trx-tables")
	}

	if defaultsFile != "" {
		args = append(args, "--defaults-file", defaultsFile)
	}

	switch len(schemas) {
	case 1:
		args = append(args, "--database", schemas[0])
	default:
		if len(schemas) > 1 {
			regex := "^(" + strings.Join(schemas, "|") + ")\\."
			args = append(args, "--regex", regex)
		}
	}

	if len(tables) > 0 {
		args = append(args, "--tables-list", strings.Join(tables, ","))
	}

	if encryptKeyPath != "" {
		absKey, err := filepath.Abs(encryptKeyPath)
		if err != nil {
			absKey = encryptKeyPath
		}
		args = append(args,
			"--exec-per-thread", fmt.Sprintf("openssl enc -aes-256-cbc -pbkdf2 -pass file:%s", absKey),
			"--exec-per-thread-extension", ".enc")
	}

	// --outputdir must be last: Docker wrapper scripts commonly use ${@: -1}
	// (the last argument) for the volume mount path.
	args = append(args, "--outputdir", outputDir)

	return args
}

// writeMydumperDefaultsFile writes the source password to a fresh temp file in
// MySQL option-file format (0600) so mydumper can read it via --defaults-file
// instead of taking it on argv. Used by Docker mode, where a passed-through
// MYSQL_PWD would still surface in `docker inspect` Config.Env. The returned
// cleanup removes the file and must be deferred by the caller even on a later
// error. os.CreateTemp opens O_EXCL with mode 0600, so creation is atomic and
// the secret is never briefly world-readable (#811).
func writeMydumperDefaultsFile(password string) (path string, cleanup func(), err error) {
	f, err := os.CreateTemp("", "bintrail-mydumper-*.cnf")
	if err != nil {
		return "", nil, fmt.Errorf("create mydumper defaults file: %w", err)
	}
	name := f.Name()
	cleanup = func() { _ = os.Remove(name) }
	// Defensive: CreateTemp already uses 0600, but pin it in case of an
	// unusual umask/implementation.
	if chmodErr := f.Chmod(0o600); chmodErr != nil {
		_ = f.Close()
		cleanup()
		return "", nil, fmt.Errorf("chmod mydumper defaults file: %w", chmodErr)
	}
	// Password under both [client] and [mydumper] so any mydumper build picks
	// it up regardless of which group it reads.
	v := escapeMyCnfValue(password)
	content := "[client]\npassword=" + v + "\n[mydumper]\npassword=" + v + "\n"
	if _, werr := f.WriteString(content); werr != nil {
		_ = f.Close()
		cleanup()
		return "", nil, fmt.Errorf("write mydumper defaults file: %w", werr)
	}
	if cerr := f.Close(); cerr != nil {
		cleanup()
		return "", nil, fmt.Errorf("close mydumper defaults file: %w", cerr)
	}
	return name, cleanup, nil
}

// escapeMyCnfValue encodes a value for a MySQL option file. The value is wrapped
// in double quotes (so a '#', ';', or whitespace is not misread as a comment or
// truncated) with backslash and double-quote escaped, matching the escape
// sequences the option-file parser recognizes inside a quoted value.
func escapeMyCnfValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return `"` + v + `"`
}

// extractSchemasFromTables derives unique schema names from a list of
// "db.table" entries. Entries without a dot are silently skipped.
// Returns nil for an empty input or when all entries lack a dot.
func extractSchemasFromTables(tables []string) []string {
	if len(tables) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	var result []string
	for _, t := range tables {
		dot := strings.IndexByte(t, '.')
		if dot < 0 {
			continue
		}
		schema := t[:dot]
		if _, ok := seen[schema]; !ok {
			seen[schema] = struct{}{}
			result = append(result, schema)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// ─── Lock mechanism ───────────────────────────────────────────────────────────

const dumpLockFilename = "bintrail-dump.lock"

func dumpLockPath() string {
	return filepath.Join(dumpLockDir(), dumpLockFilename)
}

// acquireDumpLock atomically creates the lockfile and writes the current PID.
// If the file already exists and contains a live PID, it returns an error.
// A stale lockfile (dead PID) is removed and the acquisition is retried once.
func acquireDumpLock() (*os.File, error) {
	lockPath := dumpLockPath()
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		return writePID(f, lockPath)
	}
	if !os.IsExist(err) {
		return nil, fmt.Errorf("failed to create lock file: %w", err)
	}

	// Lock exists — check whether the owning process is still alive.
	data, readErr := os.ReadFile(lockPath)
	if readErr != nil {
		return nil, fmt.Errorf("lock file exists and could not be read: %w", readErr)
	}
	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
	if parseErr == nil {
		proc, findErr := os.FindProcess(pid)
		if findErr == nil {
			if sigErr := proc.Signal(syscall.Signal(0)); sigErr == nil {
				// Process is alive — a real concurrent dump is running.
				return nil, fmt.Errorf("dump already running (PID %d)", pid)
			}
		}
	}

	// Stale lock — remove and retry once.
	if removeErr := os.Remove(lockPath); removeErr != nil && !os.IsNotExist(removeErr) {
		return nil, fmt.Errorf("failed to remove stale lock file: %w", removeErr)
	}
	f, err = os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock after removing stale file: %w", err)
	}
	return writePID(f, lockPath)
}

func writePID(f *os.File, lockPath string) (*os.File, error) {
	if _, werr := fmt.Fprintf(f, "%d", os.Getpid()); werr != nil {
		f.Close()
		os.Remove(lockPath)
		return nil, fmt.Errorf("failed to write lock PID: %w", werr)
	}
	return f, nil
}

// resolveEncryptKey returns the absolute path to the encryption key file.
// If keyPath is empty, defaultKeyPath() is used. The file must exist and
// be readable. Additionally, openssl must be available on $PATH since
// mydumper shells out to it for encryption.
func resolveEncryptKey(keyPath string) (string, error) {
	if keyPath == "" {
		keyPath = defaultKeyPath()
	}
	absPath, err := filepath.Abs(keyPath)
	if err != nil {
		return "", fmt.Errorf("resolve key path: %w", err)
	}
	if _, err := os.Stat(absPath); err != nil {
		return "", fmt.Errorf("encryption key file not found at %s; generate one with 'bintrail generate-key'", absPath)
	}
	if _, err := exec.LookPath("openssl"); err != nil {
		return "", fmt.Errorf("openssl not found on $PATH; it is required for dump encryption")
	}
	return absPath, nil
}

// releaseDumpLock closes the lockfile handle and removes the file.
func releaseDumpLock(f *os.File) {
	lockPath := f.Name()
	f.Close()
	os.Remove(lockPath)
}
