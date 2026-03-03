package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/cliutil"
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
// the key file is also bind-mounted into the container.
func buildDockerArgs(image, outputDir, host string, mydumperArgs []string, encryptKeyPath string) []string {
	absOutput, err := filepath.Abs(outputDir)
	if err != nil {
		absOutput = outputDir
	}

	args := []string{
		"run", "--rm",
		"-v", absOutput + ":" + absOutput,
	}

	if encryptKeyPath != "" {
		absKey, err := filepath.Abs(encryptKeyPath)
		if err != nil {
			absKey = encryptKeyPath
		}
		args = append(args, "-v", absKey+":"+absKey+":ro")
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
	host, port, user, password, err := parseSourceDSN(dmpSourceDSN)
	if err != nil {
		return err
	}

	// 3. Parse schema and table filters.
	schemas := parseSchemaList(dmpSchemas)
	tables := parseSchemaList(dmpTables)

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

	// 6. Build mydumper args.
	mydumperArgs := buildMydumperArgs(host, port, user, password, dmpOutputDir, dmpThreads, schemas, tables, encryptKeyPath)

	// 7. Build the final command depending on resolution mode.
	var c *exec.Cmd
	switch res.mode {
	case dumpModeDocker:
		dockerArgs := buildDockerArgs(res.image, dmpOutputDir, host, mydumperArgs, encryptKeyPath)
		c = exec.CommandContext(cmd.Context(), res.path, dockerArgs...)
		slog.Info("starting dump via Docker", "image", res.image, "output_dir", dmpOutputDir)
	default:
		c = exec.CommandContext(cmd.Context(), res.path, mydumperArgs...)
		slog.Info("starting dump", "path", res.path, "output_dir", dmpOutputDir)
	}

	if dmpFormat != "json" {
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
	}
	if runErr := c.Run(); runErr != nil {
		return fmt.Errorf("mydumper failed: %w", runErr)
	}

	slog.Info("dump complete", "output_dir", dmpOutputDir)

	if dmpFormat == "json" {
		return outputJSON(struct {
			OutputDir string `json:"output_dir"`
		}{OutputDir: dmpOutputDir})
	}
	return nil
}

// buildMydumperArgs constructs the argument slice for a mydumper invocation.
// --compress-protocol and --complete-insert are always included.
// Schema filtering: single schema → --database; multiple → --regex.
// Table filtering: --tables-list with a comma-joined list.
// When encryptKeyPath is non-empty, --exec-per-thread and
// --exec-per-thread-extension are added for AES-256-CBC encryption.
func buildMydumperArgs(host string, port uint16, user, password, outputDir string,
	threads int, schemas, tables []string, encryptKeyPath string) []string {

	args := []string{
		"--host", host,
		"--port", strconv.Itoa(int(port)),
		"--user", user,
		"--outputdir", outputDir,
		"--threads", strconv.Itoa(threads),
		"--compress-protocol",
		"--complete-insert",
		"--sync-thread-lock-mode", "NO_LOCK",
		"--trx-tables",
	}

	if password != "" {
		args = append(args, "--password", password)
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

	return args
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
