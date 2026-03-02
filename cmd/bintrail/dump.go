package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
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
	dmpSourceDSN    string
	dmpOutputDir    string
	dmpSchemas      string
	dmpTables       string
	dmpMydumperPath string
	dmpThreads      int
	dmpFormat       string
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
	dumpCmd.Flags().IntVar(&dmpThreads, "threads", 4, "Number of mydumper dump threads")
	dumpCmd.Flags().StringVar(&dmpFormat, "format", "text", "Output format: text or json")
	_ = dumpCmd.MarkFlagRequired("source-dsn")
	_ = dumpCmd.MarkFlagRequired("output-dir")

	rootCmd.AddCommand(dumpCmd)
}

func runDump(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(dmpFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", dmpFormat)
	}

	// 1. Fail fast if mydumper is not installed.
	path, err := exec.LookPath(dmpMydumperPath)
	if err != nil {
		return fmt.Errorf("mydumper not found (%q): %w", dmpMydumperPath, err)
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
	mydumperArgs := buildMydumperArgs(host, port, user, password, dmpOutputDir, dmpThreads, schemas, tables)

	// 7. Run mydumper, streaming output to the terminal.
	slog.Info("starting dump", "path", path, "output_dir", dmpOutputDir)
	c := exec.CommandContext(cmd.Context(), path, mydumperArgs...)
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
func buildMydumperArgs(host string, port uint16, user, password, outputDir string,
	threads int, schemas, tables []string) []string {

	args := []string{
		"--host", host,
		"--port", strconv.Itoa(int(port)),
		"--user", user,
		"--outputdir", outputDir,
		"--threads", strconv.Itoa(threads),
		"--compress-protocol",
		"--complete-insert",
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

// releaseDumpLock closes the lockfile handle and removes the file.
func releaseDumpLock(f *os.File) {
	lockPath := f.Name()
	f.Close()
	os.Remove(lockPath)
}
