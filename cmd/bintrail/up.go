package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/console"
	"github.com/dbtrail/bintrail/internal/indexer"
)

var upCmd = &cobra.Command{
	Use:   "up",
	Short: "One command: preflight + init + stream (the friction-free quickstart)",
	Long: `Runs preflight checks (equivalent to 'bintrail doctor'), creates the
index tables if they do not exist (equivalent to 'bintrail init'), and starts
the replication stream (equivalent to 'bintrail stream'). Re-running 'bintrail
up' is idempotent: it skips work that's already done and resumes the stream
from its saved checkpoint.

This is the friction-free entry point for new bintrail installations. The
underlying 'init', 'snapshot', 'index', and 'stream' commands remain available
for advanced workflows (e.g. running them on separate machines or for
debugging).

If --server-id is not provided, a deterministic ID is derived from
host:user:dbname of the source DSN, mapped into a high range to reduce
collision odds with existing replicas.

Examples:

  bintrail up --source-dsn "$SRC" --index-dsn "$IDX"
  bintrail up --source-dsn "$SRC" --index-dsn "$IDX" --skip-doctor
  bintrail up --source-dsn "$SRC" --index-dsn "$IDX" --schemas mydb,otherdb`,
	RunE: runUp,
}

var (
	upSourceDSN   string
	upIndexDSN    string
	upServerID    uint32
	upSchemas     string
	upTables      string
	upBatchSize   int
	upCheckpoint  int
	upMetricsAddr string
	upPartitions  int
	upSkipDoctor  bool
	upFormat      string

	upConsole            bool
	upConsoleListen      string
	upConsoleToken       string
	upConsoleBaselineDir string
	upConsoleBaselineS3  string
	upConsoleServersFile string
)

func init() {
	upCmd.Flags().StringVar(&upSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	upCmd.Flags().StringVar(&upIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	upCmd.Flags().Uint32Var(&upServerID, "server-id", 0, "MySQL replica server ID (default: hash of source host:user:dbname)")
	upCmd.Flags().StringVar(&upSchemas, "schemas", "", "Comma-separated schemas to index (default: all user schemas)")
	upCmd.Flags().StringVar(&upTables, "tables", "", "Comma-separated tables to index (default: all)")
	upCmd.Flags().IntVar(&upBatchSize, "batch-size", 1000, "Events per batch INSERT")
	upCmd.Flags().IntVar(&upCheckpoint, "checkpoint", 10, "Checkpoint interval in seconds")
	upCmd.Flags().StringVar(&upMetricsAddr, "metrics-addr", "", "Address to expose Prometheus metrics (e.g. :9090); empty = disabled")
	upCmd.Flags().IntVar(&upPartitions, "partitions", 48, "Hourly partitions to create on first init")
	upCmd.Flags().BoolVar(&upSkipDoctor, "skip-doctor", false, "Skip the preflight checks (useful when you've already verified with `bintrail doctor`)")
	upCmd.Flags().StringVar(&upFormat, "format", "text", "Output format: text or json")
	upCmd.Flags().BoolVar(&upConsole, "console", false, "Also serve the read-only web console alongside the stream")
	upCmd.Flags().StringVar(&upConsoleListen, "console-listen", "127.0.0.1:8090", "Console bind address when --console is set")
	upCmd.Flags().StringVar(&upConsoleToken, "console-token", "", "Console access token (auto-generated for loopback binds when empty)")
	upCmd.Flags().StringVar(&upConsoleBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots; enables the console's point-in-time Reconstruct surface when --console is set")
	upCmd.Flags().StringVar(&upConsoleBaselineS3, "baseline-s3", "", "S3 prefix of baseline Parquet snapshots (s3://bucket/prefix/); enables Reconstruct when --console is set")
	upCmd.Flags().StringVar(&upConsoleServersFile, "console-servers-file", "", "Path to the console server registry YAML when --console is set (default ~/.config/bintrail/console-servers.yaml)")
	_ = upCmd.MarkFlagRequired("source-dsn")
	_ = upCmd.MarkFlagRequired("index-dsn")
	bindCommandEnv(upCmd)
	rootCmd.AddCommand(upCmd)
}

func runUp(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(upFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", upFormat)
	}

	// ── Phase 1: Preflight ──────────────────────────────────────────────────
	if !upSkipDoctor {
		fmt.Fprintln(os.Stderr, "=== Phase 1/3: Preflight checks ===")
		if err := runDoctorTo(cmd.Context(), os.Stderr, "text", upSourceDSN, upIndexDSN, upSchemas); err != nil {
			return fmt.Errorf("preflight failed (use --skip-doctor to bypass at your own risk): %w", err)
		}
		fmt.Fprintln(os.Stderr)
	}

	// ── Phase 2: Init ───────────────────────────────────────────────────────
	fmt.Fprintln(os.Stderr, "=== Phase 2/3: Initializing index database ===")
	if err := runUpInit(cmd); err != nil {
		return fmt.Errorf("init failed: %w", err)
	}
	fmt.Fprintln(os.Stderr)

	// ── Phase 3: Stream ─────────────────────────────────────────────────────
	fmt.Fprintln(os.Stderr, "=== Phase 3/3: Streaming ===")
	return runUpStream(cmd, args)
}

// runUpInit calls runInit with up's flag values. We share the parent context
// so Ctrl-C propagates during the init phase (table creation + optional S3
// bucket provisioning, both of which can block on remote IO).
func runUpInit(cmd *cobra.Command) error {
	initIndexDSN = upIndexDSN
	initPartitions = upPartitions
	initFormat = "text"
	initEncrypt = false
	initS3Bucket = ""
	initS3Region = "us-east-1"
	initS3ARN = ""

	subCmd := &cobra.Command{}
	subCmd.SetContext(cmd.Context())
	return runInit(subCmd, nil)
}

// runUpStream delegates to runStream after copying every up* flag value into
// the corresponding strm* package global. The snapshot step is handled inside
// runStream via auto-snapshot when no snapshot exists and --source-dsn is set.
func runUpStream(cmd *cobra.Command, args []string) error {
	serverID := upServerID
	if serverID == 0 {
		id, err := deriveServerID(upSourceDSN)
		if err != nil {
			return fmt.Errorf("cannot auto-derive --server-id from --source-dsn: %w (pass --server-id explicitly to bypass)", err)
		}
		serverID = id
		fmt.Fprintf(os.Stderr, "Auto-derived server-id from source DSN: %d\n", serverID)
	}
	populateStreamFlags(serverID)

	if !upConsole {
		return runStream(cmd, args)
	}
	return runUpStreamWithConsole(cmd, args)
}

// runUpStreamWithConsole serves the read-only web console in this same process,
// alongside the live stream. Both share one SIGINT/SIGTERM lifecycle: the signal
// cancels the context the console runs on, and it also propagates to runStream's
// child context, so a single Ctrl-C drains both. The console gets its own index
// DB connection (the stream owns its own).
func runUpStreamWithConsole(cmd *cobra.Command, args []string) error {
	resolveUpConsoleEnv(cmd)

	db, err := config.Connect(upIndexDSN)
	if err != nil {
		return fmt.Errorf("console: connect index database: %w", err)
	}
	defer db.Close()

	// Bring the index schema up to date before serving — runStream also does
	// this, but it runs after the console goroutine starts, so a legacy index DB
	// missing newer columns (e.g. connection_id) could fail early /api/events
	// requests in the startup window.
	if err := indexer.EnsureSchema(db); err != nil {
		return fmt.Errorf("console: schema migration: %w", err)
	}

	// The server registry gives `up --console` the same UI-managed switcher as
	// the standalone console; the stream's own index is the ephemeral default.
	// A corrupt file fails loud — `--console` is an explicit opt-in, and
	// silently starting without the operator's saved servers would look like
	// data loss.
	serversPath := upConsoleServersFile
	if serversPath == "" {
		serversPath = console.DefaultRegistryPath()
	}
	registry, err := console.LoadRegistry(serversPath)
	if err != nil {
		return fmt.Errorf("console: %w", err)
	}

	cfg, err := upConsoleConfig(db, upIndexDSN, upConsoleListen, upConsoleToken, upConsoleBaselineDir, upConsoleBaselineS3)
	if err != nil {
		return err
	}
	cfg.Registry = registry
	srv, err := console.New(cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cmd.SetContext(ctx)

	// Bind synchronously so a port conflict fails `up` fast — otherwise the
	// console would report "running" while the stream blocks for hours over a
	// server that never bound.
	ln, err := srv.Listen()
	if err != nil {
		return fmt.Errorf("console: cannot bind %s: %w", upConsoleListen, err)
	}
	// The console is the secondary job: log a mid-run crash when it happens (not
	// only at shutdown), but NEVER let it take down the stream, which is the
	// primary data-capture job.
	consoleDone := make(chan struct{}, 1)
	go func() {
		if err := srv.Serve(ctx, ln); err != nil {
			slog.Warn("console server exited with error", "error", err)
		}
		consoleDone <- struct{}{}
	}()
	fmt.Fprintf(os.Stderr, "\nConsole (read-only) is running. Open:\n\n    %s\n\n", srv.URL())

	streamErr := runStream(cmd, args)
	stop()        // drain the console even if the stream returned without a signal
	<-consoleDone // order the console goroutine's exit before the deferred db.Close()
	return streamErr
}

// resolveUpConsoleEnv applies the console-specific env vars to the upConsole*
// globals with flag > env > default precedence (mirrors runConsole). These are
// read directly rather than via the shared envBindings slice: that slice
// matches by flag name, and --baseline-dir/--baseline-s3 are also defined by
// `reconstruct` (as --listen is by `shim`/`init-shim`), so a global binding
// would leak the console's env vars into those commands. Extracted from
// runUpStreamWithConsole so the precedence dance is unit-testable.
func resolveUpConsoleEnv(cmd *cobra.Command) {
	if !cmd.Flags().Changed("console-listen") {
		if v := os.Getenv("BINTRAIL_CONSOLE_LISTEN"); v != "" {
			upConsoleListen = v
		}
	}
	if !cmd.Flags().Changed("console-token") {
		if v := os.Getenv("BINTRAIL_CONSOLE_TOKEN"); v != "" {
			upConsoleToken = v
		}
	}
	if !cmd.Flags().Changed("baseline-dir") {
		if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_DIR"); v != "" {
			upConsoleBaselineDir = v
		}
	}
	if !cmd.Flags().Changed("baseline-s3") {
		if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_S3"); v != "" {
			upConsoleBaselineS3 = v
		}
	}
	if !cmd.Flags().Changed("console-servers-file") {
		if v := os.Getenv("BINTRAIL_CONSOLE_SERVERS"); v != "" {
			upConsoleServersFile = v
		}
	}
}

// upConsoleConfig builds the console configuration for `up --console`. It serves
// the Phase 1 surface (events/recover/status) over the live index, plus the
// baseline-gated Reconstruct (Time-travel) surface when a baseline source is
// supplied — still no profile or --no-archive, so the reconstruct gate
// (baselineConfigured in internal/console/server.go, which owns dir-over-s3
// precedence) reduces to baseline presence. Extracted for testability (dbName
// extraction + DSN validation).
func upConsoleConfig(db *sql.DB, indexDSN, listen, token, baselineDir, baselineS3 string) (console.Config, error) {
	cfg, err := mysql.ParseDSN(indexDSN)
	if err != nil {
		return console.Config{}, fmt.Errorf("invalid --index-dsn: %w", err)
	}
	if cfg.DBName == "" {
		return console.Config{}, fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
	}
	return console.Config{
		DB:          db,
		DBName:      cfg.DBName,
		BootDSN:     indexDSN,
		Listen:      listen,
		Token:       token,
		BaselineDir: baselineDir,
		BaselineS3:  baselineS3,
	}, nil
}

// populateStreamFlags copies every up* package global into the corresponding
// strm* global, plus the resolved server-id. Extracted from runUpStream so the
// up→strm fan-out is unit-testable.
func populateStreamFlags(serverID uint32) {
	strmIndexDSN = upIndexDSN
	strmSourceDSN = upSourceDSN
	strmServerID = serverID
	strmStartFile = ""
	strmStartPos = 4
	strmStartGTID = ""
	strmBatchSize = upBatchSize
	strmSchemas = upSchemas
	strmTables = upTables
	strmCheckpoint = upCheckpoint
	strmMetricsAddr = upMetricsAddr
	strmSSLMode = "preferred"
	strmSSLCA = ""
	strmSSLCert = ""
	strmSSLKey = ""
	strmFormat = upFormat
	strmReset = false
	strmNoGapFill = false
	strmGapTimeout = 30
}

// deriveServerID returns a deterministic uint32 server-id by hashing the
// source DSN's host:user:dbname triple. The same DSN always produces the same
// ID, so `bintrail up` resumes cleanly across restarts without the user
// remembering what server-id they used last time.
//
// Returns an error when the DSN cannot be parsed — callers must handle this
// rather than silently substituting a non-deterministic value, because a
// per-invocation ID breaks the resume-from-checkpoint contract (MySQL would
// treat each restart as a new replica).
func deriveServerID(dsn string) (uint32, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return 0, fmt.Errorf("parse DSN: %w", err)
	}
	seed := fmt.Sprintf("%s|%s|%s", cfg.Addr, cfg.User, cfg.DBName)
	sum := sha256.Sum256([]byte(seed))
	raw := binary.BigEndian.Uint32(sum[:4])
	// Map into [100000000, 4294967294]: subtract floor from uint32 range, mod
	// into the resulting width, then add the floor back. Keeps the value high
	// enough that collisions with typical hand-picked replica server-ids are
	// unlikely.
	const floor = uint32(100000000)
	const width = uint32(4294967295 - floor) // 4194967295
	return (raw % width) + floor, nil
}
