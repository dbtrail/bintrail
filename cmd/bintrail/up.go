package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/console"
	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/serverid"
	"github.com/dbtrail/bintrail/internal/streamrun"
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

	upRotateRetain    string
	upRotateInterval  string
	upRotateAddFuture int
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
	upCmd.Flags().StringVar(&upRotateRetain, "rotate-retain", "30d", "Built-in rotation: drop index partitions older than this (Nd/Nh; \"off\" disables)")
	upCmd.Flags().StringVar(&upRotateInterval, "rotate-interval", "1h", "Built-in rotation: how often to run a rotation cycle")
	upCmd.Flags().IntVar(&upRotateAddFuture, "rotate-add-future", 3, "Built-in rotation: keep at least N future hourly partitions ready")
	// --source-dsn is validated in runUp instead of MarkFlagRequired: with
	// --console the daemon may start source-less (zero-config install) and
	// sources are added from the UI.
	_ = upCmd.MarkFlagRequired("index-dsn")
	bindCommandEnv(upCmd)
	rootCmd.AddCommand(upCmd)
}

func runUp(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(upFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", upFormat)
	}
	// Validate the built-in rotation settings up front so a typo fails fast,
	// before any phase runs. The loop itself starts with phase 3. The Changed
	// check covers flag and env alike (bindCommandEnv marks env-set flags
	// Changed); an explicitly-chosen retention disables the upgrade guard.
	var err error
	upRotationCfg, err = parseUpRotation(upRotateRetain, upRotateInterval, upRotateAddFuture,
		cmd.Flags().Changed("rotate-retain"))
	if err != nil {
		return err
	}
	// --source-dsn is required for the classic single-stream `up`, but with
	// --console the daemon can start with NO source at all: it serves the
	// console + control plane, and sources are added from the UI ("+ Add
	// server" runs the preflight, provisions a per-source index, and starts
	// streaming). That is the zero-config install path.
	if upSourceDSN == "" && !upConsole {
		return fmt.Errorf("--source-dsn is required (or pass --console to start source-less and add servers from the UI)")
	}

	// Containerized installs start bintrail and the index MySQL together, and
	// the official mysql image briefly accepts-then-drops connections during
	// its first initialization — a single connect attempt turns first boot
	// into a restart loop (regenerating the console token each time). Under
	// --console (the compose path), wait for the index instead of dying;
	// plain CLI runs keep today's fail-fast behavior.
	if upConsole {
		if err := waitForIndexMySQL(cmd.Context(), upIndexDSN, 90*time.Second); err != nil {
			return fmt.Errorf("index MySQL did not become reachable: %w", err)
		}
	}

	// ── Phase 1: Preflight ──────────────────────────────────────────────────
	if upSourceDSN == "" {
		fmt.Fprintln(os.Stderr, "=== Phase 1/3: Preflight checks ===")
		fmt.Fprintln(os.Stderr, "No source configured yet — the preflight runs when you add a server from the console.")
		fmt.Fprintln(os.Stderr)
	} else if !upSkipDoctor {
		fmt.Fprintln(os.Stderr, "=== Phase 1/3: Preflight checks ===")
		// The capacity projection uses up's actual rotation window (0 when
		// built-in rotation is disabled → it reports unbounded growth). Its
		// FAIL is ADVISORY here: blocking the stream over a disk forecast
		// would manufacture the very forensic gap it warns about (an
		// unattended reboot would crash-loop instead of capturing while
		// there is still room). Standalone `doctor` keeps full FAIL
		// semantics for CI.
		preflight := buildDoctorReport(cmd.Context(), upSourceDSN, upIndexDSN, upSchemas, upRotationCfg.retain)
		if err := preflight.Write(os.Stderr, "text"); err != nil {
			return fmt.Errorf("write preflight report: %w", err)
		}
		fatal, warnCapacity := upPreflightOutcome(preflight)
		if fatal != nil {
			return fmt.Errorf("preflight failed (use --skip-doctor to bypass at your own risk): %w", fatal)
		}
		if warnCapacity {
			fmt.Fprintln(os.Stderr, "WARNING: the index disk capacity check FAILED — starting anyway (capturing beats not capturing), but act on its remediation before the volume fills.")
		}
		fmt.Fprintln(os.Stderr)
	}

	// ── Phase 2: Init ───────────────────────────────────────────────────────
	fmt.Fprintln(os.Stderr, "=== Phase 2/3: Initializing index database ===")
	if err := runUpInit(cmd); err != nil {
		return fmt.Errorf("init failed: %w", err)
	}
	fmt.Fprintln(os.Stderr)

	// ── Phase 3: Stream (or console-only daemon when no source yet) ─────────
	if upSourceDSN == "" {
		fmt.Fprintln(os.Stderr, "=== Phase 3/3: Console + control plane ===")
		return runUpConsoleOnly(cmd)
	}
	fmt.Fprintln(os.Stderr, "=== Phase 3/3: Streaming ===")
	return runUpStream(cmd, args)
}

// upPreflightOutcome maps the preflight report to up's boot decision: fatal
// is non-nil for any non-advisory failure (boot refused); warnCapacity is
// true when the capacity projection was the ONLY failure — boot proceeds,
// but the operator must hear about it (the caller prints the WARNING).
// Extracted so the advisory semantics are unit-testable: losing either half
// would silently change what blocks `up` or swallow the disk-full signal.
func upPreflightOutcome(r *doctorReport) (fatal error, warnCapacity bool) {
	if err := r.ErrExcluding(capacityCheckName); err != nil {
		return err, false
	}
	return nil, r.Err() != nil
}

// waitForIndexMySQL retries a server-level connection (database name
// stripped — init may not have created it yet) until the index MySQL accepts
// connections or the timeout elapses. Progress is logged so a compose
// first-boot reads as "waiting for MySQL", not as a crash loop.
func waitForIndexMySQL(ctx context.Context, dsn string, timeout time.Duration) error {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}
	cfg.DBName = ""
	serverDSN := cfg.FormatDSN()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for attempt := 0; ; attempt++ {
		db, err := config.Connect(serverDSN)
		if err == nil {
			db.Close()
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return lastErr
		}
		if attempt%5 == 0 {
			fmt.Fprintf(os.Stderr, "Waiting for index MySQL at %s…\n", cfg.Addr)
		}
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// runUpConsoleOnly is the zero-config daemon: no initial source, just the
// index, the console, and the control-plane supervisor. Every source is added
// (and resumed at boot, via Reconcile) from the UI. Mirrors
// runUpStreamWithConsole minus the main stream.
func runUpConsoleOnly(cmd *cobra.Command) error {
	resolveUpConsoleEnv(cmd)

	db, err := config.Connect(upIndexDSN)
	if err != nil {
		return fmt.Errorf("console: connect index database: %w", err)
	}
	defer db.Close()
	if err := indexer.EnsureSchema(db); err != nil {
		return fmt.Errorf("console: schema migration: %w", err)
	}

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

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	supervisor := newMonitorSupervisor(ctx, upIndexDSN, registry)
	cfg.MonitorCtrl = supervisor

	// Built-in rotation covers the boot index plus every per-source database
	// the control plane provisions — the unattended quickstart's real data
	// lives in the latter.
	startUpRotation(ctx, upRotationCfg, func() []string {
		return append([]string{upIndexDSN}, supervisor.ActiveIndexDSNs()...)
	})

	srv, err := console.New(cfg)
	if err != nil {
		return err
	}
	ln, err := srv.Listen()
	if err != nil {
		return fmt.Errorf("console: cannot bind %s: %w", upConsoleListen, err)
	}

	// One daemon-level /metrics endpoint for ALL supervised streams — the
	// Prometheus registry is process-global and every stream metric carries
	// a "source" label (the entry ID), so per-stream servers are unnecessary
	// (and would fight over the bind). Synchronous bind: fails fast, like
	// the console bind.
	if upMetricsAddr != "" {
		stopMetrics, err := streamrun.StartMetricsServer(upMetricsAddr)
		if err != nil {
			return err
		}
		defer stopMetrics()
	}

	fmt.Fprintf(os.Stderr, "\nConsole is running — open it and add the MySQL servers to watch:\n\n    %s\n\n", srv.URL())
	go supervisor.Reconcile(registry)

	serveErr := srv.Serve(ctx, ln)
	supervisor.Shutdown() // final checkpoints for every monitored stream
	return serveErr
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
		id, err := serverid.DeriveServerID(upSourceDSN)
		if err != nil {
			return fmt.Errorf("cannot auto-derive --server-id from --source-dsn: %w (pass --server-id explicitly to bypass)", err)
		}
		serverID = id
		fmt.Fprintf(os.Stderr, "Auto-derived server-id from source DSN: %d\n", serverID)
	}
	populateStreamFlags(serverID)

	if !upConsole {
		// Classic single-stream up: rotate the boot index only. The loop gets
		// its own signal-bound context (cmd's root context is never cancelled
		// by SIGINT — runStream installs its handler on a derived child), so
		// rotation stops when the stream starts draining, same as the console
		// paths.
		rotCtx, rotStop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer rotStop()
		startUpRotation(rotCtx, upRotationCfg, func() []string {
			return []string{upIndexDSN}
		})
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

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cmd.SetContext(ctx)

	// The control-plane supervisor: "+ Add server" in the console starts real
	// monitoring through it. Streams live on the daemon context (ctx), not on
	// the HTTP requests that start them.
	supervisor := newMonitorSupervisor(ctx, upIndexDSN, registry)
	cfg.MonitorCtrl = supervisor

	// Built-in rotation: boot index + every per-source database the control
	// plane provisions, on the daemon lifecycle.
	startUpRotation(ctx, upRotationCfg, func() []string {
		return append([]string{upIndexDSN}, supervisor.ActiveIndexDSNs()...)
	})

	// With the console comes the multi-stream control plane, so /metrics is
	// served once at the daemon level (per-source "source" labels keep the
	// series apart). Clear the flag fan-out so the main stream does not
	// double-bind the same address inside streamrun.One. Synchronous bind:
	// fails fast, like the console bind below.
	if upMetricsAddr != "" {
		stopMetrics, err := streamrun.StartMetricsServer(upMetricsAddr)
		if err != nil {
			return err
		}
		defer stopMetrics()
		strmMetricsAddr = ""
	}

	srv, err := console.New(cfg)
	if err != nil {
		return err
	}

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

	// Resume whatever the operator had monitoring before the restart —
	// desired state lives in the registry, positions in each per-source
	// stream_state checkpoint.
	go supervisor.Reconcile(registry)

	streamErr := runStream(cmd, args)
	stop()                // drain the console even if the stream returned without a signal
	<-consoleDone         // order the console goroutine's exit before the deferred db.Close()
	supervisor.Shutdown() // final checkpoints for every monitored stream
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
		// MonitorCtrl (the control-plane supervisor) is wired by the caller —
		// runUpStreamWithConsole — because it needs the registry and the
		// daemon lifecycle context, which this config builder doesn't have.
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
