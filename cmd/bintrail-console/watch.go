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
	"github.com/dbtrail/bintrail/internal/doctor"
	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/rotation"
	"github.com/dbtrail/bintrail/internal/serverid"
	"github.com/dbtrail/bintrail/internal/streamdeps"
	"github.com/dbtrail/bintrail/internal/streamrun"
)

var watchCmd = &cobra.Command{
	Use:   "watch",
	Short: "Watch one or more MySQL servers: stream + console + control plane in one daemon",
	Long: `Runs the combined capture-and-observe daemon (the standalone successor to
'bintrail up --console'): preflight checks, index initialization, the live
replication stream, AND the read-only web console with its control plane —
all in one process sharing one SIGINT/SIGTERM lifecycle.

--source-dsn is optional: without it the daemon starts source-less (the
zero-config install) serving the console + control plane only, and sources
are added from the UI ("+ Add server" runs the preflight, provisions a
per-source index database, and starts streaming).

If --server-id is not provided, a deterministic ID is derived from
host:user:dbname of the source DSN, mapped into a high range to reduce
collision odds with existing replicas.

Examples:

  bintrail-console watch --index-dsn "$IDX"
  bintrail-console watch --source-dsn "$SRC" --index-dsn "$IDX"
  bintrail-console watch --source-dsn "$SRC" --index-dsn "$IDX" --schemas mydb`,
	RunE: runWatch,
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

	upConsoleListen      string
	upConsoleToken       string
	upConsoleBaselineDir string
	upConsoleBaselineS3  string
	upConsoleServersFile string

	upRotateRetain    string
	upRotateInterval  string
	upRotateAddFuture int

	// upRotationCfg holds the parsed built-in-rotation settings from runWatch's
	// validation, read at the phase-3 start sites (the cobra-accumulator
	// pattern; the parsing and the loop itself live in internal/rotation).
	upRotationCfg rotation.Settings
)

// watchEnvBindings maps watch's flags to their BINTRAIL_ environment
// variables — the subset of the core CLI's envBindings (cmd/bintrail/
// envload.go) that exists on this command. Applied by bindWatchEnv with the
// same semantics: an env-set flag is marked Changed, which both satisfies
// MarkFlagRequired and keeps rotation.ParseSettings' explicit-retention
// detection working for env-configured daemons (the compose path).
var watchEnvBindings = []struct {
	Flag   string
	EnvVar string
}{
	{"index-dsn", "BINTRAIL_INDEX_DSN"},
	{"source-dsn", "BINTRAIL_SOURCE_DSN"},
	{"schemas", "BINTRAIL_SCHEMAS"},
	{"tables", "BINTRAIL_TABLES"},
	{"server-id", "BINTRAIL_SERVER_ID"},
	{"batch-size", "BINTRAIL_BATCH_SIZE"},
	{"metrics-addr", "BINTRAIL_METRICS_ADDR"},
	{"rotate-retain", "BINTRAIL_ROTATE_RETAIN"},
	{"rotate-interval", "BINTRAIL_ROTATE_INTERVAL"},
	{"rotate-add-future", "BINTRAIL_ROTATE_ADD_FUTURE"},
}

// bindWatchEnv loads the env file (once) and applies BINTRAIL_* environment
// variables to watch's flags, mirroring the core CLI's bindCommandEnv. The
// console-specific BINTRAIL_CONSOLE_* vars are NOT bound here — they are read
// directly in resolveUpConsoleEnv, because --baseline-dir/--baseline-s3 also
// exist on core commands and the direct read keeps the precedence dance in
// one auditable place.
func bindWatchEnv(cmd *cobra.Command) {
	envOnce.Do(loadEnvFile)
	for _, b := range watchEnvBindings {
		v := os.Getenv(b.EnvVar)
		if v == "" {
			continue
		}
		if cmd.Flags().Lookup(b.Flag) == nil {
			continue
		}
		if err := cmd.Flags().Set(b.Flag, v); err != nil {
			fmt.Fprintf(os.Stderr, "warning: cannot set --%s from %s: %v\n", b.Flag, b.EnvVar, err)
		}
	}
}

func init() {
	watchCmd.Flags().StringVar(&upSourceDSN, "source-dsn", "", "DSN for the source MySQL server (omit to start source-less and add servers from the UI)")
	watchCmd.Flags().StringVar(&upIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	watchCmd.Flags().Uint32Var(&upServerID, "server-id", 0, "MySQL replica server ID (default: hash of source host:user:dbname)")
	watchCmd.Flags().StringVar(&upSchemas, "schemas", "", "Comma-separated schemas to index (default: all user schemas)")
	watchCmd.Flags().StringVar(&upTables, "tables", "", "Comma-separated tables to index (default: all)")
	watchCmd.Flags().IntVar(&upBatchSize, "batch-size", 1000, "Events per batch INSERT")
	watchCmd.Flags().IntVar(&upCheckpoint, "checkpoint", 10, "Checkpoint interval in seconds")
	watchCmd.Flags().StringVar(&upMetricsAddr, "metrics-addr", "", "Address to expose Prometheus metrics (e.g. :9090); empty = disabled")
	watchCmd.Flags().IntVar(&upPartitions, "partitions", 48, "Hourly partitions to create on first init")
	watchCmd.Flags().BoolVar(&upSkipDoctor, "skip-doctor", false, "Skip the preflight checks (useful when you've already verified with `bintrail doctor`)")
	watchCmd.Flags().StringVar(&upFormat, "format", "text", "Output format: text or json")
	watchCmd.Flags().StringVar(&upConsoleListen, "console-listen", "127.0.0.1:8090", "Console bind address")
	watchCmd.Flags().StringVar(&upConsoleToken, "console-token", "", "Console access token (auto-generated for loopback binds when empty)")
	watchCmd.Flags().StringVar(&upConsoleBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots; enables the console's point-in-time Reconstruct surface")
	watchCmd.Flags().StringVar(&upConsoleBaselineS3, "baseline-s3", "", "S3 prefix of baseline Parquet snapshots (s3://bucket/prefix/); enables Reconstruct")
	watchCmd.Flags().StringVar(&upConsoleServersFile, "console-servers-file", "", "Path to the console server registry YAML (default ~/.config/bintrail/console-servers.yaml)")
	watchCmd.Flags().StringVar(&upRotateRetain, "rotate-retain", "30d", "Built-in rotation: drop index partitions older than this (Nd/Nh; \"off\" disables)")
	watchCmd.Flags().StringVar(&upRotateInterval, "rotate-interval", "1h", "Built-in rotation: how often to run a rotation cycle")
	watchCmd.Flags().IntVar(&upRotateAddFuture, "rotate-add-future", 3, "Built-in rotation: keep at least N future hourly partitions ready")
	// --source-dsn is deliberately NOT required: the daemon may start
	// source-less (zero-config install) and sources are added from the UI.
	_ = watchCmd.MarkFlagRequired("index-dsn")
	bindWatchEnv(watchCmd)
	rootCmd.AddCommand(watchCmd)
}

func runWatch(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(upFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", upFormat)
	}
	// Validate the built-in rotation settings up front so a typo fails fast,
	// before any phase runs. The loop itself starts with phase 3. The Changed
	// check covers flag and env alike (bindWatchEnv marks env-set flags
	// Changed); an explicitly-chosen retention disables the upgrade guard.
	var err error
	upRotationCfg, err = rotation.ParseSettings(upRotateRetain, upRotateInterval, upRotateAddFuture,
		cmd.Flags().Changed("rotate-retain"))
	if err != nil {
		return err
	}

	// Containerized installs start the daemon and the index MySQL together,
	// and the official mysql image briefly accepts-then-drops connections
	// during its first initialization — a single connect attempt turns first
	// boot into a restart loop (regenerating the console token each time).
	// Wait for the index instead of dying.
	if err := waitForIndexMySQL(cmd.Context(), upIndexDSN, 90*time.Second); err != nil {
		return fmt.Errorf("index MySQL did not become reachable: %w", err)
	}

	// ── Phase 1: Preflight ──────────────────────────────────────────────────
	if upSourceDSN == "" {
		fmt.Fprintln(os.Stderr, "=== Phase 1/3: Preflight checks ===")
		fmt.Fprintln(os.Stderr, "No source configured yet — the preflight runs when you add a server from the console.")
		fmt.Fprintln(os.Stderr)
	} else if !upSkipDoctor {
		fmt.Fprintln(os.Stderr, "=== Phase 1/3: Preflight checks ===")
		// The capacity projection uses the daemon's actual rotation window (0
		// when built-in rotation is disabled → it reports unbounded growth).
		// Its FAIL is ADVISORY here: blocking the stream over a disk forecast
		// would manufacture the very forensic gap it warns about (an
		// unattended reboot would crash-loop instead of capturing while
		// there is still room). Standalone `bintrail doctor` keeps full FAIL
		// semantics for CI.
		preflight := doctor.Build(cmd.Context(), upSourceDSN, upIndexDSN, upSchemas, upRotationCfg.Retain)
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
	if err := watchInit(cmd.Context()); err != nil {
		return fmt.Errorf("init failed: %w", err)
	}
	fmt.Fprintln(os.Stderr)

	// ── Phase 3: Stream + console (or console-only daemon when no source) ───
	if upSourceDSN == "" {
		fmt.Fprintln(os.Stderr, "=== Phase 3/3: Console + control plane ===")
		return runUpConsoleOnly(cmd)
	}
	fmt.Fprintln(os.Stderr, "=== Phase 3/3: Streaming ===")
	return runUpStreamWithConsole(cmd, args)
}

// upPreflightOutcome maps the preflight report to the daemon's boot decision:
// fatal is non-nil for any non-advisory failure (boot refused); warnCapacity
// is true when the capacity projection was the ONLY failure — boot proceeds,
// but the operator must hear about it (the caller prints the WARNING).
// Duplicated from cmd/bintrail/up.go (6 lines, the PR-C replication
// precedent): the advisory semantics are up-policy shared by both daemons.
func upPreflightOutcome(r *doctor.Report) (fatal error, warnCapacity bool) {
	if err := r.ErrExcluding(doctor.CapacityCheckName); err != nil {
		return err, false
	}
	return nil, r.Err() != nil
}

// watchInit provisions the boot index database directly via the indexer's
// provisioning API (the same triple the control-plane supervisor runs for
// per-source databases): CREATE DATABASE, the index table set, and the
// schema migration for a pre-existing legacy index. Idempotent — re-running
// `watch` skips work that's already done.
func watchInit(ctx context.Context) error {
	cfg, err := mysql.ParseDSN(upIndexDSN)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}
	if cfg.DBName == "" {
		return fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
	}
	if err := indexer.EnsureDatabase(cfg, cfg.DBName, func(s string) { fmt.Fprintln(os.Stderr, s) }); err != nil {
		return err
	}
	db, err := config.Connect(upIndexDSN)
	if err != nil {
		return fmt.Errorf("connect index database: %w", err)
	}
	defer db.Close()
	if err := indexer.CreateIndexTables(ctx, db, upPartitions, false, func(name string) {
		fmt.Fprintf(os.Stderr, "  ✓ %s\n", name)
	}); err != nil {
		return err
	}
	return indexer.EnsureSchema(db)
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

	supervisor := newMonitorSupervisor(ctx, upIndexDSN, registry, upRotationCfg.Retain)
	cfg.MonitorCtrl = supervisor

	// Built-in rotation covers the boot index plus every per-source database
	// the control plane provisions — the unattended quickstart's real data
	// lives in the latter.
	rotation.StartLoop(ctx, upRotationCfg, func() []string {
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

// runUpStreamWithConsole serves the read-only web console in this same process,
// alongside the live stream. Both share one SIGINT/SIGTERM lifecycle: the signal
// cancels the context the console runs on, and the main stream runs on the same
// context, so a single Ctrl-C drains both. The console gets its own index DB
// connection (the stream owns its own).
func runUpStreamWithConsole(cmd *cobra.Command, args []string) error {
	serverID := upServerID
	if serverID == 0 {
		id, err := serverid.DeriveServerID(upSourceDSN)
		if err != nil {
			return fmt.Errorf("cannot auto-derive --server-id from --source-dsn: %w (pass --server-id explicitly to bypass)", err)
		}
		serverID = id
		fmt.Fprintf(os.Stderr, "Auto-derived server-id from source DSN: %d\n", serverID)
	}

	resolveUpConsoleEnv(cmd)

	db, err := config.Connect(upIndexDSN)
	if err != nil {
		return fmt.Errorf("console: connect index database: %w", err)
	}
	defer db.Close()

	// Bring the index schema up to date before serving — streamrun.One also
	// does this, but it runs after the console goroutine starts, so a legacy
	// index DB missing newer columns (e.g. connection_id) could fail early
	// /api/events requests in the startup window.
	if err := indexer.EnsureSchema(db); err != nil {
		return fmt.Errorf("console: schema migration: %w", err)
	}

	// The server registry gives `watch` the same UI-managed switcher as the
	// standalone console; the stream's own index is the ephemeral default.
	// A corrupt file fails loud — silently starting without the operator's
	// saved servers would look like data loss.
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

	// The control-plane supervisor: "+ Add server" in the console starts real
	// monitoring through it. Streams live on the daemon context (ctx), not on
	// the HTTP requests that start them.
	supervisor := newMonitorSupervisor(ctx, upIndexDSN, registry, upRotationCfg.Retain)
	cfg.MonitorCtrl = supervisor

	// Built-in rotation: boot index + every per-source database the control
	// plane provisions, on the daemon lifecycle.
	rotation.StartLoop(ctx, upRotationCfg, func() []string {
		return append([]string{upIndexDSN}, supervisor.ActiveIndexDSNs()...)
	})

	// With the console comes the multi-stream control plane, so /metrics is
	// served once at the daemon level (per-source "source" labels keep the
	// series apart). The main stream's config keeps MetricsAddr empty so it
	// never double-binds the same address inside streamrun.One. Synchronous
	// bind: fails fast, like the console bind below.
	if upMetricsAddr != "" {
		stopMetrics, err := streamrun.StartMetricsServer(upMetricsAddr)
		if err != nil {
			return err
		}
		defer stopMetrics()
	}

	srv, err := console.New(cfg)
	if err != nil {
		return err
	}

	// Bind synchronously so a port conflict fails `watch` fast — otherwise the
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

	streamErr := streamrun.One(ctx, watchStreamConfig(serverID))
	stop()                // drain the console even if the stream returned without a signal
	<-consoleDone         // order the console goroutine's exit before the deferred db.Close()
	supervisor.Shutdown() // final checkpoints for every monitored stream
	return streamErr
}

// watchStreamConfig snapshots watch's flag values into the main stream's
// streamrun.Config. The pinned values (StartPos 4, SSLMode preferred,
// GapTimeout 30, …) replicate what core `up` produced via its
// populateStreamFlags → streamConfigFromFlags fan-out: `watch`, like `up`,
// deliberately exposes only the quickstart subset of stream's flags.
// MetricsAddr stays empty on purpose — the daemon serves ONE /metrics
// endpoint for all streams (see runUpStreamWithConsole).
func watchStreamConfig(serverID uint32) streamrun.Config {
	return streamrun.Config{
		IndexDSN:   upIndexDSN,
		SourceDSN:  upSourceDSN,
		ServerID:   serverID,
		StartFile:  "",
		StartPos:   4,
		StartGTID:  "",
		BatchSize:  upBatchSize,
		Schemas:    upSchemas,
		Tables:     upTables,
		Checkpoint: upCheckpoint,
		SSLMode:    "preferred",
		Format:     upFormat,
		GapTimeout: 30,
		Deps:       streamdeps.Default(),
	}
}

// resolveUpConsoleEnv applies the console-specific env vars to the upConsole*
// globals with flag > env > default precedence (mirrors runServe). These are
// read directly rather than bound in watchEnvBindings: BINTRAIL_CONSOLE_* are
// console-only vars whose flags (--baseline-dir/--baseline-s3) also exist on
// core bintrail commands, and the direct read keeps the precedence dance in
// one unit-testable place.
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

// upConsoleConfig builds the console configuration for `watch`. It serves
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
		// runUpStreamWithConsole / runUpConsoleOnly — because it needs the
		// registry and the daemon lifecycle context, which this config builder
		// doesn't have.
	}, nil
}
