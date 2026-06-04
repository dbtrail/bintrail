package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/console"
	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/query"
)

var consoleCmd = &cobra.Command{
	Use:   "console",
	Short: "Serve a read-only web UI over the index (browse events, generate undo SQL)",
	Long: `Starts a local, read-only, single-operator web console over the binlog index.

It is the MCP server with a web face: browse indexed row events with full
before/after diffs, and generate recovery (undo) SQL — all from a browser. The
console NEVER executes SQL; recover produces a script you review and apply
yourself.

Security:
  - Binds to loopback (127.0.0.1) by default and requires an access token.
  - A token is auto-generated for loopback binds and printed in the URL.
  - Binding to a non-loopback address REQUIRES an explicit --token.

Example:
  bintrail console --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"`,
	RunE: runConsole,
}

var (
	conIndexDSN     string
	conListen       string
	conToken        string
	conNoArchive    bool
	conProfile      string
	conAllowedHosts []string
	conBaselineDir  string
	conBaselineS3   string
)

func init() {
	consoleCmd.Flags().StringVar(&conIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	consoleCmd.Flags().StringVar(&conListen, "listen", "127.0.0.1:8090", "Address to listen on (host:port)")
	consoleCmd.Flags().StringVar(&conToken, "token", "", "Access token (auto-generated for loopback binds when empty)")
	consoleCmd.Flags().BoolVar(&conNoArchive, "no-archive", false, "Disable Parquet archive auto-discovery (MySQL-only)")
	consoleCmd.Flags().StringVar(&conProfile, "profile", "", "RBAC profile: deny tables / redact columns; forces --no-archive")
	consoleCmd.Flags().StringSliceVar(&conAllowedHosts, "allowed-hosts", nil, "Extra hostnames allowed in the Host header (for reverse-proxy setups; IP literals and localhost are always allowed)")
	consoleCmd.Flags().StringVar(&conBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots; enables the point-in-time Reconstruct surface")
	consoleCmd.Flags().StringVar(&conBaselineS3, "baseline-s3", "", "S3 prefix of baseline Parquet snapshots (s3://bucket/prefix/); enables Reconstruct")
	_ = consoleCmd.MarkFlagRequired("index-dsn")
	// bindCommandEnv wires the shared BINTRAIL_* env vars (notably
	// BINTRAIL_INDEX_DSN). The console-specific BINTRAIL_CONSOLE_LISTEN /
	// BINTRAIL_CONSOLE_TOKEN are handled in runConsole rather than added to the
	// global envBindings slice: that slice matches by flag name, and --listen is
	// also used by `shim`/`init-shim`, so a global binding would leak the
	// console's listen address into those commands.
	bindCommandEnv(consoleCmd)
	rootCmd.AddCommand(consoleCmd)
}

func runConsole(cmd *cobra.Command, args []string) error {
	// Console-specific env vars, with flag > env > default precedence.
	if !cmd.Flags().Changed("listen") {
		if v := os.Getenv("BINTRAIL_CONSOLE_LISTEN"); v != "" {
			conListen = v
		}
	}
	if !cmd.Flags().Changed("token") {
		if v := os.Getenv("BINTRAIL_CONSOLE_TOKEN"); v != "" {
			conToken = v
		}
	}
	if !cmd.Flags().Changed("baseline-dir") {
		if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_DIR"); v != "" {
			conBaselineDir = v
		}
	}
	if !cmd.Flags().Changed("baseline-s3") {
		if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_S3"); v != "" {
			conBaselineS3 = v
		}
	}

	cfg, err := mysql.ParseDSN(conIndexDSN)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}
	dbName := cfg.DBName
	if dbName == "" {
		return fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
	}

	db, err := config.Connect(conIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	// Run startup migrations once here (not per request), keeping request
	// handlers free of DDL — the recover path is then provably read-only.
	if err := indexer.EnsureSchema(db); err != nil {
		return fmt.Errorf("schema migration: %w", err)
	}

	// Resolve profile RBAC rules up front. Archives don't enforce RBAC, so a
	// profile forces --no-archive to avoid leaking redacted columns.
	var denyTables []query.SchemaTable
	var redactCols []query.SchemaTableColumn
	if conProfile != "" {
		denyTables, redactCols, err = query.LoadProfileRules(cmd.Context(), db, conProfile)
		if err != nil {
			return fmt.Errorf("load profile %q: %w", conProfile, err)
		}
	}

	srv, err := console.New(console.Config{
		DB:            db,
		DBName:        dbName,
		Listen:        conListen,
		Token:         conToken,
		NoArchive:     conNoArchive || conProfile != "",
		DenyTables:    denyTables,
		RedactColumns: redactCols,
		AllowedHosts:  conAllowedHosts,
		BaselineDir:   conBaselineDir,
		BaselineS3:    conBaselineS3,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "\nBintrail console (read-only) is running. Open:\n\n    %s\n\n", srv.URL())
	slog.Info("console listening", "addr", conListen, "no_archive", conNoArchive || conProfile != "")

	if err := srv.Run(ctx); err != nil {
		return fmt.Errorf("console server: %w", err)
	}
	return nil
}
