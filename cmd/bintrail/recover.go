package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/cliutil"
	"github.com/bintrail/bintrail/internal/config"
	"github.com/bintrail/bintrail/internal/metadata"
	"github.com/bintrail/bintrail/internal/parquetquery"
	"github.com/bintrail/bintrail/internal/query"
	"github.com/bintrail/bintrail/internal/recovery"
)

var recoverCmd = &cobra.Command{
	Use:   "recover",
	Short: "Generate reversal SQL from indexed binlog events",
	Long: `Query the binlog event index and generate a transaction-wrapped SQL script
that reverses each matching event. Review the output carefully before applying.

Reversal logic:
  DELETE → INSERT INTO ... (row_before values)
  UPDATE → UPDATE ... SET (row_before) WHERE (row_after, current state)
  INSERT → DELETE FROM ... WHERE (row_after, current state)

Examples:
  # Recover deleted rows in a time window
  bintrail recover --index-dsn "..." \
    --schema mydb --table orders --event-type DELETE \
    --since "2026-02-19 14:00:00" --until "2026-02-19 14:05:00" \
    --output recovery.sql

  # Reverse updates to a specific row (preview first)
  bintrail recover --index-dsn "..." \
    --schema mydb --table orders --pk '12345' --event-type UPDATE \
    --dry-run

  # Reverse an entire transaction
  bintrail recover --index-dsn "..." \
    --gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:42" \
    --output recovery.sql`,
	RunE: runRecover,
}

var (
	rIndexDSN   string
	rSchema     string
	rTable      string
	rPK         string
	rEventType  string
	rGTID       string
	rSince      string
	rUntil      string
	rFlag       string
	rOutput     string
	rDryRun     bool
	rLimit      int
	rProfile    string
	rFormat     string
	rNoArchive  bool
)

func init() {
	recoverCmd.Flags().StringVar(&rIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	recoverCmd.Flags().StringVar(&rSchema, "schema", "", "Filter by schema name")
	recoverCmd.Flags().StringVar(&rTable, "table", "", "Filter by table name")
	recoverCmd.Flags().StringVar(&rPK, "pk", "", "Filter by primary key value(s), pipe-delimited for composite PKs")
	recoverCmd.Flags().StringVar(&rEventType, "event-type", "", "Filter by event type: INSERT, UPDATE, or DELETE")
	recoverCmd.Flags().StringVar(&rGTID, "gtid", "", "Filter by GTID (e.g. uuid:42)")
	recoverCmd.Flags().StringVar(&rSince, "since", "", "Filter events at or after this time (2006-01-02 15:04:05)")
	recoverCmd.Flags().StringVar(&rUntil, "until", "", "Filter events at or before this time (2006-01-02 15:04:05)")
	recoverCmd.Flags().StringVar(&rFlag, "flag", "", "Filter events from tables or columns carrying this flag (see 'bintrail flag list')")
	recoverCmd.Flags().StringVar(&rOutput, "output", "", "Write recovery SQL to this file (required unless --dry-run)")
	recoverCmd.Flags().BoolVar(&rDryRun, "dry-run", false, "Print recovery SQL to stdout instead of writing a file")
	recoverCmd.Flags().IntVar(&rLimit, "limit", 1000, "Maximum number of events to reverse")
	recoverCmd.Flags().StringVar(&rProfile, "profile", "", "Apply RBAC access rules for this profile (table-level deny and column-level redaction)")
	recoverCmd.Flags().StringVar(&rFormat, "format", "text", "Output format: text or json")
	recoverCmd.Flags().BoolVar(&rNoArchive, "no-archive", false, "Disable auto-routing to Parquet archives (MySQL-only results)")
	_ = recoverCmd.MarkFlagRequired("index-dsn")
	bindCommandEnv(recoverCmd)

	rootCmd.AddCommand(recoverCmd)
}

func runRecover(cmd *cobra.Command, args []string) error {
	start := time.Now()
	// ── Validate flags ────────────────────────────────────────────────────────
	if !cliutil.IsValidOutputFormat(rFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", rFormat)
	}
	if !rDryRun && rOutput == "" {
		return fmt.Errorf("one of --output or --dry-run is required")
	}
	if rPK != "" && (rSchema == "" || rTable == "") {
		return fmt.Errorf("--pk requires both --schema and --table")
	}

	// ── Parse filter values ───────────────────────────────────────────────────
	eventType, err := cliutil.ParseEventType(rEventType)
	if err != nil {
		return err
	}
	since, err := cliutil.ParseTime(rSince)
	if err != nil {
		return fmt.Errorf("--since: %w", err)
	}
	until, err := cliutil.ParseTime(rUntil)
	if err != nil {
		return fmt.Errorf("--until: %w", err)
	}

	opts := query.Options{
		Schema:    rSchema,
		Table:     rTable,
		PKValues:  rPK,
		EventType: eventType,
		GTID:      rGTID,
		Since:     since,
		Until:     until,
		Flag:      rFlag,
		Limit:     rLimit,
	}

	// ── Connect to index database ─────────────────────────────────────────────
	db, err := config.Connect(rIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	if rProfile != "" {
		denyTables, redactCols, err := query.LoadProfileRules(cmd.Context(), db, rProfile)
		if err != nil {
			return fmt.Errorf("load profile rules for %q: %w", rProfile, err)
		}
		opts.DenyTables = denyTables
		opts.RedactColumns = redactCols
	}

	// ── Load schema resolver (best-effort; non-fatal) ─────────────────────────
	// The resolver enables PK-only WHERE clauses in recovery SQL.
	// If unavailable, the generator falls back to all-columns WHERE — verbose
	// but always correct for tables without duplicate rows.
	resolver, err := metadata.NewResolver(db, 0) // 0 = latest snapshot
	if err != nil {
		slog.Warn("could not load schema snapshot; WHERE clauses will use all columns", "error", err)
		resolver = nil
	}

	// ── Fetch events (live + archives) ────────────────────────────────────────
	// Skip archive auto-discovery when --no-archive is set or --profile is
	// active (archive queries do not enforce DenyTables/RedactColumns rules).
	engine := query.New(db)
	var archSources []string
	if !rNoArchive && rProfile == "" {
		archSources = query.ResolveArchiveSources(cmd.Context(), db)
	}

	// ── Coverage warnings and per-partition routing ───────────────────────────
	var plan *query.QueryPlan
	if !rNoArchive && (len(archSources) > 0 || since != nil || until != nil) {
		cfg, parseErr := mysqldriver.ParseDSN(rIndexDSN)
		if parseErr != nil {
			slog.Warn("could not parse DSN for query planning", "error", parseErr)
		} else if cfg.DBName != "" {
			plan = query.RunPlanAndWarn(cmd.Context(), db, cfg.DBName, since, until)
		}
	}

	var rows []query.ResultRow
	if len(archSources) > 0 {
		fetchOpts := opts

		if plan != nil && plan.SkipMySQL() {
			slog.Debug("planner: skipping MySQL query (range fully archived)")
		} else {
			rows, err = engine.Fetch(cmd.Context(), fetchOpts)
			if err != nil {
				return err
			}
		}
		for _, src := range archSources {
			ar, err := parquetquery.Fetch(cmd.Context(), fetchOpts, src)
			if err != nil {
				slog.Warn("archive query failed, skipping", "source", src, "error", err)
				continue
			}
			rows = append(rows, ar...)
		}
		rows = query.MergeResults(rows, opts.Limit)
	} else {
		rows, err = engine.Fetch(cmd.Context(), opts)
		if err != nil {
			return err
		}
	}

	// ── Generate recovery SQL ─────────────────────────────────────────────────
	gen := recovery.New(db, resolver)

	if rDryRun {
		if rFormat == "json" {
			// Capture SQL into a buffer for JSON output.
			var buf bytes.Buffer
			n, err := gen.GenerateSQLFromRows(rows, &buf)
			if err != nil {
				return err
			}
			slog.Info("recovery SQL generated",
				"statements", n, "dry_run", true,
				"duration_ms", time.Since(start).Milliseconds())
			return outputJSON(struct {
				Statements int    `json:"statements"`
				DryRun     bool   `json:"dry_run"`
				SQL        string `json:"sql"`
			}{Statements: n, DryRun: true, SQL: buf.String()})
		}

		n, err := gen.GenerateSQLFromRows(rows, os.Stdout)
		if err != nil {
			return err
		}
		slog.Info("recovery SQL generated",
			"statements", n, "dry_run", true,
			"duration_ms", time.Since(start).Milliseconds())
		if n > 0 {
			fmt.Fprintf(os.Stderr, "\n%d reversal statement(s) generated.\n", n)
		}
		return nil
	}

	// Write to output file with a buffered writer for efficiency.
	f, err := os.Create(rOutput)
	if err != nil {
		return fmt.Errorf("failed to create output file %q: %w", rOutput, err)
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	n, err := gen.GenerateSQLFromRows(rows, bw)
	if err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush output file: %w", err)
	}

	slog.Info("recovery SQL generated",
		"statements", n, "dry_run", false, "output", rOutput,
		"duration_ms", time.Since(start).Milliseconds())

	if rFormat == "json" {
		return outputJSON(struct {
			Statements int    `json:"statements"`
			DryRun     bool   `json:"dry_run"`
			Output     string `json:"output"`
		}{Statements: n, DryRun: false, Output: rOutput})
	}

	if n == 0 {
		fmt.Fprintln(os.Stderr, "No events matched the specified criteria.")
	} else {
		fmt.Fprintf(os.Stderr, "%d reversal statement(s) written to %s\n", n, rOutput)
	}
	return nil
}
