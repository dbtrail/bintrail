package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/cliutil"
	"github.com/bintrail/bintrail/internal/config"
	"github.com/bintrail/bintrail/internal/metadata"
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
	rIndexDSN  string
	rSchema    string
	rTable     string
	rPK        string
	rEventType string
	rGTID      string
	rSince     string
	rUntil     string
	rFlag      string
	rOutput    string
	rDryRun    bool
	rLimit     int
	rProfile   string
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
	_ = recoverCmd.MarkFlagRequired("index-dsn")

	rootCmd.AddCommand(recoverCmd)
}

func runRecover(cmd *cobra.Command, args []string) error {
	start := time.Now()
	// ── Validate flags ────────────────────────────────────────────────────────
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

	// ── Generate recovery SQL ─────────────────────────────────────────────────
	gen := recovery.New(db, resolver)

	if rDryRun {
		n, err := gen.GenerateSQL(cmd.Context(), opts, os.Stdout)
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
	n, err := gen.GenerateSQL(cmd.Context(), opts, bw)
	if err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush output file: %w", err)
	}

	slog.Info("recovery SQL generated",
		"statements", n, "dry_run", false, "output", rOutput,
		"duration_ms", time.Since(start).Milliseconds())
	if n == 0 {
		fmt.Fprintln(os.Stderr, "No events matched the specified criteria.")
	} else {
		fmt.Fprintf(os.Stderr, "%d reversal statement(s) written to %s\n", n, rOutput)
	}
	return nil
}
