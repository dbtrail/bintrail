package main

import (
	"cmp"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/cliutil"
	"github.com/bintrail/bintrail/internal/config"
	"github.com/bintrail/bintrail/internal/parquetquery"
	"github.com/bintrail/bintrail/internal/query"
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Search the binlog event index",
	Long: `Query the binlog_events index with flexible filters. Results are printed to
stdout in the chosen format (table, json, or csv).

Examples:
  # All events for a PK
  bintrail query --index-dsn "..." --schema mydb --table orders --pk 12345

  # Composite PK (pipe-delimited, ordinal order)
  bintrail query --index-dsn "..." --schema mydb --table order_items --pk '12345|2'

  # DELETEs in a time window
  bintrail query --index-dsn "..." --schema mydb --table orders \
    --event-type DELETE --since "2026-02-19 14:00:00" --until "2026-02-19 15:00:00"

  # Everything touched by a GTID
  bintrail query --index-dsn "..." --gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:42"

  # Rows where 'status' changed
  bintrail query --index-dsn "..." --schema mydb --table orders \
    --changed-column status --since "2026-02-19 14:00:00"`,
	RunE: runQuery,
}

var (
	qIndexDSN   string
	qSchema     string
	qTable      string
	qPK         string
	qEventType  string
	qGTID       string
	qSince      string
	qUntil      string
	qChangedCol string
	qFormat     string
	qLimit      int
	qArchiveDir string
	qArchiveS3  string
)

func init() {
	queryCmd.Flags().StringVar(&qIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	queryCmd.Flags().StringVar(&qSchema, "schema", "", "Filter by schema name")
	queryCmd.Flags().StringVar(&qTable, "table", "", "Filter by table name")
	queryCmd.Flags().StringVar(&qPK, "pk", "", "Filter by primary key value(s), pipe-delimited for composite PKs")
	queryCmd.Flags().StringVar(&qEventType, "event-type", "", "Filter by event type: INSERT, UPDATE, or DELETE")
	queryCmd.Flags().StringVar(&qGTID, "gtid", "", "Filter by GTID (e.g. uuid:42)")
	queryCmd.Flags().StringVar(&qSince, "since", "", "Filter events at or after this time (2006-01-02 15:04:05)")
	queryCmd.Flags().StringVar(&qUntil, "until", "", "Filter events at or before this time (2006-01-02 15:04:05)")
	queryCmd.Flags().StringVar(&qChangedCol, "changed-column", "", "Filter UPDATEs that modified this column")
	queryCmd.Flags().StringVar(&qFormat, "format", "table", "Output format: table, json, or csv")
	queryCmd.Flags().IntVar(&qLimit, "limit", 100, "Maximum number of rows to return")
	queryCmd.Flags().StringVar(&qArchiveDir, "archive-dir", "", "Local directory of Parquet archive files to merge with live index results")
	queryCmd.Flags().StringVar(&qArchiveS3, "archive-s3", "", "S3 URL prefix of Parquet archive files to include (e.g. s3://bucket/prefix/)")
	_ = queryCmd.MarkFlagRequired("index-dsn")

	rootCmd.AddCommand(queryCmd)
}

func runQuery(cmd *cobra.Command, args []string) error {
	start := time.Now()
	// ── Validate flag combinations ────────────────────────────────────────────
	if qPK != "" && (qSchema == "" || qTable == "") {
		return fmt.Errorf("--pk requires both --schema and --table")
	}
	if qChangedCol != "" && (qSchema == "" || qTable == "") {
		return fmt.Errorf("--changed-column requires both --schema and --table")
	}
	if !cliutil.IsValidFormat(qFormat) {
		return fmt.Errorf("invalid --format %q; must be table, json, or csv", qFormat)
	}

	// ── Parse filter values ───────────────────────────────────────────────────
	eventType, err := cliutil.ParseEventType(qEventType)
	if err != nil {
		return err
	}
	since, err := cliutil.ParseTime(qSince)
	if err != nil {
		return fmt.Errorf("--since: %w", err)
	}
	until, err := cliutil.ParseTime(qUntil)
	if err != nil {
		return fmt.Errorf("--until: %w", err)
	}

	opts := query.Options{
		Schema:        qSchema,
		Table:         qTable,
		PKValues:      qPK,
		EventType:     eventType,
		GTID:          qGTID,
		Since:         since,
		Until:         until,
		ChangedColumn: qChangedCol,
		Limit:         qLimit,
	}

	// ── Connect and fetch from the index ─────────────────────────────────────
	db, err := config.Connect(qIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	engine := query.New(db)

	// When no archive sources are configured, take the fast path (fetch + format
	// in one step, same as before this feature was added).
	if qArchiveDir == "" && qArchiveS3 == "" {
		n, err := engine.Run(cmd.Context(), opts, qFormat, os.Stdout)
		if err != nil {
			return err
		}
		slog.Info("query complete",
			"results", n,
			"format", qFormat,
			"duration_ms", time.Since(start).Milliseconds())
		if qFormat == "table" && n > 0 {
			fmt.Fprintf(os.Stderr, "\n%d row(s)\n", n)
		}
		return nil
	}

	// ── Fetch from index + archives, then merge ───────────────────────────────
	// Each source fetches without a per-source row limit (Limit: 0) so that
	// chronologically older events in the archives are not discarded before the
	// merge sort. The user's --limit is applied once, after sorting.
	fetchOpts := opts
	fetchOpts.Limit = 0

	results, err := engine.Fetch(cmd.Context(), fetchOpts)
	if err != nil {
		return err
	}

	for _, src := range archiveSources() {
		ar, err := parquetquery.Fetch(cmd.Context(), fetchOpts, src)
		if err != nil {
			slog.Warn("archive query failed, skipping", "source", src, "error", err)
			continue
		}
		results = append(results, ar...)
	}

	results = mergeResults(results, opts.Limit)

	n, err := query.Format(results, qFormat, os.Stdout)
	if err != nil {
		return err
	}

	slog.Info("query complete",
		"results", n,
		"format", qFormat,
		"duration_ms", time.Since(start).Milliseconds())
	if qFormat == "table" && n > 0 {
		fmt.Fprintf(os.Stderr, "\n%d row(s)\n", n)
	}
	return nil
}

// archiveSources returns the non-empty archive source flags as a slice.
func archiveSources() []string {
	var sources []string
	if qArchiveDir != "" {
		sources = append(sources, qArchiveDir)
	}
	if qArchiveS3 != "" {
		sources = append(sources, qArchiveS3)
	}
	return sources
}

// mergeResults deduplicates rows by event_id, sorts by (event_timestamp, event_id),
// and applies the limit. MySQL rows are always processed first, so in the rare case
// of a duplicate event_id the index version is kept.
func mergeResults(rows []query.ResultRow, limit int) []query.ResultRow {
	seen := make(map[uint64]struct{}, len(rows))
	unique := rows[:0]
	for _, r := range rows {
		if _, dup := seen[r.EventID]; !dup {
			seen[r.EventID] = struct{}{}
			unique = append(unique, r)
		}
	}
	slices.SortFunc(unique, func(a, b query.ResultRow) int {
		if c := a.EventTimestamp.Compare(b.EventTimestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.EventID, b.EventID)
	})
	if limit > 0 && len(unique) > limit {
		unique = unique[:limit]
	}
	return unique
}
