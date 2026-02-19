package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/config"
	"github.com/bintrail/bintrail/internal/parser"
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
	qIndexDSN     string
	qSchema       string
	qTable        string
	qPK           string
	qEventType    string
	qGTID         string
	qSince        string
	qUntil        string
	qChangedCol   string
	qFormat       string
	qLimit        int
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
	_ = queryCmd.MarkFlagRequired("index-dsn")

	rootCmd.AddCommand(queryCmd)
}

func runQuery(cmd *cobra.Command, args []string) error {
	// ── Validate flag combinations ────────────────────────────────────────────
	if qPK != "" && (qSchema == "" || qTable == "") {
		return fmt.Errorf("--pk requires both --schema and --table")
	}
	if qChangedCol != "" && (qSchema == "" || qTable == "") {
		return fmt.Errorf("--changed-column requires both --schema and --table")
	}
	if !isValidFormat(qFormat) {
		return fmt.Errorf("invalid --format %q; must be table, json, or csv", qFormat)
	}

	// ── Parse filter values ───────────────────────────────────────────────────
	eventType, err := parseEventType(qEventType)
	if err != nil {
		return err
	}
	since, err := parseQueryTime(qSince)
	if err != nil {
		return fmt.Errorf("--since: %w", err)
	}
	until, err := parseQueryTime(qUntil)
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

	// ── Connect and run ───────────────────────────────────────────────────────
	db, err := config.Connect(qIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	engine := query.New(db)
	n, err := engine.Run(cmd.Context(), opts, qFormat, os.Stdout)
	if err != nil {
		return err
	}

	if qFormat == "table" {
		// Row count summary is only useful in table mode; JSON/CSV consumers
		// count rows programmatically.
		if n > 0 {
			fmt.Fprintf(os.Stderr, "\n%d row(s)\n", n)
		}
	}
	return nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// parseEventType converts an event-type string to a *parser.EventType.
// Returns nil for an empty string (meaning "all types").
func parseEventType(s string) (*parser.EventType, error) {
	switch strings.ToUpper(s) {
	case "":
		return nil, nil
	case "INSERT":
		et := parser.EventInsert
		return &et, nil
	case "UPDATE":
		et := parser.EventUpdate
		return &et, nil
	case "DELETE":
		et := parser.EventDelete
		return &et, nil
	default:
		return nil, fmt.Errorf("invalid --event-type %q; must be INSERT, UPDATE, or DELETE", s)
	}
}

// parseQueryTime parses a datetime string in "2006-01-02 15:04:05" format
// using the local timezone. Returns nil for an empty string.
func parseQueryTime(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.Local)
	if err != nil {
		return nil, fmt.Errorf("invalid time %q; expected format: 2006-01-02 15:04:05", s)
	}
	return &t, nil
}

func isValidFormat(s string) bool {
	switch strings.ToLower(s) {
	case "table", "json", "csv":
		return true
	}
	return false
}
