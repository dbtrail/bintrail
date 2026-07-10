package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/forensics"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
)

// errForensicsDisabled is the uniform surface-level answer when the
// entitlement gate is closed. Policy lives here at the command entry points —
// never inside the forensics library (epic #701, decision D1).
var errForensicsDisabled = fmt.Errorf("forensics is not enabled in this build")

// ─── who-changed ──────────────────────────────────────────────────────────────

var whoChangedCmd = &cobra.Command{
	Use:   "who-changed",
	Short: "Attribute binlog row changes to the database sessions that made them",
	Long: `Correlate indexed binlog events for a table (optionally a single row) with
session-identity sources on the source server and answer "who changed this?".

Each event is attributed through a tier cascade and labeled with its source
and a confidence grade (exact / corroborated / heuristic):

  1. audit log        — durable identity, works after disconnect; each
                        connection id is bounded by its CONNECT..DISCONNECT
                        lifetime so id reuse (pool churn) cannot misattribute
  2. performance_schema — live sessions on the source
  3. connection_cache — identities cached by 'bintrail up' for sessions that
                        already disconnected (--attribution-retention)
  4. binlog only      — no identity available; the event is still returned
                        with an explanatory note, never an error

When the source captures the original statement per row event
(binlog_rows_query_log_events=ON), it is shown alongside the attribution.

Without --since/--until the search is bounded to the last 24 hours and the
output says so. --source-dsn is optional: without it only the index-side
tiers run (connection_cache, binlog-only).

Examples:
  # Who touched order 42 in the last 24 hours?
  bintrail who-changed --index-dsn "$IDX" --source-dsn "$SRC" \
    --schema shop --table orders --pk 42

  # Everything on the table in an incident window, machine-readable
  bintrail who-changed --index-dsn "$IDX" --source-dsn "$SRC" \
    --schema shop --table orders \
    --since "2026-06-15 10:00:00" --until "2026-06-15 11:00:00" --format json`,
	RunE: runWhoChanged,
}

var (
	wcIndexDSN  string
	wcSourceDSN string
	wcSchema    string
	wcTable     string
	wcPK        string
	wcSince     string
	wcUntil     string
	wcLimit     int
	wcOrder     string
	wcFormat    string
)

func init() {
	f := whoChangedCmd.Flags()
	f.StringVar(&wcIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	f.StringVar(&wcSourceDSN, "source-dsn", "", "DSN for the source MySQL server (optional; enables the audit-log and performance_schema tiers)")
	f.StringVar(&wcSchema, "schema", "", "Schema of the changed table (required)")
	f.StringVar(&wcTable, "table", "", "Changed table (required)")
	f.StringVar(&wcPK, "pk", "", "Restrict to a single row's primary key (pipe-delimited for composite PKs)")
	f.StringVar(&wcSince, "since", "", "Only changes at or after this time (2006-01-02 15:04:05); default: last 24 hours")
	f.StringVar(&wcUntil, "until", "", "Only changes at or before this time (2006-01-02 15:04:05)")
	f.IntVar(&wcLimit, "limit", 100, "Maximum number of events to attribute")
	f.StringVar(&wcOrder, "order", "ASC", "Sort direction applied before --limit: ASC (oldest first) or DESC (newest first)")
	f.StringVar(&wcFormat, "format", "table", "Output format: table or json")
	AddDuckDBTuningFlags(whoChangedCmd)
	_ = whoChangedCmd.MarkFlagRequired("index-dsn")
	_ = whoChangedCmd.MarkFlagRequired("schema")
	_ = whoChangedCmd.MarkFlagRequired("table")
	BindCommandEnv(whoChangedCmd)
}

func runWhoChanged(cmd *cobra.Command, args []string) error {
	if !forensics.Enabled() {
		return errForensicsDisabled
	}
	if wcFormat != "table" && wcFormat != "json" {
		return fmt.Errorf("invalid --format %q; must be table or json", wcFormat)
	}
	if !strings.EqualFold(wcOrder, "ASC") && !strings.EqualFold(wcOrder, "DESC") {
		return fmt.Errorf("invalid --order %q; must be ASC or DESC", wcOrder)
	}
	if wcLimit < 1 {
		return fmt.Errorf("--limit must be >= 1")
	}
	since, err := cliutil.ParseTime(wcSince)
	if err != nil {
		return fmt.Errorf("--since: %w", err)
	}
	until, err := cliutil.ParseTime(wcUntil)
	if err != nil {
		return fmt.Errorf("--until: %w", err)
	}
	duckTuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}

	indexDB, err := config.Connect(wcIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer indexDB.Close()
	if err := indexer.EnsureSchema(indexDB); err != nil {
		return indexer.WrapSchemaMigrationErr(err)
	}

	deps := forensics.WhoChangedDeps{IndexDB: indexDB}
	if wcSourceDSN != "" {
		sourceDB, err := config.Connect(wcSourceDSN)
		if err != nil {
			return fmt.Errorf("failed to connect to source database: %w", err)
		}
		defer sourceDB.Close()
		// Fail fast on a bad --source-dsn: the engine degrades an unreachable
		// source to binlog-only mid-flow (resilience), but an operator who
		// explicitly asked for source tiers should not get a silent downgrade
		// because of a typo.
		if err := sourceDB.PingContext(cmd.Context()); err != nil {
			return fmt.Errorf("source database unreachable: %w", err)
		}
		deps.SourceDB = sourceDB
		// Carry the source host (bare, port stripped) so the audit tier can
		// reach the RDS file API when the log lives on AWS-managed storage
		// (RDS/Aurora) rather than a local file. ParseSourceDSN returns the host
		// without the port, matching the agent's derivation; a unix-socket DSN
		// errors here and leaves SourceHost empty (correctly local-file-only).
		if host, _, _, _, perr := config.ParseSourceDSN(wcSourceDSN); perr == nil {
			deps.SourceHost = host
		}
	}

	// Fetch through the shared live-index + archive pipeline (auto-discovery
	// via archive_state). Permissive on gaps — who-changed is investigative —
	// but a detected coverage gap must reach the RESULT, not just the logs:
	// a thin answer over unindexed hours would otherwise read as "nobody
	// changed it", the exact misreading the honest-notes design exists to
	// prevent.
	var dbName string
	if cfg, perr := mysqldriver.ParseDSN(wcIndexDSN); perr == nil {
		dbName = cfg.DBName
	}
	engine := query.New(indexDB)
	var gapHours []time.Time
	deps.Fetch = func(ctx context.Context, opts query.Options) ([]query.ResultRow, error) {
		rows, plan, ferr := query.FetchMerged(ctx, indexDB, engine, query.FetchMergedOptions{
			Opts:           opts,
			DBName:         dbName,
			AllowGaps:      true,
			ArchiveFetcher: TunedArchiveFetcher(duckTuning),
		})
		if plan != nil {
			gapHours = plan.GapHours
		}
		return rows, ferr
	}

	res, err := forensics.WhoChanged(cmd.Context(), deps, forensics.WhoChangedParams{
		Schema: wcSchema,
		Table:  wcTable,
		PK:     wcPK,
		Since:  since,
		Until:  until,
		Limit:  wcLimit,
		Order:  wcOrder,
	})
	if err != nil {
		return err
	}
	if len(gapHours) > 0 {
		res.Notes = append(res.Notes, "Index coverage warning: "+query.FormatGapWarning(gapHours))
	}

	if wcFormat == "json" {
		return cliutil.OutputJSON(res)
	}
	renderWhoChangedTable(os.Stdout, res)
	if n := len(res.Events); n > 0 {
		fmt.Fprintf(os.Stderr, "\n%d event(s)\n", n)
		if n >= wcLimit {
			fmt.Fprintf(os.Stderr, "Warning: results truncated at %d events. Use a narrower time range or --limit to adjust.\n", wcLimit)
		}
	}
	return nil
}

// renderWhoChangedTable writes the human-readable view: one row per event
// with its attribution, then the caveat notes and any fallback SQL.
func renderWhoChangedTable(w io.Writer, res forensics.WhoChangedResult) {
	if len(res.Events) > 0 {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "TIMESTAMP\tTYPE\tPK\tCONN\tWHO\tSOURCE\tCONFIDENCE\tQUERY")
		for _, ev := range res.Events {
			conn := "-"
			if ev.ConnectionID != nil {
				conn = fmt.Sprintf("%d", *ev.ConnectionID)
			}
			who, source, confidence := "unknown", "-", "-"
			if a := ev.Attribution; a != nil {
				who = a.User
				if a.Host != "" {
					who = a.User + "@" + a.Host
				}
				if a.ClientProgram != "" {
					who += " (" + a.ClientProgram + ")"
				}
				source, confidence = a.Source, string(a.Confidence)
			}
			queryText := "-"
			if ev.QueryText != nil && *ev.QueryText != "" {
				queryText = truncateCell(*ev.QueryText, 60)
			} else if a := ev.Attribution; a != nil && a.AuditSQL != "" {
				queryText = truncateCell(a.AuditSQL, 60)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				ev.Timestamp.Format("2006-01-02 15:04:05"), ev.EventType, ev.PKValues,
				conn, who, source, confidence, queryText)
		}
		tw.Flush()
	}
	printForensicNotes(w, res.Notes)
	printFallbackQueries(w, res.FallbackQueries)
}

// truncateCell shortens a value for a table cell, collapsing newlines so one
// event always occupies one line.
func truncateCell(s string, max int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}

func printForensicNotes(w io.Writer, notes []string) {
	if len(notes) == 0 {
		return
	}
	fmt.Fprintln(w, "\nNotes:")
	for _, n := range notes {
		fmt.Fprintf(w, "  - %s\n", n)
	}
}

// printFallbackQueries renders the aggregated fallback-SQL block emitted when
// attribution sources are unavailable — SQL the operator can run manually
// against the source server.
func printFallbackQueries(w io.Writer, fqs []forensics.FallbackQuery) {
	if len(fqs) == 0 {
		return
	}
	fmt.Fprintln(w, "\nFallback queries (run manually against the source server):")
	for _, fq := range fqs {
		fmt.Fprintf(w, "  -- %s\n  %s\n", fq.Description, fq.SQL)
	}
}

// ─── user-activity / connection-history ──────────────────────────────────────

var userActivityCmd = &cobra.Command{
	Use:   "user-activity",
	Short: "Show a MySQL user's recent statements from performance_schema",
	Long: `Query the source server's performance_schema statement history for one user.
When the needed consumers are disabled or the history is empty, the command
returns diagnostics plus fallback SQL to run manually — a degraded source is
an answer, not an error.

Example:
  bintrail user-activity --source-dsn "$SRC" --user app_rw --limit 20`,
	RunE: runUserActivity,
}

var connectionHistoryCmd = &cobra.Command{
	Use:   "connection-history",
	Short: "Show current connections and account history for a user or host",
	Long: `List the source server's live connections filtered by user and/or host, from
performance_schema.threads. Falls back to executable SQL (processlist,
account summaries) when performance_schema is not accessible.

Example:
  bintrail connection-history --source-dsn "$SRC" --user app_rw`,
	RunE: runConnectionHistory,
}

var (
	uaSourceDSN, uaUser, uaSince, uaUntil, uaOrder, uaFormat string
	uaLimit                                                  int

	chSourceDSN, chUser, chHost, chOrder, chFormat string
	chLimit                                        int
)

func init() {
	f := userActivityCmd.Flags()
	f.StringVar(&uaSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	f.StringVar(&uaUser, "user", "", "MySQL user whose activity to show (required)")
	f.StringVar(&uaSince, "since", "", "Time lower bound; shapes the generated fallback SQL only (performance_schema history has no wall-clock column)")
	f.StringVar(&uaUntil, "until", "", "Time upper bound; shapes the generated fallback SQL only")
	f.IntVar(&uaLimit, "limit", 50, "Maximum number of statements to return (capped at 1000)")
	f.StringVar(&uaOrder, "order", "DESC", "Sort direction: ASC (oldest first) or DESC (newest first)")
	f.StringVar(&uaFormat, "format", "table", "Output format: table or json")
	_ = userActivityCmd.MarkFlagRequired("source-dsn")
	_ = userActivityCmd.MarkFlagRequired("user")
	BindCommandEnv(userActivityCmd)

	f = connectionHistoryCmd.Flags()
	f.StringVar(&chSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	f.StringVar(&chUser, "user", "", "Filter by MySQL user (one of --user/--host is required)")
	f.StringVar(&chHost, "host", "", "Filter by client host substring (one of --user/--host is required)")
	f.IntVar(&chLimit, "limit", 50, "Maximum number of connections to return (capped at 1000)")
	f.StringVar(&chOrder, "order", "DESC", "Sort direction by connection age: ASC or DESC")
	f.StringVar(&chFormat, "format", "table", "Output format: table or json")
	_ = connectionHistoryCmd.MarkFlagRequired("source-dsn")
	BindCommandEnv(connectionHistoryCmd)
}

func runUserActivity(cmd *cobra.Command, args []string) error {
	return runActivity(cmd, uaFormat, uaSourceDSN, forensics.ActivityQuery{
		Type:  forensics.QueryUserActivity,
		User:  uaUser,
		Since: uaSince,
		Until: uaUntil,
		Limit: uaLimit,
		Order: uaOrder,
	})
}

func runConnectionHistory(cmd *cobra.Command, args []string) error {
	if !forensics.Enabled() {
		return errForensicsDisabled
	}
	if chUser == "" && chHost == "" {
		return fmt.Errorf("one of --user or --host is required")
	}
	return runActivity(cmd, chFormat, chSourceDSN, forensics.ActivityQuery{
		Type:  forensics.QueryConnectionHistory,
		User:  chUser,
		Host:  chHost,
		Limit: chLimit,
		Order: chOrder,
	})
}

// runActivity is the shared body of the two thin activity wrappers over
// forensics.Activity (#716): gate, validate, connect, query, render.
func runActivity(cmd *cobra.Command, format, sourceDSN string, q forensics.ActivityQuery) error {
	if !forensics.Enabled() {
		return errForensicsDisabled
	}
	if format != "table" && format != "json" {
		return fmt.Errorf("invalid --format %q; must be table or json", format)
	}
	if q.Order != "" && !strings.EqualFold(q.Order, "ASC") && !strings.EqualFold(q.Order, "DESC") {
		return fmt.Errorf("invalid --order %q; must be ASC or DESC", q.Order)
	}

	sourceDB, err := config.Connect(sourceDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to source database: %w", err)
	}
	defer sourceDB.Close()
	if err := sourceDB.PingContext(cmd.Context()); err != nil {
		return fmt.Errorf("source database unreachable: %w", err)
	}

	res, err := forensics.Activity(cmd.Context(), sourceDB, q)
	if err != nil {
		return err
	}
	if format == "json" {
		return cliutil.OutputJSON(res)
	}
	renderActivityTable(os.Stdout, res)
	return nil
}

// renderActivityTable writes the human-readable view of an ActivityResult:
// a statement or connection table, the source, any diagnostic note, and the
// fallback-SQL block when the live source was unavailable.
func renderActivityTable(w io.Writer, res forensics.ActivityResult) {
	switch {
	case len(res.Events) > 0:
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "CONN\tUSER\tHOST\tROWS\tDURATION_MS\tSQL")
		for _, ev := range res.Events {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
				cellValue(ev, "connection_id"), cellValue(ev, "user"), cellValue(ev, "host"),
				cellValue(ev, "rows_affected"), cellValue(ev, "duration_ms"),
				dashIfEmpty(truncateCell(stringValue(ev, "sql_text"), 80)))
		}
		tw.Flush()
	case len(res.Connections) > 0:
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "CONN\tUSER\tHOST\tDB\tCOMMAND\tSTATE\tAGE_S\tQUERY")
		for _, c := range res.Connections {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				cellValue(c, "connection_id"), cellValue(c, "user"), cellValue(c, "host"),
				cellValue(c, "current_db"), cellValue(c, "command"), cellValue(c, "state"),
				cellValue(c, "time_seconds"), dashIfEmpty(truncateCell(stringValue(c, "current_query"), 60)))
		}
		tw.Flush()
	default:
		fmt.Fprintln(w, "No results.")
	}
	fmt.Fprintf(w, "\nSource: %s\n", res.Source)
	if res.Note != "" {
		fmt.Fprintf(w, "Note: %s\n", res.Note)
	}
	printFallbackQueries(w, res.FallbackQueries)
}

// cellValue renders one generic map value for a table cell ("-" when absent).
func cellValue(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return "-"
	}
	if f, isFloat := v.(float64); isFloat {
		return fmt.Sprintf("%.1f", f)
	}
	return fmt.Sprintf("%v", v)
}

// stringValue returns a map value as a string ("" when absent), for cells
// that go through truncateCell.
func stringValue(m map[string]any, key string) string {
	if v, ok := m[key]; ok && v != nil {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

// dashIfEmpty keeps empty table cells visibly aligned.
func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
