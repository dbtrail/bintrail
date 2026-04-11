package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/parquetquery"
	"github.com/dbtrail/bintrail/internal/query"
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
	qFlag       string
	qFormat     string
	qLimit      int
	qArchiveDir string
	qArchiveS3  string
	qBintrailID string
	qProfile    string
	qNoArchive  bool
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
	queryCmd.Flags().StringVar(&qFlag, "flag", "", "Filter events from tables or columns carrying this flag (see 'bintrail flag list')")
	queryCmd.Flags().StringVar(&qFormat, "format", "table", "Output format: table, json, or csv")
	queryCmd.Flags().IntVar(&qLimit, "limit", 100, "Maximum number of rows to return")
	queryCmd.Flags().StringVar(&qArchiveDir, "archive-dir", "", "Local root directory of Parquet archives (requires --bintrail-id)")
	queryCmd.Flags().StringVar(&qArchiveS3, "archive-s3", "", "S3 root URL prefix of Parquet archives (requires --bintrail-id; e.g. s3://bucket/prefix/); uses the standard AWS credential chain")
	queryCmd.Flags().StringVar(&qBintrailID, "bintrail-id", "", "Server identity UUID (required when --archive-dir or --archive-s3 is set)")
	queryCmd.Flags().StringVar(&qProfile, "profile", "", "Apply RBAC access rules for this profile (table-level deny and column-level redaction)")
	queryCmd.Flags().BoolVar(&qNoArchive, "no-archive", false, "Disable auto-routing to Parquet archives (MySQL-only results)")
	_ = queryCmd.MarkFlagRequired("index-dsn")
	bindCommandEnv(queryCmd)

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
	if (qArchiveDir != "" || qArchiveS3 != "") && qBintrailID == "" {
		return fmt.Errorf("--bintrail-id is required when --archive-dir or --archive-s3 is set")
	}
	if qProfile != "" && (qArchiveDir != "" || qArchiveS3 != "") {
		return fmt.Errorf("--profile cannot be combined with --archive-dir or --archive-s3")
	}
	if qNoArchive && (qArchiveDir != "" || qArchiveS3 != "") {
		return fmt.Errorf("--no-archive cannot be combined with --archive-dir or --archive-s3")
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
		Flag:          qFlag,
		Limit:         qLimit,
	}

	// ── Connect and fetch from the index ─────────────────────────────────────
	db, err := config.Connect(qIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	if err := indexer.EnsureSchema(db); err != nil {
		return fmt.Errorf("schema migration: %w", err)
	}

	if qProfile != "" {
		denyTables, redactCols, err := query.LoadProfileRules(cmd.Context(), db, qProfile)
		if err != nil {
			return fmt.Errorf("load profile rules for %q: %w", qProfile, err)
		}
		opts.DenyTables = denyTables
		opts.RedactColumns = redactCols
	}

	engine := query.New(db)

	// Determine archive sources: explicit flags take precedence; otherwise auto-discover.
	// Skip auto-discovery when --no-archive is set, or when --profile is active
	// (archive queries do not enforce DenyTables/RedactColumns rules; explicit
	// archive flags are already blocked by the --profile validation above).
	var archSources []string
	if !qNoArchive {
		archSources = archiveSources()
		if len(archSources) == 0 && qArchiveDir == "" && qArchiveS3 == "" && qProfile == "" {
			archSources = query.ResolveArchiveSources(cmd.Context(), db)
		}
	}

	// ── Coverage warnings and per-partition routing ───────────────────────────
	var plan *query.QueryPlan
	if !qNoArchive && (len(archSources) > 0 || since != nil || until != nil) {
		cfg, parseErr := mysqldriver.ParseDSN(qIndexDSN)
		if parseErr != nil {
			slog.Warn("could not parse DSN for query planning", "error", parseErr)
		} else if cfg.DBName != "" {
			plan = query.RunPlanAndWarn(cmd.Context(), db, cfg.DBName, since, until)
		}
	}

	// When no archive sources are configured, take the fast path (fetch + format
	// in one step, same as before this feature was added).
	if len(archSources) == 0 {
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
		if n >= qLimit {
			fmt.Fprintf(os.Stderr, "Warning: results truncated at %d rows. Use a narrower time range or --limit to adjust.\n", qLimit)
		}
		return nil
	}

	// ── Fetch from index + archives, then merge ───────────────────────────────
	// Each source applies ORDER BY + LIMIT independently. The global top-K is
	// always a subset of the union of per-source top-K results (all sources
	// sort by the same key), so MergeResults correctly picks the final top-K.
	fetchOpts := opts

	// When the planner says MySQL can be skipped (entire range is archived),
	// avoid the unnecessary MySQL query.
	var results []query.ResultRow
	if plan != nil && plan.SkipMySQL() {
		slog.Debug("planner: skipping MySQL query (range fully archived)")
	} else {
		results, err = engine.Fetch(cmd.Context(), fetchOpts)
		if err != nil {
			return err
		}
	}

	archResults, err := queryArchiveSources(
		cmd.Context(),
		archSources,
		fetchOpts,
		parquetquery.Fetch,
		os.Stderr,
	)
	if err != nil {
		return err
	}
	results = append(results, archResults...)

	results = query.MergeResults(results, opts.Limit)

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
	if n >= qLimit {
		fmt.Fprintf(os.Stderr, "Warning: results truncated at %d rows. Use a narrower time range or --limit to adjust.\n", qLimit)
	}
	return nil
}

// queryArchiveSources is the single choke point for issue #203: it fetches
// events from each archive source, surfaces per-source failures on stderr
// (independent of log level), and aborts the whole query immediately on
// context cancellation instead of iterating every remaining source printing
// a warning for each.
//
// Contract:
//
//   - Success path: accumulates events from every source in order (no dedup —
//     MergeResults runs at the call site) and returns (rows, nil). rows is
//     nil when sources is empty or every source returns zero rows.
//   - Plain fetch error: emits a visible stderr warning AND a structured
//     slog.Warn for that source, then continues to the next source. Both
//     channels must fire. The stderr path exists specifically so that it
//     does NOT depend on slog configuration — an operator who has changed
//     --log-level, --log-format, or has a misconfigured slog.Default() must
//     still see the warning. A log-level or log-format change must never be
//     able to silence a #203 warning. Operators running the default text
//     handler will see both lines; that duplication is deliberate and is the
//     price of the visibility guarantee. This invariant is pinned by
//     TestQueryArchiveSources_plainErrorKeepsGoingWithDualChannel; do not
//     drop either channel without updating that test.
//   - Context canceled / deadline exceeded: returns a wrapped context error
//     and stops iterating. No stderr warning and no slog.Warn for the
//     canceled source — a Ctrl-C'd query should not dump per-source noise
//     before exiting. The caller bubbles the error through cobra to
//     os.Exit(non-zero).
//
// Partial-drop on cancellation: when the helper short-circuits mid-loop, any
// archive rows already accumulated from earlier sources are dropped along
// with any live-MySQL rows the caller had fetched before this call. That is
// a UX tradeoff, not an oversight — a canceled query is an incomplete query,
// and showing partial results alongside a "canceled" error would invite the
// operator to treat them as authoritative. If that tradeoff needs to change
// (e.g. to flush partial rows on timeout), the change belongs at the call
// site in runQuery, not inside this helper.
//
// The cancellation detection path has two checks because the fetch error
// itself can wrap context.Canceled/context.DeadlineExceeded before the
// ambient ctx.Err() transitions (child-context races, DuckDB/httpfs
// cancellation propagation). Either signal aborts the loop. The checks use
// errors.Is, which walks the standard Unwrap chain — including errors.Join
// trees — but does NOT detect custom cancel causes from
// context.WithCancelCause. Nothing in bintrail uses WithCancelCause for the
// query path, so that gap is only theoretical; noting it so a future
// maintainer who adds WithCancelCause elsewhere knows to extend the check.
//
// The fetch parameter is injected (typed as query.ArchiveFetcher so the
// signature stays in lockstep with the shared FetchMerged pipeline) so unit
// tests drive the real loop body with a fake fetcher — no DuckDB, no real
// database, and the exact same code path that production hits. Similarly
// stderr is an io.Writer so tests capture into a bytes.Buffer without
// touching os.Stderr.
//
// Stderr messages are sanitized against every line-terminator character via
// sanitizeArchiveErrorMessage so multi-line DuckDB and AWS SDK errors do not
// split across lines — breaking line-oriented stderr consumers (grep,
// systemd-journald message framing, log shippers keyed on line prefix) was
// the concrete class of regression that prompted the sanitization step.
func queryArchiveSources(
	ctx context.Context,
	sources []string,
	opts query.Options,
	fetch query.ArchiveFetcher,
	stderr io.Writer,
) ([]query.ResultRow, error) {
	var results []query.ResultRow
	for _, src := range sources {
		ar, err := fetch(ctx, opts, src)
		if err != nil {
			// Dual cancellation check: ambient ctx + the fetch error chain.
			// See the doc comment above for the race this guards against.
			if cerr := ctx.Err(); cerr != nil {
				return nil, fmt.Errorf("query canceled: %w", cerr)
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("query canceled: %w", err)
			}
			// stderr gets the sanitized form (one line = one failure).
			// slog.Warn gets the raw error so structured-log handlers can
			// preserve the full text — a JSON handler encodes embedded
			// newlines natively, and a future "consistency" refactor that
			// passes the sanitized form to slog would silently degrade the
			// primary debuggability channel.
			fmt.Fprintf(stderr, "Warning: archive query failed for %s: %s\n",
				src, sanitizeArchiveErrorMessage(err))
			slog.Warn("archive query failed, skipping", "source", src, "error", err)
			continue
		}
		results = append(results, ar...)
	}
	return results, nil
}

// lineBreakReplacer rewrites every kind of line terminator that can appear in
// a Go error message to " | " so that sanitizeArchiveErrorMessage always
// produces a single-line result. Order matters: "\r\n" MUST be listed before
// the bare "\r" and "\n" replacements, otherwise CRLF input would expand to
// " |  | " instead of a single separator. strings.NewReplacer guarantees
// left-to-right longest-match semantics so the rule is: compound first, then
// singles.
//
// The characters covered are:
//
//   - "\r\n" and "\r" — CRLF from AWS SDK error chains that bubble through
//     stringified *http.Response bodies; bare "\r" on a tty overwrites the
//     stderr line and hides part of the warning.
//   - "\n" — the common case (DuckDB Binder/Parser errors).
//   - "\v" (vertical tab) and "\f" (form feed) — rare but can appear in
//     errors from text/template and some validation libraries; both break
//     line-oriented stderr consumers on some platforms.
//
// Unicode line separators (NEL U+0085, LS U+2028, PS U+2029) are intentionally
// NOT handled — they only appear in error messages when the underlying error
// embeds JSON-escaped user data, which is not a shape bintrail emits.
var lineBreakReplacer = strings.NewReplacer(
	"\r\n", " | ",
	"\r", " | ",
	"\n", " | ",
	"\v", " | ",
	"\f", " | ",
)

// sanitizeArchiveErrorMessage collapses every line-terminator character in an
// error message to " | " so that a single archive failure always occupies
// exactly one stderr line. DuckDB Binder/Parser errors and AWS SDK errors are
// the common offenders; see lineBreakReplacer for the full character list.
//
// Extracted from queryArchiveSources so the behavior has its own table-driven
// test (TestSanitizeArchiveErrorMessage) that can exercise edge cases
// independently of the archive fetch loop.
func sanitizeArchiveErrorMessage(err error) string {
	return lineBreakReplacer.Replace(err.Error())
}

// archiveSources returns the Hive-scoped archive source paths for the current
// --bintrail-id. Each source points to the bintrail_id=<uuid> subdirectory so
// DuckDB only reads files for this server.
func archiveSources() []string {
	var sources []string
	if qArchiveDir != "" {
		sources = append(sources, filepath.Join(qArchiveDir, "bintrail_id="+qBintrailID))
	}
	if qArchiveS3 != "" {
		base := strings.TrimSuffix(qArchiveS3, "/")
		sources = append(sources, base+"/bintrail_id="+qBintrailID)
	}
	return sources
}

