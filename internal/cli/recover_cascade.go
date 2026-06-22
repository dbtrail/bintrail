package cli

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

var recoverCascadeCmd = &cobra.Command{
	Use:   "recover-cascade",
	Short: "Generate reversal SQL for rows deleted by a foreign-key ON DELETE CASCADE",
	Long: `Reconstruct rows that an InnoDB foreign-key ON DELETE CASCADE removed but
never wrote to the binary log.

On MySQL <= 8.x (and MariaDB) InnoDB runs FK cascades below the binlog, so only
the parent DELETE is logged — the cascaded child deletes are invisible to plain
` + "`recover`" + ` (MySQL Bug #32506). This command finds the deleted parent rows in
the index, infers which child rows referenced them in their last indexed state,
and emits reversal SQL that re-inserts BOTH the parent rows and their
cascade-deleted descendants, wrapped in SET FOREIGN_KEY_CHECKS=0/1.

It NEVER executes SQL — review the dry-run/output before applying.

Phase-1 (binlog-window) recovery: a child untouched within --lookback and not in
a baseline cannot be reconstructed (baseline fallback is tracked in #548/#552).
The command searches the live index only — archived partitions are not scanned;
when they exist, the output is flagged incomplete.

Examples:
  # Preview recovery for one accidentally-deleted parent and its cascade
  bintrail recover-cascade --index-dsn "..." \
    --schema shop --table orders --pk '42' --dry-run

  # All order deletes in a window, written to a file
  bintrail recover-cascade --index-dsn "..." \
    --schema shop --table orders \
    --since "2026-06-21 14:00:00" --until "2026-06-21 14:10:00" \
    --output cascade-recovery.sql`,
	RunE: runRecoverCascade,
}

var (
	rcIndexDSN        string
	rcSchema          string
	rcTable           string
	rcPK              string
	rcPKs             []string
	rcSince           string
	rcUntil           string
	rcOutput          string
	rcDryRun          bool
	rcFormat          string
	rcLookback        string
	rcMaxDepth        int
	rcLimit           int
	rcAllowIncomplete bool
)

func init() {
	f := recoverCascadeCmd.Flags()
	f.StringVar(&rcIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	f.StringVar(&rcSchema, "schema", "", "Schema of the parent table whose delete cascaded (required)")
	f.StringVar(&rcTable, "table", "", "Parent table whose ON DELETE CASCADE removed children (required)")
	f.StringVar(&rcPK, "pk", "", "Restrict to a single deleted parent PK (pipe-delimited for composite PKs)")
	f.StringSliceVar(&rcPKs, "pks", nil, "Restrict to multiple deleted parent PKs (comma-separated or repeated); mutually exclusive with --pk")
	f.StringVar(&rcSince, "since", "", "Only parent deletes at or after this time (2006-01-02 15:04:05)")
	f.StringVar(&rcUntil, "until", "", "Only parent deletes at or before this time (2006-01-02 15:04:05)")
	f.StringVar(&rcOutput, "output", "", "Write recovery SQL to this file (required unless --dry-run)")
	f.BoolVar(&rcDryRun, "dry-run", false, "Print recovery SQL to stdout instead of writing a file")
	f.StringVar(&rcFormat, "format", "text", "Output format: text or json")
	f.StringVar(&rcLookback, "lookback", "30d", "How far before each parent delete to search for child state (e.g. 30d, 24h)")
	f.IntVar(&rcMaxDepth, "max-depth", 5, "Maximum cascade recursion depth (parent -> child -> grandchild ...)")
	f.IntVar(&rcLimit, "limit", 1000, "Maximum number of parent DELETE events to process")
	f.BoolVar(&rcAllowIncomplete, "allow-incomplete", false, "Exit 0 even when the reconstruction is provably partial (coverage gaps only; an operational failure still exits non-zero)")
	_ = recoverCascadeCmd.MarkFlagRequired("index-dsn")
	_ = recoverCascadeCmd.MarkFlagRequired("schema")
	_ = recoverCascadeCmd.MarkFlagRequired("table")
	BindCommandEnv(recoverCascadeCmd)
}

func runRecoverCascade(cmd *cobra.Command, args []string) error {
	start := time.Now()

	// ── Validate flags ────────────────────────────────────────────────────────
	if !cliutil.IsValidOutputFormat(rcFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", rcFormat)
	}
	if !rcDryRun && rcOutput == "" {
		return fmt.Errorf("one of --output or --dry-run is required")
	}
	if rcPK != "" && len(rcPKs) > 0 {
		return fmt.Errorf("--pk and --pks are mutually exclusive; use one or the other")
	}
	if rcMaxDepth < 1 {
		return fmt.Errorf("--max-depth must be >= 1")
	}
	if rcLimit < 1 {
		return fmt.Errorf("--limit must be >= 1")
	}
	cleanedPKs, err := cleanPKList(rcPKs)
	if err != nil {
		return err
	}
	rcPKs = cleanedPKs
	lookback, err := cliutil.ParseRetain(rcLookback)
	if err != nil {
		return fmt.Errorf("--lookback: %w", err)
	}
	since, err := cliutil.ParseTime(rcSince)
	if err != nil {
		return fmt.Errorf("--since: %w", err)
	}
	until, err := cliutil.ParseTime(rcUntil)
	if err != nil {
		return fmt.Errorf("--until: %w", err)
	}

	// ── Connect + migrate ─────────────────────────────────────────────────────
	db, err := config.Connect(rcIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()
	if err := indexer.EnsureSchema(db); err != nil {
		return fmt.Errorf("schema migration: %w", err)
	}

	// Resolver enables PK-only WHERE clauses; best-effort (recovery here only
	// emits INSERTs, so a nil resolver is harmless).
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		slog.Warn("could not load schema snapshot; recovery INSERTs still use full row images", "error", err)
		resolver = nil
	}

	eng := query.New(db)
	del := event.EventDelete

	// ── Fetch the parent DELETE events (live index only) ──────────────────────
	parentDeletes, err := eng.Fetch(cmd.Context(), query.Options{
		Schema:     rcSchema,
		Table:      rcTable,
		PKValues:   rcPK,
		PKValuesIn: rcPKs,
		EventType:  &del,
		Since:      since,
		Until:      until,
		Order:      "ASC",
		Limit:      rcLimit,
	})
	if err != nil {
		return fmt.Errorf("fetch parent deletes: %w", err)
	}

	// Coverage caveats accumulate here (detectable gaps only); the always-on
	// Phase-1 scope note is separate and printed unconditionally.
	var caveats []string

	// Live-only trap: if the index has archived partitions, a parent or child
	// whose events were rotated out is invisible here. Signal it rather than
	// let "nothing found" read as "nothing to recover".
	if archives, aerr := query.ResolveArchiveSources(cmd.Context(), db); aerr != nil {
		slog.Warn("could not check for archived partitions", "error", aerr)
	} else if len(archives) > 0 {
		caveats = append(caveats, "the index has archived partitions, which cascade recovery does NOT search (live index only); events rotated out may be missed")
	}

	if len(parentDeletes) >= rcLimit {
		caveats = append(caveats, fmt.Sprintf("parent DELETE events were capped at --limit=%d; narrow --pk/--since/--until or raise --limit", rcLimit))
	}

	// ── Synthesize the cascade victims ────────────────────────────────────────
	var res cascade.Result
	var synthErr error
	if len(parentDeletes) > 0 {
		fks, lerr := cascade.LoadCascadeFKs(cmd.Context(), db, []string{rcSchema})
		if lerr != nil {
			return fmt.Errorf("load FK graph: %w", lerr)
		}
		res, synthErr = cascade.SynthesizeVictims(cmd.Context(), eng, fks, parentDeletes, cascade.Options{
			Lookback: lookback,
			MaxDepth: rcMaxDepth,
		})
	}
	caveats = append(caveats, res.Incomplete...)
	if synthErr != nil {
		caveats = append(caveats, "an index query failed mid-synthesis; the result is partial: "+synthErr.Error())
	}

	rows := append(append([]query.ResultRow{}, parentDeletes...), res.Victims...)

	// ── Emit ──────────────────────────────────────────────────────────────────
	hdr := cascadeHeader{
		schema:   rcSchema,
		table:    rcTable,
		parents:  len(parentDeletes),
		children: len(res.Victims),
		caveats:  caveats,
	}

	if rcFormat == "json" {
		var buf bytes.Buffer
		n, gerr := emitCascadeSQL(&buf, recovery.New(db, resolver), rows, hdr)
		if gerr != nil {
			return gerr
		}
		if rcOutput != "" {
			if werr := os.WriteFile(rcOutput, buf.Bytes(), 0o600); werr != nil {
				return fmt.Errorf("failed to write output file %q: %w", rcOutput, werr)
			}
		}
		out := struct {
			Parents    int      `json:"parents"`
			Children   int      `json:"children"`
			Statements int      `json:"statements"`
			Complete   bool     `json:"complete"`
			Incomplete []string `json:"incomplete,omitempty"`
			Output     string   `json:"output,omitempty"`
			SQL        string   `json:"sql,omitempty"`
		}{
			Parents: len(parentDeletes), Children: len(res.Victims), Statements: n,
			Complete: len(caveats) == 0 && synthErr == nil, Incomplete: caveats,
			Output: rcOutput,
		}
		if rcOutput == "" {
			out.SQL = buf.String()
		}
		// JSON carries `complete`; the consumer branches on it, so exit 0.
		return cliutil.OutputJSON(out)
	}

	var w io.Writer = os.Stdout
	var closeFn func() error
	if rcOutput != "" {
		f, ferr := os.OpenFile(rcOutput, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if ferr != nil {
			return fmt.Errorf("failed to create output file %q: %w", rcOutput, ferr)
		}
		bw := bufio.NewWriter(f)
		w = bw
		closeFn = func() error {
			if e := bw.Flush(); e != nil {
				return e
			}
			return f.Close()
		}
	}
	n, gerr := emitCascadeSQL(w, recovery.New(db, resolver), rows, hdr)
	if gerr != nil {
		return gerr
	}
	if closeFn != nil {
		if e := closeFn(); e != nil {
			return fmt.Errorf("failed to flush output file: %w", e)
		}
	}

	dest := "stdout"
	if rcOutput != "" {
		dest = rcOutput
	}
	for _, c := range caveats {
		slog.Warn("cascade recovery incomplete", "reason", c)
	}
	slog.Info("cascade recovery SQL generated",
		"parents", len(parentDeletes), "children", len(res.Victims), "statements", n,
		"complete", len(caveats) == 0 && synthErr == nil,
		"output", dest, "duration_ms", time.Since(start).Milliseconds())

	// An operational failure always exits non-zero. Coverage gaps exit non-zero
	// unless the operator opted in with --allow-incomplete.
	if synthErr != nil {
		return fmt.Errorf("SQL written to %s but synthesis hit an operational failure (result is partial): %w", dest, synthErr)
	}
	if len(caveats) > 0 && !rcAllowIncomplete {
		return fmt.Errorf("SQL written to %s but the recovery is INCOMPLETE (%d caveat(s) above); review, then re-run with --allow-incomplete to exit 0", dest, len(caveats))
	}
	return nil
}

// cascadeHeader carries the values rendered into the SQL preamble.
type cascadeHeader struct {
	schema, table     string
	parents, children int
	caveats           []string
}

// emitCascadeSQL writes the documented preamble, the FK-checks-off wrapper, and
// the reversal statements, returning the statement count from the generator.
func emitCascadeSQL(w io.Writer, gen *recovery.Generator, rows []query.ResultRow, hdr cascadeHeader) (int, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "-- bintrail recover-cascade: reverse ON DELETE CASCADE side effects on %s.%s\n", hdr.schema, hdr.table)
	fmt.Fprintf(&b, "-- Re-inserts %d deleted parent row(s) and %d cascade-deleted child row(s)\n", hdr.parents, hdr.children)
	b.WriteString("-- that InnoDB removed below the binlog (MySQL Bug #32506). NEVER auto-applied.\n")
	b.WriteString("--\n")
	b.WriteString("-- Phase-1 (binlog-window) recovery: a child untouched within --lookback and not\n")
	b.WriteString("-- in a baseline is NOT reconstructed (baseline fallback: #552). \"Complete\" means\n")
	b.WriteString("-- everything DETECTABLE was recovered, not that no row was missed.\n")
	b.WriteString("--\n")
	b.WriteString("-- If you have already re-created a deleted parent, delete its INSERT below:\n")
	b.WriteString("-- SET FOREIGN_KEY_CHECKS=0 does NOT suppress PRIMARY KEY violations.\n")
	if len(hdr.caveats) > 0 {
		b.WriteString("--\n-- !!! INCOMPLETE RECOVERY — the result is provably partial:\n")
		for _, c := range hdr.caveats {
			fmt.Fprintf(&b, "--   - %s\n", c)
		}
	}
	b.WriteString("\nSET FOREIGN_KEY_CHECKS=0;\n\n")
	if _, err := io.WriteString(w, b.String()); err != nil {
		return 0, err
	}

	n, err := gen.GenerateSQLFromRows(rows, w)
	if err != nil {
		return 0, err
	}

	if _, err := io.WriteString(w, "\nSET FOREIGN_KEY_CHECKS=1;\n"); err != nil {
		return n, err
	}
	return n, nil
}
