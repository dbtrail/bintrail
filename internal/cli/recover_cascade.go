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

On MySQL <= 8.x and MariaDB, InnoDB runs FK cascades below the binlog (fixed in
MySQL 9.6), so only the parent DELETE is logged — the cascaded child deletes are
invisible to plain
` + "`recover`" + ` (MySQL Bug #32506). This command finds the deleted parent rows in
the index, infers which child rows referenced them in their last indexed state,
and emits reversal SQL that re-inserts BOTH the parent rows and their
cascade-deleted descendants, wrapped in SET FOREIGN_KEY_CHECKS=0/1.

It NEVER executes SQL — review the dry-run/output before applying.

Phase-1 (binlog-window) recovery: a child untouched within --lookback and not in
a baseline cannot be reconstructed (baseline fallback is tracked in #552).
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

	// Coverage caveats accumulate here (detectable gaps that gate exit); the
	// always-on Phase-1 scope note is separate and printed unconditionally.
	var caveats []string

	// A plain empty match is legitimately "complete", but the operator must not
	// read silence as "nothing was deleted" — it could be a wrong filter.
	if len(parentDeletes) == 0 {
		slog.Warn("no parent DELETE events matched in the live index; verify --schema/--table/--pk/--since/--until")
	}

	// Live-only trap: cascade recovery searches the LIVE index only.
	//   - probe failure  → we cannot tell whether archives exist → coverage
	//     unknown (hard caveat).
	//   - archives exist AND nothing matched live → the deleted parent may itself
	//     be archived → hard caveat (the dangerous "nothing found" case).
	//   - archives exist but parents WERE found → a child whose events were
	//     archived could still be missed → a visible warning, NOT a hard caveat:
	//     otherwise every archived deployment trips INCOMPLETE on every run and
	//     --allow-incomplete becomes routine, masking the real coverage gaps.
	if archives, aerr := query.ResolveArchiveSources(cmd.Context(), db); aerr != nil {
		caveats = append(caveats, "could not determine whether archived partitions exist (probe failed: "+aerr.Error()+"); coverage is unknown")
	} else if len(archives) > 0 {
		if len(parentDeletes) == 0 {
			caveats = append(caveats, "no parent DELETE matched in the live index, but the index has archived partitions (cascade recovery does NOT search them); the deleted parent may be archived")
		} else {
			slog.Warn("index has archived partitions, which cascade recovery does NOT search (live index only); a child whose events were archived may be missed")
		}
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
			Parents          int      `json:"parents"`
			Children         int      `json:"children"`
			Statements       int      `json:"statements"`
			Complete         bool     `json:"complete"`
			OperationalError bool     `json:"operational_error,omitempty"`
			Incomplete       []string `json:"incomplete,omitempty"`
			Output           string   `json:"output,omitempty"`
			SQL              string   `json:"sql,omitempty"`
		}{
			Parents: len(parentDeletes), Children: len(res.Victims), Statements: n,
			Complete: len(caveats) == 0 && synthErr == nil, OperationalError: synthErr != nil,
			Incomplete: caveats, Output: rcOutput,
		}
		if rcOutput == "" {
			out.SQL = buf.String()
		}
		if err := cliutil.OutputJSON(out); err != nil {
			return err
		}
		// Same exit contract as text mode: the `complete` field is on stdout, but
		// a consumer gating on EXIT CODE must still see a non-zero exit when the
		// recovery is partial. Returning an error here makes the root emit
		// {"error":...} to stderr while the result stays on stdout (#568 review).
		dest := "stdout"
		if rcOutput != "" {
			dest = rcOutput
		}
		return cascadeExit(dest, synthErr, caveats, rcAllowIncomplete)
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

	return cascadeExit(dest, synthErr, caveats, rcAllowIncomplete)
}

// cascadeExit returns the error the command should end with, shared by text and
// JSON modes so the exit-code contract is identical: an operational failure
// always exits non-zero (even with --allow-incomplete); detectable coverage
// gaps exit non-zero unless --allow-incomplete. The SQL is already durable by
// the time this is called, so the error reports "written but partial".
func cascadeExit(dest string, synthErr error, caveats []string, allowIncomplete bool) error {
	if synthErr != nil {
		return fmt.Errorf("SQL written to %s but synthesis hit an operational failure (result is partial): %w", dest, synthErr)
	}
	if len(caveats) > 0 && !allowIncomplete {
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
