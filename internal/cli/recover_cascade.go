package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// cascadeBaselineProvider implements cascade.BaselineProvider over
// internal/reconstruct: it finds the child table's baseline snapshot, scans it
// for rows referencing the deleted parent, and encodes each row's PK to match
// binlog_events.pk_values so the cascade engine can dedup against Phase-1.
type cascadeBaselineProvider struct {
	source   string             // local dir or s3:// prefix
	resolver *metadata.Resolver // for child PK columns
}

func (p *cascadeBaselineProvider) BaselineChildren(ctx context.Context, schema, table, fkCol, parentPK string, at time.Time, limit int) (cascade.BaselineLookup, bool, error) {
	path, snap, _, err := reconstruct.FindBaseline(ctx, p.source, schema, table, at)
	if err != nil {
		if errors.Is(err, reconstruct.ErrNoBaseline) {
			return cascade.BaselineLookup{}, false, nil // table not covered → Phase-1 only
		}
		return cascade.BaselineLookup{}, false, err
	}

	tm, err := p.resolver.Resolve(schema, table)
	if err != nil {
		return cascade.BaselineLookup{}, false, fmt.Errorf("resolve %s.%s for baseline: %w", schema, table, err)
	}
	// The FK filter binds parentPK as a STRING against the baseline column.
	// DuckDB coerces it exactly for integer/string FK columns, but for
	// DATETIME/DECIMAL/DATE the string form may not match the stored value and
	// would silently zero-match. Refuse those (flagged as a coverage gap) rather
	// than under-recover silently.
	if !fkFilterSafe(columnDataType(tm, fkCol)) {
		return cascade.BaselineLookup{}, false, fmt.Errorf(
			"baseline scan of %s.%s by FK column %q (type %q) is unsupported (string match may not coerce); baseline augmentation skipped",
			schema, table, fkCol, columnDataType(tm, fkCol))
	}

	// Fetch one more than the cap so truncation is observable.
	fetch := 0
	if limit > 0 {
		fetch = limit + 1
	}
	rows, err := reconstruct.ReadBaselineRows(ctx, path, map[string]string{fkCol: parentPK}, fetch)
	if err != nil {
		return cascade.BaselineLookup{}, false, err
	}
	trunc := false
	if limit > 0 && len(rows) > limit {
		trunc = true
		rows = rows[:limit]
	}

	pkCols := tm.PKColumnMetas()
	out := make([]cascade.BaselineRow, 0, len(rows))
	for _, r := range rows {
		// Canonicalize PK values the same way the indexer encoded pk_values, so
		// the dedup key matches a Phase-1 victim's PKValues exactly.
		canon, cerr := reconstruct.CanonicalizePKMap(r, pkCols)
		if cerr != nil {
			return cascade.BaselineLookup{}, false, fmt.Errorf("canonicalize baseline PK for %s.%s: %w", schema, table, cerr)
		}
		out = append(out, cascade.BaselineRow{
			PKValues: event.BuildPKValues(pkCols, canon),
			Row:      r,
		})
	}
	return cascade.BaselineLookup{SnapshotTime: snap, Rows: out, Truncated: trunc}, true, nil
}

func columnDataType(tm *metadata.TableMeta, name string) string {
	for _, c := range tm.Columns {
		if c.Name == name {
			return c.DataType
		}
	}
	return ""
}

// fkFilterSafe reports whether a string-bound equality filter on a column of
// this DATA_TYPE coerces exactly in DuckDB (integer + string families). Types
// where the string form may diverge from the stored value (datetime, decimal,
// date, …) are excluded so the baseline FK scan never silently zero-matches.
func fkFilterSafe(dataType string) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "int", "integer", "smallint", "tinyint", "mediumint", "bigint",
		"char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set":
		return true
	default:
		return false
	}
}

var recoverCascadeCmd = &cobra.Command{
	Use:   "recover-cascade",
	Short: "Generate reversal SQL for rows hit by a foreign-key ON DELETE CASCADE / SET NULL",
	Long: `Reconstruct the side effects of an InnoDB foreign-key ON DELETE CASCADE or
ON DELETE SET NULL that were never written to the binary log.

On MySQL <= 8.x and MariaDB, InnoDB runs FK cascades below the binlog (fixed in
MySQL 9.6), so only the parent DELETE is logged — the cascaded child deletes (and
SET NULL FK-nullings) are invisible to plain
` + "`recover`" + ` (MySQL Bug #32506). This command finds the deleted parent rows in
the index, infers which child rows referenced them in their last indexed state,
and emits reversal SQL:
  - ON DELETE CASCADE: re-INSERT the parent rows and their cascade-deleted
    descendants (recursing through multi-level cascades).
  - ON DELETE SET NULL: an idempotent UPDATE restoring each nulled FK, guarded by
    "... AND fk IS NULL" so a re-run or a later re-point is never clobbered.
All wrapped in SET FOREIGN_KEY_CHECKS=0/1.

It NEVER executes SQL — review the dry-run/output before applying.

Phase-1 (binlog-window) recovers children with a binlog event within --lookback.
A child untouched in that window (e.g. an insert-once row from months ago) needs
Phase-2: point --baseline-dir/--baseline-s3 at a ` + "`bintrail baseline`" + ` snapshot and
those untouched children are recovered from it too. When the result is provably
partial — no baseline, a per-parent overflow, or archived partitions the live
scan cannot see — it is flagged INCOMPLETE and the command exits non-zero unless
--allow-incomplete.

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
	rcBaselineDir     string
	rcBaselineS3      string
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
	f.StringVar(&rcBaselineDir, "baseline-dir", "", "Local baseline-snapshot directory for Phase-2 fallback (also recovers children present in the snapshot but untouched since it)")
	f.StringVar(&rcBaselineS3, "baseline-s3", "", "S3 baseline-snapshot prefix (s3://bucket/prefix) for Phase-2 fallback; alternative to --baseline-dir")
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

	// Resolver enables PK-only WHERE clauses. Best-effort for the CASCADE path
	// (INSERTs fall back to full row images), but REQUIRED for SET NULL restores
	// (their WHERE needs the child PK columns) — emitCascadeSQL errors loudly,
	// before writing anything, if SET NULL rows exist with a nil resolver.
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
	archivesExist := false
	if archives, aerr := query.ResolveArchiveSources(cmd.Context(), db); aerr != nil {
		caveats = append(caveats, "could not determine whether archived partitions exist (probe failed: "+aerr.Error()+"); coverage is unknown")
	} else if len(archives) > 0 {
		archivesExist = true
		if len(parentDeletes) == 0 {
			caveats = append(caveats, "no parent DELETE matched in the live index, but the index has archived partitions (cascade recovery does NOT search them); the deleted parent may be archived")
		} else {
			slog.Warn("index has archived partitions, which cascade recovery does NOT search (live index only); a child whose events were archived may be missed")
		}
	}

	if len(parentDeletes) >= rcLimit {
		caveats = append(caveats, fmt.Sprintf("parent DELETE events were capped at --limit=%d; narrow --pk/--since/--until or raise --limit", rcLimit))
	}

	// Phase-2 baseline fallback provider — enabled when --baseline-dir or
	// --baseline-s3 is set AND a schema snapshot is available (needed to encode
	// each baseline row's PK to match binlog pk_values).
	var baselineProvider cascade.BaselineProvider
	baselineSrc := rcBaselineDir
	if baselineSrc == "" {
		baselineSrc = rcBaselineS3
	}
	if baselineSrc != "" {
		if resolver == nil {
			slog.Warn("baseline source set but no schema snapshot is available; Phase-2 fallback disabled (run `bintrail snapshot`)")
		} else {
			baselineProvider = &cascadeBaselineProvider{source: baselineSrc, resolver: resolver}
		}
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
			Lookback:        lookback,
			MaxDepth:        rcMaxDepth,
			Baseline:        baselineProvider,
			ArchivesPresent: archivesExist,
		})
	}
	caveats = append(caveats, res.Incomplete...)
	if synthErr != nil {
		caveats = append(caveats, "an index query failed mid-synthesis; the result is partial: "+synthErr.Error())
	}

	rows := append(append([]query.ResultRow{}, parentDeletes...), res.Victims...)

	// ── Emit ──────────────────────────────────────────────────────────────────
	hdr := cascadeHeader{
		schema:         rcSchema,
		table:          rcTable,
		parents:        len(parentDeletes),
		children:       len(res.Victims),
		setnulls:       len(res.SetNullRows),
		caveats:        caveats,
		baselineActive: baselineProvider != nil,
	}

	if rcFormat == "json" {
		var buf bytes.Buffer
		n, gerr := emitCascadeSQL(&buf, recovery.New(db, resolver), rows, res.SetNullRows, resolver, hdr)
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
			SetNullRestores  int      `json:"set_null_restores"`
			Statements       int      `json:"statements"`
			Complete         bool     `json:"complete"`
			OperationalError bool     `json:"operational_error,omitempty"`
			Incomplete       []string `json:"incomplete,omitempty"`
			Output           string   `json:"output,omitempty"`
			SQL              string   `json:"sql,omitempty"`
		}{
			Parents: len(parentDeletes), Children: len(res.Victims), SetNullRestores: len(res.SetNullRows), Statements: n,
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
	n, gerr := emitCascadeSQL(w, recovery.New(db, resolver), rows, res.SetNullRows, resolver, hdr)
	if gerr != nil {
		if closeFn != nil {
			_ = closeFn() // best-effort: gerr is the real failure, don't mask it (and don't leak the fd)
		}
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
	setnulls          int
	caveats           []string
	baselineActive    bool
}

// emitCascadeSQL writes the documented preamble, the FK-checks-off wrapper, the
// CASCADE reversal statements (DELETE→INSERT via the generator), and the SET
// NULL FK restorations (idempotent guarded UPDATEs). Returns the total statement
// count. resolver supplies child PK columns for the SET NULL WHERE clauses.
func emitCascadeSQL(w io.Writer, gen *recovery.Generator, rows []query.ResultRow, setNullRows []cascade.SetNullRestore, resolver *metadata.Resolver, hdr cascadeHeader) (int, error) {
	// Build every SET NULL restoration BEFORE writing a byte (all-or-nothing): a
	// missing resolver, an unresolvable table, or an absent PK column must abort
	// the whole emit cleanly — returning mid-script would leave the parent/child
	// INSERTs written but drop the closing `SET FOREIGN_KEY_CHECKS=1`, handing the
	// operator a script that re-enables nothing.
	var setNullStmts []string
	if len(setNullRows) > 0 {
		if resolver == nil {
			return 0, fmt.Errorf("a schema snapshot is required to restore SET NULL foreign keys (run `bintrail snapshot`)")
		}
		for _, sr := range setNullRows {
			tm, terr := resolver.Resolve(sr.Schema, sr.Table)
			if terr != nil {
				return 0, fmt.Errorf("resolve %s.%s for SET NULL restore: %w", sr.Schema, sr.Table, terr)
			}
			stmt, ferr := recovery.FormatSetNullRestore(sr.Schema, sr.Table, sr.Column, sr.Value, tm.PKColumnMetas(), sr.Row)
			if ferr != nil {
				return 0, ferr
			}
			setNullStmts = append(setNullStmts, stmt)
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "-- bintrail recover-cascade: reverse ON DELETE CASCADE / SET NULL side effects on %s.%s\n", hdr.schema, hdr.table)
	fmt.Fprintf(&b, "-- Re-inserts %d deleted parent row(s) and %d cascade-deleted child row(s); restores %d SET NULL'd FK(s)\n", hdr.parents, hdr.children, hdr.setnulls)
	b.WriteString("-- that InnoDB removed/nulled below the binlog (MySQL Bug #32506). NEVER auto-applied.\n")
	b.WriteString("--\n")
	if hdr.baselineActive {
		b.WriteString("-- Phase-2 baseline fallback ACTIVE: children present in a covered baseline are\n")
		b.WriteString("-- reconstructed even if untouched within the window. Tables NOT covered by a\n")
		b.WriteString("-- baseline are flagged above. \"Complete\" means everything DETECTABLE was recovered.\n")
	} else {
		b.WriteString("-- Phase-1 (binlog-window) recovery: a child untouched within --lookback and not\n")
		b.WriteString("-- in a baseline is NOT reconstructed — pass --baseline-dir/--baseline-s3 to enable\n")
		b.WriteString("-- Phase-2 fallback. \"Complete\" means everything DETECTABLE was recovered.\n")
	}
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

	// SET NULL restorations: idempotent UPDATEs (… AND fk IS NULL) that only
	// touch rows still in the post-cascade nulled state, so a re-run or a later
	// re-point of the child is never clobbered. Pre-built above, so nothing here
	// can fail after the INSERTs are already on disk.
	if len(setNullStmts) > 0 {
		if _, err := io.WriteString(w, "\n-- SET NULL FK restorations (idempotent: only rows whose FK is still NULL):\n"); err != nil {
			return n, err
		}
		for _, stmt := range setNullStmts {
			if _, werr := io.WriteString(w, stmt+";\n"); werr != nil {
				return n, werr
			}
			n++
		}
	}

	if _, err := io.WriteString(w, "\nSET FOREIGN_KEY_CHECKS=1;\n"); err != nil {
		return n, err
	}
	return n, nil
}
