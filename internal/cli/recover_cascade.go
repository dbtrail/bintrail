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

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/cascaderecover"
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
	// The baseline's exact recorded binlog position, when it has one (#797) —
	// see BaselineLookup.SincePos. Best-effort: a read failure just leaves the
	// candidate-victim fetch anchored on SnapshotTime alone, same as before
	// #797 — it must not block the (already-succeeded) baseline row scan below.
	var sincePos *query.BinlogPos
	if bmeta, berr := baseline.ReadParquetMetadataAny(ctx, path); berr != nil {
		slog.Warn("cascade: could not read baseline metadata for position-anchored victim fetch; falling back to timestamp-only Since",
			"schema", schema, "table", table, "path", path, "error", berr)
	} else if bmeta.BinlogFile != "" && bmeta.BinlogPos > 0 {
		sincePos = &query.BinlogPos{File: bmeta.BinlogFile, Pos: uint64(bmeta.BinlogPos)}
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
	return cascade.BaselineLookup{SnapshotTime: snap, Rows: out, Truncated: trunc, SincePos: sincePos}, true, nil
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
	f.StringVar(&rcSince, "since", "", "Only parent deletes at or after this time (2006-01-02 15:04:05, interpreted as UTC; use RFC3339 with an explicit offset, e.g. 2006-01-02T15:04:05-05:00, for another zone)")
	f.StringVar(&rcUntil, "until", "", "Only parent deletes at or before this time (2006-01-02 15:04:05, interpreted as UTC; use RFC3339 with an explicit offset, e.g. 2006-01-02T15:04:05-05:00, for another zone)")
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
	// (their WHERE needs the child PK columns) — cascaderecover.EmitSQL errors
	// loudly, before writing anything, if SET NULL rows exist with a nil resolver.
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
		// FK graph resolved PER ROOT, not batch-anchored on the earliest root:
		// a --pks/--since/--until batch can span an FK topology change, and a
		// single earliest-anchored graph would silently mis-recover a later
		// root (#834 applied per-root, not once for the whole batch).
		groups, fkCaveats, lerr := cascade.GroupParentDeletesByFKGraph(cmd.Context(), db, rcSchema, parentDeletes)
		if lerr != nil {
			return fmt.Errorf("load FK graph: %w", lerr)
		}
		caveats = append(caveats, fkCaveats...)
		results := make([]cascade.Result, 0, len(groups))
		for _, g := range groups {
			r, serr := cascade.SynthesizeVictims(cmd.Context(), eng, g.FKs, g.Roots, cascade.Options{
				Lookback:        lookback,
				MaxDepth:        rcMaxDepth,
				Baseline:        baselineProvider,
				ArchivesPresent: archivesExist,
			})
			results = append(results, r)
			if serr != nil {
				synthErr = errors.Join(synthErr, serr)
			}
		}
		res = cascade.MergeResults(results...)
	}
	caveats = append(caveats, res.Incomplete...)
	if synthErr != nil {
		caveats = append(caveats, "an index query failed mid-synthesis; the result is partial: "+synthErr.Error())
	}

	rows := append(append([]query.ResultRow{}, parentDeletes...), res.Victims...)

	// ── Emit ──────────────────────────────────────────────────────────────────
	hdr := cascaderecover.Header{
		Schema:         rcSchema,
		Table:          rcTable,
		Parents:        len(parentDeletes),
		Children:       len(res.Victims),
		Caveats:        caveats,
		BaselineActive: baselineProvider != nil,
	}

	if rcFormat == "json" {
		var buf bytes.Buffer
		n, gerr := cascaderecover.EmitSQL(&buf, recovery.New(db, resolver), rows, res.SetNullRows, resolver, hdr)
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
	n, gerr := cascaderecover.EmitSQL(w, recovery.New(db, resolver), rows, res.SetNullRows, resolver, hdr)
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
