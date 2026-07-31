package cli

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// pkChangeSuspected reports whether a single-row lookup that found no baseline
// row is better explained by a PK-changing UPDATE than by a genuinely-absent
// row (#782). Change events are stored keyed by their before-image PK, so an
// `UPDATE pk old→new` never appears in a fetch filtered by `new`; the row still
// looks absent even though it existed. Signature: the earliest fetched event
// for the searched PK is an UPDATE or DELETE — the row was assumed to pre-exist
// that event, yet the baseline (its only legitimate origin besides an INSERT)
// has no such row. events must be sorted ascending (FetchMerged guarantees it).
func pkChangeSuspected(events []query.ResultRow) bool {
	return len(events) > 0 && events[0].EventType != event.EventInsert
}

// resolvePKMetas loads the searched table's primary-key column metadata from
// the latest schema snapshot. Best-effort by design: every caller below only
// uses it to IMPROVE a lookup or an error message, so a missing/unreadable
// snapshot degrades to the pre-#1155 behaviour instead of failing a
// reconstruct that would otherwise have worked.
func resolvePKMetas(db *sql.DB, schema, table string) []metadata.ColumnMeta {
	res, err := metadata.NewResolver(db, 0)
	if err != nil {
		slog.Debug("could not load schema snapshot for PK metadata", "error", err)
		return nil
	}
	tm, err := res.Resolve(schema, table)
	if err != nil {
		slog.Debug("could not resolve table for PK metadata", "error", err)
		return nil
	}
	return tm.PKColumnMetas()
}

// unsupportedPKType returns the first primary-key column whose type the
// baseline canonicalizer cannot handle, or nil when every column is supported.
//
// An EMPTY DataType is not a verdict. It is the PostgreSQL snapshot signature —
// metadata.WritePGSnapshot leaves both data_type and column_type empty (#533),
// and single-row reconstruct deliberately runs generically for a PG source
// (whose raw-text baseline makes every PK a string-identity match, so it never
// needs the MySQL canonicalizer). Treating it as unsupported would tell every
// PostgreSQL operator their schema is unsupported when it works.
func unsupportedPKType(pkMetas []metadata.ColumnMeta) *metadata.ColumnMeta {
	for i, c := range pkMetas {
		if strings.TrimSpace(c.DataType) == "" {
			continue
		}
		if !reconstruct.SupportedPKType(c.DataType) {
			return &pkMetas[i]
		}
	}
	return nil
}

// indexPKSpelling rewrites a --pk value into the spelling the indexer stored in
// binlog_events.pk_values, so the event fetch matches what the operator typed.
//
// Only fixed-width BINARY(n) components are touched, and this is the INVERSE of
// padFixedBinaryFilter — the two run in opposite directions on purpose, because
// they target different stores. Reproducing event.formatPKValue exactly:
// trailing 0x00 padding is stripped (the ROW image never carries it), and the
// hex is uppercased, but ONLY when the trimmed bytes are not valid UTF-8 —
// formatPKValue is content-gated, so a binary key whose bytes are printable
// ASCII is stored verbatim and must stay that way.
//
// Everything else — every other column type, and every component that is
// already in the stored spelling — is returned untouched, so this cannot
// disturb a lookup that resolves today.
func indexPKSpelling(pk string, pkMetas []metadata.ColumnMeta) string {
	if pk == "" || len(pkMetas) == 0 {
		return pk
	}
	parts := strings.Split(pk, "|")
	if len(parts) != len(pkMetas) {
		// --pk/--pk-columns arity is validated against the operator's
		// --pk-columns, not against the snapshot; if the two disagree, leave
		// the value alone rather than re-spell the wrong component.
		return pk
	}
	changed := false
	for i, c := range pkMetas {
		if !strings.EqualFold(strings.TrimSpace(c.DataType), "binary") {
			continue
		}
		raw, isHex := decodeHexPKValue(parts[i])
		if !isHex {
			continue // already the verbatim/stored spelling
		}
		trimmed := reconstruct.TrimFixedBinaryPad(raw)
		var spelled string
		if utf8.Valid(trimmed) {
			spelled = string(trimmed)
		} else {
			spelled = "0x" + strings.ToUpper(hex.EncodeToString(trimmed))
		}
		if spelled != parts[i] {
			parts[i] = spelled
			changed = true
		}
	}
	if !changed {
		return pk
	}
	return strings.Join(parts, "|")
}

// padFixedBinaryFilter re-spells a fixed-width BINARY(n) filter value back to
// the width the baseline stores, returning false when nothing needs re-spelling.
//
// This is the INVERSE of reconstruct.TrimFixedBinaryPad, and the direction is
// deliberate: pk_values holds the binlog ROW image's spelling, which has every
// trailing 0x00 stripped, while the baseline Parquet holds the full n bytes
// MySQL padded on storage. An operator who copies a key out of the index — the
// workflow #1155 reports — therefore hands us a value SHORTER than the one to
// match. Re-padding it is exact (MySQL only ever pads a BINARY(n) with 0x00),
// and it is only ever attempted after an exact lookup already came back empty,
// so it cannot turn a correct hit into a different row.
func padFixedBinaryFilter(pkFilter map[string]string, pkMetas []metadata.ColumnMeta) (map[string]string, bool) {
	out := make(map[string]string, len(pkFilter))
	maps.Copy(out, pkFilter)
	changed := false
	for _, c := range pkMetas {
		if !strings.EqualFold(strings.TrimSpace(c.DataType), "binary") {
			continue
		}
		width := reconstruct.FixedBinaryWidth(c.ColumnType)
		if width == 0 {
			// Pre-#212 snapshot with no COLUMN_TYPE: the pad width is
			// unknowable, so leave the value alone rather than guess.
			continue
		}
		// --pk-columns is operator-typed and MySQL column names are
		// case-insensitive, so an exact-only match would silently skip the
		// retry for `--pk-columns K` against column `k`. (The lookup underneath
		// is case-insensitive on both links since #1155: DuckDB resolves the
		// quoted identifier, and parquetBlobColumns is keyed lowercase.)
		key, ok := filterKeyFor(out, c.Name)
		if !ok {
			continue
		}
		val := out[key]
		raw, isHex := decodeHexPKValue(val)
		if !isHex {
			raw = []byte(val)
		}
		if len(raw) >= width {
			continue
		}
		padded := make([]byte, width)
		copy(padded, raw)
		out[key] = "0x" + strings.ToUpper(hex.EncodeToString(padded))
		changed = true
	}
	return out, changed
}

// filterKeyFor finds the filter entry naming column col, preferring an exact
// match and falling back to a case-insensitive one.
func filterKeyFor(filter map[string]string, col string) (string, bool) {
	if _, ok := filter[col]; ok {
		return col, true
	}
	for k := range filter {
		if strings.EqualFold(k, col) {
			return k, true
		}
	}
	return "", false
}

// decodeHexPKValue decodes the "0x"+hex spelling event.formatPKValue produces
// for non-UTF-8 PK bytes (#1132). A value that is not in that shape is not
// hex-encoded — it is the raw key text — and is reported as such.
func decodeHexPKValue(s string) ([]byte, bool) {
	if len(s) < 4 || len(s)%2 != 0 || !strings.HasPrefix(s, "0x") {
		return nil, false
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		return nil, false
	}
	return b, true
}

var reconstructCmd = &cobra.Command{
	Use:   "reconstruct",
	Short: "Reconstruct the state of a row at a given point in time",
	Long: `Combine a baseline Parquet snapshot with indexed binlog events to reconstruct
the exact state of a row at a target timestamp.

Requires a baseline directory or S3 location produced by "bintrail baseline".
The most recent snapshot at or before --at is automatically selected.

Events are fetched from both live MySQL partitions and any Parquet archives
auto-discovered via archive_state. Pass --no-archive to query MySQL only.
By default, a coverage gap (an hour rotated out of MySQL with no archive)
aborts the reconstruction; pass --allow-gaps to proceed with a warning.

Full-table mode (--output-format mydumper) reconstructs entire tables at a
target point in time and emits a mydumper-compatible dump directory
(schema file + chunked INSERT files + metadata) restorable with a plain
mysql client. Use --tables schema.table,... to select which tables to
reconstruct and --output-dir for the destination.

Examples:
  # Current state of a row (baseline + all binlog events up to now)
  bintrail reconstruct --index-dsn "..." --schema mydb --table orders \
    --pk 12345 --pk-columns id --baseline-dir /data/baselines

  # Full-table point-in-time dump for multiple tables
  bintrail reconstruct --index-dsn "..." \
    --tables mydb.orders,mydb.users --baseline-dir /data/baselines \
    --at "2026-04-01 15:30:00" \
    --output-format mydumper --output-dir ./pitr-dump

  # State at a past timestamp
  bintrail reconstruct --index-dsn "..." --schema mydb --table orders \
    --pk 12345 --pk-columns id --baseline-dir /data/baselines \
    --at "2026-02-15 14:30:00"

  # Full change history (one entry per binlog event)
  bintrail reconstruct --index-dsn "..." --schema mydb --table orders \
    --pk 12345 --pk-columns id --baseline-dir /data/baselines --history

  # Baseline snapshot only — no binlog replay, no --index-dsn needed
  bintrail reconstruct --schema mydb --table orders \
    --pk 12345 --pk-columns id --baseline-dir /data/baselines --baseline-only

  # Free-form DuckDB SQL against a baseline directory
  bintrail reconstruct \
    --sql "SELECT * FROM parquet_scan('/data/baselines/2026-02-28T00-00-00Z/mydb/orders.parquet') LIMIT 10"

  # S3 baseline (uses standard AWS credential chain)
  bintrail reconstruct --index-dsn "..." --schema mydb --table orders \
    --pk 12345 --pk-columns id --baseline-s3 s3://bucket/baselines`,
	RunE: runReconstruct,
}

var (
	recIndexDSN     string
	recSchema       string
	recTable        string
	recPK           string
	recPKColumns    string
	recAt           string
	recBaselineDir  string
	recBaselineS3   string
	recBaselineOnly bool
	recHistory      bool
	recSQL          string
	recFormat       string
	recNoArchive    bool
	recAllowGaps    bool

	// Full-table mydumper output mode (#187).
	recOutputFormat string
	recOutputDir    string
	recTables       string
	recChunkSize    string
	recParallelism  int
	recWarnEvents   int64
	recFetchBatch   int
)

func init() {
	reconstructCmd.Flags().StringVar(&recIndexDSN, "index-dsn", "", "DSN for the index MySQL database (not required with --baseline-only or --sql)")
	reconstructCmd.Flags().StringVar(&recSchema, "schema", "", "Schema (database) name")
	reconstructCmd.Flags().StringVar(&recTable, "table", "", "Table name")
	reconstructCmd.Flags().StringVar(&recPK, "pk", "", "Primary key value(s), pipe-delimited for composite PKs (e.g. 12345 or 12345|2)")
	reconstructCmd.Flags().StringVar(&recPKColumns, "pk-columns", "", "Comma-separated PK column name(s) matching --pk order (e.g. id or order_id,item_id)")
	reconstructCmd.Flags().StringVar(&recAt, "at", "", "Target timestamp for reconstruction (default: now); accepts 2006-01-02 15:04:05 (interpreted as UTC) or RFC3339 (use an explicit offset, e.g. 2006-01-02T15:04:05-05:00, for another zone); 1-second granularity")
	reconstructCmd.Flags().StringVar(&recBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots produced by bintrail baseline")
	reconstructCmd.Flags().StringVar(&recBaselineS3, "baseline-s3", "", "S3 URL prefix of baseline Parquet snapshots (e.g. s3://bucket/baselines/); uses the standard AWS credential chain")
	reconstructCmd.Flags().BoolVar(&recBaselineOnly, "baseline-only", false, "Return the baseline row without applying binlog events (no --index-dsn needed)")
	reconstructCmd.Flags().BoolVar(&recHistory, "history", false, "Return all intermediate states (one entry per binlog event) instead of just the final state")
	reconstructCmd.Flags().StringVar(&recSQL, "sql", "", "Execute arbitrary DuckDB SQL and print results (bypasses --schema/table/pk/at; --baseline-dir/s3 only controls whether the httpfs extension is loaded for S3 access)")
	reconstructCmd.Flags().StringVar(&recFormat, "format", "json", "Output format: json, table, or csv")
	reconstructCmd.Flags().BoolVar(&recNoArchive, "no-archive", false, "Disable auto-routing to Parquet archives (MySQL-only event fetch)")
	reconstructCmd.Flags().BoolVar(&recAllowGaps, "allow-gaps", false, "Proceed even when the event index has missing hours in the baseline-to-target range (may produce incomplete reconstruction)")
	// Full-table mydumper mode (#187).
	reconstructCmd.Flags().StringVar(&recOutputFormat, "output-format", "", "Output format for full-table mode: 'mydumper' to produce a mydumper-compatible dump directory (default: single-row mode)")
	reconstructCmd.Flags().StringVar(&recOutputDir, "output-dir", "", "Output directory for --output-format=mydumper (will be created if missing)")
	reconstructCmd.Flags().StringVar(&recTables, "tables", "", "Comma-separated schema.table list for --output-format=mydumper (e.g. mydb.orders,mydb.users)")
	reconstructCmd.Flags().StringVar(&recChunkSize, "chunk-size", "256MB", "Max size per SQL chunk file in full-table mode (e.g. 64MB, 1GB)")
	reconstructCmd.Flags().IntVar(&recParallelism, "parallelism", 0, "Max tables to reconstruct concurrently in full-table mode (default: runtime.NumCPU())")
	reconstructCmd.Flags().Int64Var(&recWarnEvents, "warn-event-threshold", 5_000_000, "Full-table mode: log a memory warning when a table's reconstruct window exceeds this many events (#654; this threshold is divided by --parallelism, capped to the number of --tables, so it reflects the total concurrent RAM across tables reconstructing at once, #842; 0 disables)")
	reconstructCmd.Flags().IntVar(&recFetchBatch, "fetch-batch-size", 0, "Full-table mode: number of events fetched per page when streaming a table's event window (#1097). 0 uses the built-in default (100000). Lower it to cut peak memory on a small box; raise it to cut archive round trips — with S3 archives an hour's parquet file is re-fetched about (events in that hour / this value) times, and roughly double that counting the preceding hour's file, which pruning keeps, so a value well below your peak hourly event count multiplies downloads")
	AddDuckDBTuningFlags(reconstructCmd)
	BindCommandEnv(reconstructCmd)

}

func runReconstruct(cmd *cobra.Command, args []string) error {
	start := time.Now()

	// ── --output-format mydumper mode: full-table reconstruct (#187) ───────────
	if recOutputFormat != "" {
		if recOutputFormat != "mydumper" {
			return fmt.Errorf("--output-format: only 'mydumper' is supported, got %q", recOutputFormat)
		}
		return runReconstructFullTable(cmd, start)
	}

	// ── --sql mode: execute arbitrary DuckDB SQL ───────────────────────────────
	if recSQL != "" {
		return runReconstructSQL(cmd, start)
	}

	// ── Validate flags ─────────────────────────────────────────────────────────
	if !cliutil.IsValidFormat(recFormat) {
		return fmt.Errorf("invalid --format %q; must be json, table, or csv", recFormat)
	}
	if recSchema == "" {
		return fmt.Errorf("--schema is required")
	}
	if recTable == "" {
		return fmt.Errorf("--table is required")
	}
	if recPK == "" {
		return fmt.Errorf("--pk is required")
	}
	if recPKColumns == "" {
		return fmt.Errorf("--pk-columns is required")
	}
	if recBaselineDir == "" && recBaselineS3 == "" {
		return fmt.Errorf("one of --baseline-dir or --baseline-s3 is required")
	}
	if !recBaselineOnly && recIndexDSN == "" {
		return fmt.Errorf("--index-dsn is required unless --baseline-only is set")
	}
	if recHistory && recBaselineOnly {
		return fmt.Errorf("--history and --baseline-only are mutually exclusive")
	}

	// ── Parse --at ─────────────────────────────────────────────────────────────
	at := time.Now().UTC()
	if recAt != "" {
		parsed, err := cliutil.ParseTime(recAt)
		if err != nil {
			return fmt.Errorf("--at: %w", err)
		}
		if parsed != nil {
			at = *parsed
		}
	}

	// ── Build pkFilter from --pk and --pk-columns ──────────────────────────────
	// Note: --pk uses | as the composite PK separator. Literal | in PK values
	// is not supported (strings.Split cannot honour the \| escaping convention).
	pkCols := strings.Split(recPKColumns, ",")
	pkVals := strings.Split(recPK, "|")
	if len(pkCols) != len(pkVals) {
		return fmt.Errorf("--pk has %d value(s) but --pk-columns has %d column(s); they must match",
			len(pkVals), len(pkCols))
	}
	pkFilter := make(map[string]string, len(pkCols))
	for i, col := range pkCols {
		pkFilter[strings.TrimSpace(col)] = pkVals[i]
	}

	// ── Choose baseline source ─────────────────────────────────────────────────
	baselineSrc := recBaselineDir
	if baselineSrc == "" {
		baselineSrc = recBaselineS3
	}

	// ── Find and read the baseline snapshot ────────────────────────────────────
	// The stale-fallback warning (#466) is already logged inside FindBaseline;
	// the CLI relies on that server-side log.
	baselinePath, snapshotTime, _, err := reconstruct.FindBaseline(cmd.Context(), baselineSrc, recSchema, recTable, at)
	if err != nil {
		return err
	}
	slog.Debug("found baseline snapshot", "path", baselinePath, "snapshot_time", snapshotTime.UTC().Format(time.RFC3339))

	// Read baseline position metadata (binlog file/pos for MySQL, the WAL LSN
	// anchor for PG). ReadParquetMetadataAny reads S3 too — the same reader the
	// full-table path uses (fulltable.go) — so an S3 baseline populates bmeta,
	// which the old local-only read silently skipped: without it, gap detection
	// and the PG beta warn below were disabled for every S3 baseline (#916). A
	// read failure is non-fatal — warn and proceed with a zero bmeta (gap
	// detection then reports "unavailable", the pre-#916 S3 behavior).
	var bmeta baseline.DumpMetadata
	if bm, metaErr := baseline.ReadParquetMetadataAny(cmd.Context(), baselinePath); metaErr != nil {
		slog.Warn("could not read baseline metadata", "error", metaErr)
	} else {
		bmeta = bm
		if bmeta.BinlogFile != "" {
			slog.Debug("baseline binlog position",
				"file", bmeta.BinlogFile, "pos", bmeta.BinlogPos, "gtid", bmeta.GTIDSet)
		}
	}

	baselineRow, err := reconstruct.ReadBaselineRow(cmd.Context(), baselinePath, pkFilter)
	if err != nil {
		return fmt.Errorf("read baseline: %w", err)
	}
	// A nil baselineRow (this PK absent from the snapshot) is NOT resolved here
	// anymore: it flows past the event fetch below so the "no row found" error
	// can be told apart from a PK-changing UPDATE that stored the row under a
	// different (before-image) PK (#782). Baseline-only mode has no events to
	// consult, so it keeps the original error immediately.
	if recBaselineOnly {
		if baselineRow == nil {
			return fmt.Errorf("no row found in baseline %q matching pk filter %v", baselinePath, pkFilter)
		}
		// ── Baseline-only mode ───────────────────────────────────────────────────
		if err := writeReconstructOutput(baselineRow, nil, snapshotTime, at, false, recFormat, os.Stdout); err != nil {
			return err
		}
		auditReconstruct(cmd.Context(), "baseline-only", recSchema, recTable, map[string]string{
			"pk":     recPK,
			"at":     at.UTC().Format(time.RFC3339),
			"format": recFormat,
		})
		slog.Info("reconstruct complete",
			"mode", "baseline-only",
			"snapshot", snapshotTime.UTC().Format(time.RFC3339),
			"duration_ms", time.Since(start).Milliseconds())
		return nil
	}

	// ── Fetch binlog events from live MySQL + archives ────────────────────────
	// Routed through query.FetchMerged so archived events are not silently
	// missed — the single-row path previously called engine.Fetch directly
	// (#209). Strict mode (AllowGaps=false) aborts on any condition that
	// would silently degrade coverage.
	db, err := config.Connect(recIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index database: %w", err)
	}
	defer db.Close()

	// Idempotent schema migration: the query engine SELECTs
	// post-initial-schema binlog_events columns (query_text/query_hash,
	// #699); without this, single-row/--history reconstruct against a
	// pre-upgrade index 1054s while full-table mode (which already calls
	// EnsureSchema in reconstruct.ReconstructTable) self-heals.
	if err := indexer.EnsureSchema(db); err != nil {
		return indexer.WrapSchemaMigrationErr(err)
	}

	// Single-row reconstruct (and the shim's _snapshot) run generically for a
	// PostgreSQL source — there is no PG-specific gate. Scalar types INCLUDING
	// the GUC-sensitive set (timestamptz/timestamp/date/time, float4/float8,
	// bytea, interval) are proven end-to-end through the baseline↔delta fold
	// under rendering GUCs pinned identically on the baseline COPY and
	// walsender sessions (#593 slice D, pgcapture/gucpin.go,
	// TestOne_PGTypeMatrixThroughReconstructFold); the residual beta surface
	// is container types only. Warn — never refuse (the path is honest-best-
	// effort and a refusal would break the surface that DOES work). Detect PG
	// by the recorded flavor OR the baseline's LSN anchor (read from S3 too
	// since #916). Both clauses are load-bearing: the LSN clause catches any
	// PG baseline carrying an anchor (post-#593, local or S3); the flavor
	// clause catches a pre-#593 PG baseline with LSN==0 (no anchor) and
	// backstops a baseline whose metadata read failed. --baseline-only returns
	// above, before the DB is ever opened, so it never reaches these warns.
	if pgReconstructBeta(query.SourceFlavor(db), bmeta.LSN) {
		slog.Warn("single-row reconstruct for a PostgreSQL source: scalar types (including timestamptz/timestamp/date/time, float, bytea, interval — rendered under session GUCs pinned identically at capture and baseline) are validated end-to-end; container types (arrays beyond integer[]/text[], composite, range/multirange, hstore, geometric) remain beta — verify the round-trip before relying on them (#593)")
		// A PG baseline whose rendering-GUC stamp is absent (pre-pin, rendered
		// under the server's session defaults) OR different from the current
		// pin was rendered under other GUCs; on a server whose defaults differ
		// from the pin, its GUC-sensitive text will not join post-pin deltas —
		// the merge is an exact text join. Warn with the remediation.
		// (Comparing the VALUE, not mere presence, also catches a baseline
		// produced by a binary with a different pinned set.)
		if bmeta.LSN != 0 && bmeta.RenderGUCs != baseline.RenderGUCsPinned {
			slog.Warn("this baseline's rendering-GUC stamp does not match the current pin (#593) — it predates GUC pinning or was produced under a different pin; on a server whose TimeZone/DateStyle/extra_float_digits/bytea_output/IntervalStyle defaults differ from the pinned values, its GUC-sensitive text will not match newer deltas; re-run `bintrail-pg baseline` to refresh it",
				"baseline_render_gucs", bmeta.RenderGUCs)
		}
	}

	// Refuse if a TRUNCATE/DROP/RENAME hit this table in the window: it emits
	// no row events, so folding the fetched deltas onto the baseline below
	// would silently resolve a truncated-away row as if it still existed at
	// --at (#764; same guard as the full-table path and the shim's
	// _snapshot).
	if err := reconstruct.CheckDestructiveDDL(cmd.Context(), db, recSchema, recTable, snapshotTime, at); err != nil {
		return err
	}

	engine := query.New(db)

	// Refuse/warn on a stamped capture gap inside the window (#765): a
	// query.GapError below only catches archive-coverage gaps (rotated hours
	// with no archive); stream_state.gap_lost_at records events permanently
	// lost at the source, which no archive can fill.
	if err := reconstruct.CheckCaptureGap(cmd.Context(), db, recSchema, recTable, snapshotTime, at, recAllowGaps); err != nil {
		return err
	}

	// The planner needs a database name derived from the DSN.
	var dbName string
	if cfg, parseErr := mysqldriver.ParseDSN(recIndexDSN); parseErr != nil {
		slog.Warn("could not parse DSN for query planning", "error", parseErr)
	} else {
		dbName = cfg.DBName
	}

	// PK column metadata from the schema snapshot, resolved before BOTH lookups
	// that need it: the event fetch just below (which matches
	// binlog_events.pk_values) and the baseline retry further down (which
	// matches the Parquet column). The two want the key spelled DIFFERENTLY —
	// see indexPKSpelling and padFixedBinaryFilter (#1155).
	pkMetas := resolvePKMetas(db, recSchema, recTable)

	opts := query.Options{
		Schema: recSchema,
		Table:  recTable,
		// A fixed BINARY(n) key is stored in pk_values with its trailing 0x00
		// padding stripped and its hex uppercased, so the full-width or
		// lowercase spelling an operator can legitimately produce
		// (`SELECT CONCAT('0x', HEX(k))`) matches NO event. Left uncorrected
		// that is silent and wrong rather than loud: the baseline lookup above
		// resolves such a key, the fetch returns zero events, and ApplyAt then
		// renders baseline-era state as the state at --at.
		PKValues: indexPKSpelling(recPK, pkMetas),
		Since:    &snapshotTime,
		Until:    &at,
	}
	duckTuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}
	// FetchEventsAtomic (not a plain query.FetchMerged) cuts the `--at` upper
	// bound at the transaction boundary, not the row: a multi-statement
	// transaction straddling `at` is excluded whole rather than half-applied
	// (#783).
	events, _, err := reconstruct.FetchEventsAtomic(cmd.Context(), db, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         dbName,
		NoArchive:      recNoArchive,
		AllowGaps:      recAllowGaps,
		ArchiveFetcher: TunedArchiveFetcher(duckTuning),
	}, at)
	if err != nil {
		// Surface CLI hints only at the CLI layer; the library types
		// (query.GapError, query.SourceEmptyError) stay command-neutral.
		var gapErr *query.GapError
		if errors.As(err, &gapErr) {
			return fmt.Errorf("%w; pass --allow-gaps to proceed with an incomplete reconstruction", err)
		}
		var emptyErr *query.SourceEmptyError
		if errors.As(err, &emptyErr) {
			return fmt.Errorf("%w; run `bintrail archive reconcile` to re-sync archive_state with storage, or pass --allow-gaps to proceed without that source", err)
		}
		return fmt.Errorf("fetch binlog events: %w", err)
	}
	slog.Debug("fetched binlog events", "count", len(events))

	// A fixed BINARY(n) key copied out of binlog_events.pk_values carries the
	// ROW image's trailing-0x00-stripped spelling, which is SHORTER than the
	// padded value the baseline stores. Retry once at the storage width — only
	// after the exact lookup came back empty, so a hit is never overridden.
	if baselineRow == nil {
		if padded, ok := padFixedBinaryFilter(pkFilter, pkMetas); ok {
			slog.Debug("retrying baseline lookup with the fixed BINARY(n) storage padding", "pk_filter", padded)
			baselineRow, err = reconstruct.ReadBaselineRow(cmd.Context(), baselinePath, padded)
			if err != nil {
				return fmt.Errorf("read baseline: %w", err)
			}
		}
	}

	// Still no baseline row for this PK. Refuse — but tell a genuinely-absent
	// row apart from one whose existence traces to a PK-changing UPDATE the
	// change map can't fold (#782): such an UPDATE is stored under its
	// BEFORE-image PK, so a fetch by the (new) searched PK never retrieves it
	// and the row looks absent. The earliest fetched event for the PK is the
	// tell (see pkChangeSuspected).
	if baselineRow == nil {
		// The PK-changing-UPDATE explanation below presumes the lookup itself
		// was capable of resolving this key. When the PK type is outside the
		// baseline canonicalizer's set, the lookup could never have matched, so
		// blaming a schema event that may never have happened sends the
		// operator after a remedy (re-run `bintrail baseline`) that cannot
		// help. Report the same reason `verify` reports (#1155).
		if c := unsupportedPKType(pkMetas); c != nil {
			return fmt.Errorf(
				"reconstruct: no baseline row for %s.%s pk %q — primary-key column %q has type %q unsupported by the baseline canonicalizer, "+
					"so this row cannot be located in the snapshot regardless of whether it exists",
				recSchema, recTable, recPK, c.Name, c.DataType)
		}
		if pkChangeSuspected(events) {
			return fmt.Errorf(
				"reconstruct: no baseline row for %s.%s pk %q, yet the earliest indexed event for it is not an INSERT — "+
					"a PK-changing UPDATE in the window likely brought this PK into existence under a different before-image key. "+
					"reconstruct folds events by the before-image primary key and cannot follow an UPDATE into its new key, so this "+
					"row cannot be resolved. Re-run `bintrail baseline` to capture a snapshot at or after the PK change, then reconstruct from there",
				recSchema, recTable, recPK)
		}
		return fmt.Errorf("no row found in baseline %q matching pk filter %v", baselinePath, pkFilter)
	}

	// ENUM/SET ordinals → labels (#476), each delta decoded with the
	// snapshot in effect at its event time (#475). No latest-resolver
	// fallback here: if the epoch lookup fails, ordinals pass through
	// raw — the pre-#476 CLI output.
	reconstruct.MapEventEnumLabels(db, nil, recSchema, recTable, events)
	// BLOB/TEXT columns are stored base64-encoded; decode them on the deltas
	// before the ApplyAt/BuildHistory fold so the output carries the real value,
	// not its base64 text (#666). Baseline rows are read raw and untouched.
	reconstruct.DecodeEventBinaries(db, recSchema, recTable, events)

	// Warn if there is a gap between the baseline position and the first indexed
	// event — events in that gap are missing from the reconstruction. The
	// comparable anchor is flavor-dependent (#593): PostgreSQL baselines carry
	// the numeric WAL LSN delta-replay floor (baseline.MetaKeyLSN — an
	// INCLUSIVE lower bound, corrected by #771 to be the replication slot's
	// own confirmed_flush_lsn/restart_lsn rather than the snapshot's live
	// pg_current_wal_lsn(); see MetaKeyLSN's doc comment); MySQL/MariaDB on
	// binlog file+pos. PG LSN TEXT ("0/1A2B3C4") is NOT lexically ordered, so
	// the binlog_file column must never be compared for a PG source — see
	// reconstruct.GapDetected and resolveGapCheck. NOTE: single-row reconstruct
	// (and the shim's _snapshot) DO run for a PG source today — a beta warn
	// fired above; full-table reconstruct stays gated for PG (#830/#597). The
	// remaining slice-D GA hardening is end-to-end type-fidelity validation and
	// pinning the baseline COPY vs pgoutput rendering GUCs so their text agrees;
	// it must honor "replay at/after the floor", not "strictly after the
	// snapshot's live LSN".
	if len(events) > 0 {
		first := events[0]
		flavor, lineageGuard, anchorPresent, eventPosMissing := resolveGapCheck(
			query.SourceFlavor(db), bmeta, first.BinlogFile, first.StartPos)
		if lineageGuard {
			slog.Warn("source flavor unknown but baseline carries an LSN anchor — treating source as postgres for gap detection (LSN text is never compared lexically)",
				"baseline_lsn", bmeta.LSN)
		}
		switch {
		case !anchorPresent && flavor == "postgres":
			// The permanent steady state for every PG baseline until the
			// LSN-writing producer ships (#593 slice C): the reconstruction may
			// contain an undetectable hole, which deserves the same visibility
			// as the #318 missing-event-position warn below — not an INFO line
			// invisible at --log-level warn. No remediation command exists yet
			// ("bintrail baseline" is the MySQL/mydumper pipeline), so the
			// message must not recommend one.
			slog.Warn("gap detection unavailable — this baseline predates LSN anchoring (no bintrail.baseline_lsn metadata); a gap between the baseline and the first indexed event would go undetected",
				"flavor", flavor)
		case !anchorPresent:
			slog.Info("gap detection skipped — baseline lacks position metadata; consider re-running 'bintrail baseline' to embed position data",
				"flavor", flavor)
		case eventPosMissing:
			// A first event with no comparable position — NULL binlog_file
			// (MySQL) or a zero LSN (PostgreSQL; 0 is not a valid WAL position)
			// — skips the gap check rather than silently degrade to "no gap".
			// See dbtrail/bintrail#318.
			slog.Warn("gap detection skipped — first indexed event lacks position metadata",
				"event_id", first.EventID,
				"baseline_file", bmeta.BinlogFile,
				"baseline_pos", bmeta.BinlogPos,
				"baseline_lsn", bmeta.LSN,
				"flavor", flavor)
		case reconstruct.GapDetected(flavor, first.BinlogFile, first.StartPos, bmeta.BinlogFile, bmeta.BinlogPos, bmeta.LSN):
			slog.Warn("gap between baseline and first indexed event — reconstruction may be incomplete",
				"baseline_file", bmeta.BinlogFile,
				"baseline_pos", bmeta.BinlogPos,
				"baseline_gtid", bmeta.GTIDSet,
				"baseline_lsn", bmeta.LSN,
				"first_event_file", first.BinlogFile,
				"first_event_pos", first.StartPos,
				"flavor", flavor)
		}
	}

	// ── Reconstruct and format output ──────────────────────────────────────────
	if err := writeReconstructOutput(baselineRow, events, snapshotTime, at, recHistory, recFormat, os.Stdout); err != nil {
		return err
	}

	mode := "row"
	if recHistory {
		mode = "history"
	}
	auditReconstruct(cmd.Context(), mode, recSchema, recTable, map[string]string{
		"pk":     recPK,
		"at":     at.UTC().Format(time.RFC3339),
		"events": strconv.Itoa(len(events)),
		"format": recFormat,
	})

	slog.Info("reconstruct complete",
		"schema", recSchema, "table", recTable, "pk", recPK,
		"at", at.UTC().Format(time.RFC3339),
		"snapshot", snapshotTime.UTC().Format(time.RFC3339),
		"events_applied", len(events),
		"duration_ms", time.Since(start).Milliseconds())
	return nil
}

// runReconstructSQL handles the --sql mode.
func runReconstructSQL(cmd *cobra.Command, start time.Time) error {
	if !cliutil.IsValidFormat(recFormat) {
		return fmt.Errorf("invalid --format %q; must be json, table, or csv", recFormat)
	}
	source := recBaselineDir
	if source == "" {
		source = recBaselineS3
	}
	results, cols, err := reconstruct.ExecSQL(cmd.Context(), source, recSQL)
	if err != nil {
		return err
	}
	switch recFormat {
	case "json":
		if err := reconstruct.WriteSQLResultsJSON(results, os.Stdout); err != nil {
			return err
		}
	case "csv":
		if err := reconstruct.WriteSQLResultsCSV(results, cols, os.Stdout); err != nil {
			return err
		}
	default:
		if err := reconstruct.WriteSQLResultsTable(results, cols, os.Stdout); err != nil {
			return err
		}
	}
	auditReconstruct(cmd.Context(), "sql", recSchema, recTable, map[string]string{
		"rows":   strconv.Itoa(len(results)),
		"format": recFormat,
	})

	slog.Info("reconstruct SQL complete",
		"rows", len(results),
		"duration_ms", time.Since(start).Milliseconds())
	return nil
}

// writeReconstructOutput formats the reconstructed state (or history) to w.
func writeReconstructOutput(baselineRow map[string]any, events []query.ResultRow, snapshotTime, at time.Time, history bool, format string, w io.Writer) error {
	if history {
		entries, err := reconstruct.BuildHistory(baselineRow, snapshotTime, events, at)
		if err != nil {
			return err
		}
		switch format {
		case "json":
			return reconstruct.WriteHistoryJSON(entries, w)
		case "csv":
			return reconstruct.WriteHistoryCSV(entries, w)
		default:
			return reconstruct.WriteHistoryTable(entries, w)
		}
	}
	state, err := reconstruct.ApplyAt(baselineRow, events, at)
	if err != nil {
		return err
	}
	switch format {
	case "json":
		return reconstruct.WriteStateJSON(state, w)
	case "csv":
		return reconstruct.WriteStateCSV(state, w)
	default:
		return reconstruct.WriteStateTable(state, w)
	}
}

// runReconstructFullTable handles --output-format mydumper. It validates
// flag combinations, builds a FullTableConfig, invokes
// reconstruct.ReconstructTables, and prints a one-line summary per table.
func runReconstructFullTable(cmd *cobra.Command, start time.Time) error {
	// ── Validate incompatible flags ────────────────────────────────────────
	if recPK != "" || recPKColumns != "" {
		return fmt.Errorf("--output-format=mydumper is incompatible with --pk / --pk-columns (full-table mode reconstructs every row)")
	}
	if recHistory {
		return fmt.Errorf("--output-format=mydumper is incompatible with --history")
	}
	if recBaselineOnly {
		return fmt.Errorf("--output-format=mydumper is incompatible with --baseline-only")
	}
	if recSQL != "" {
		return fmt.Errorf("--output-format=mydumper is incompatible with --sql")
	}
	if recSchema != "" || recTable != "" {
		return fmt.Errorf("--output-format=mydumper uses --tables for schema.table selection, not --schema/--table")
	}

	// ── Validate required flags ────────────────────────────────────────────
	if recTables == "" {
		return fmt.Errorf("--tables is required with --output-format=mydumper (e.g. --tables mydb.orders,mydb.users)")
	}
	if recOutputDir == "" {
		return fmt.Errorf("--output-dir is required with --output-format=mydumper")
	}
	if recIndexDSN == "" {
		return fmt.Errorf("--index-dsn is required with --output-format=mydumper")
	}
	if recBaselineDir == "" && recBaselineS3 == "" {
		return fmt.Errorf("one of --baseline-dir or --baseline-s3 is required with --output-format=mydumper")
	}

	// ── Parse --at ─────────────────────────────────────────────────────────
	at := time.Now().UTC()
	if recAt != "" {
		parsed, err := cliutil.ParseTime(recAt)
		if err != nil {
			return fmt.Errorf("--at: %w", err)
		}
		if parsed != nil {
			at = *parsed
		}
	}

	// ── Parse --chunk-size ─────────────────────────────────────────────────
	chunkSize, err := cliutil.ParseByteSize(recChunkSize)
	if err != nil {
		return fmt.Errorf("--chunk-size: %w", err)
	}
	if recFetchBatch < 0 {
		return fmt.Errorf("--fetch-batch-size must be >= 0 (0 uses the default)")
	}
	if recWarnEvents < 0 {
		return fmt.Errorf("--warn-event-threshold must be >= 0 (0 disables)")
	}

	// ── Parse --tables (comma-separated schema.table list) ────────────────
	tables := splitAndTrim(recTables, ",")
	if len(tables) == 0 {
		return fmt.Errorf("--tables: no entries after trimming")
	}
	for _, entry := range tables {
		if !strings.Contains(entry, ".") {
			return fmt.Errorf("--tables entry %q must be schema.table", entry)
		}
	}

	// ── Pick baseline source (local dir takes precedence) ─────────────────
	baselineSrc := recBaselineDir
	if baselineSrc == "" {
		baselineSrc = recBaselineS3
	}

	duckTuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}

	// ── Run ────────────────────────────────────────────────────────────────
	cfg := reconstruct.FullTableConfig{
		IndexDSN:           recIndexDSN,
		BaselineSrc:        baselineSrc,
		Tables:             tables,
		At:                 at,
		OutputDir:          recOutputDir,
		ChunkSize:          chunkSize,
		Parallelism:        recParallelism,
		AllowGaps:          recAllowGaps,
		WarnEventThreshold: recWarnEvents,
		FetchBatchSize:     recFetchBatch,
		ArchiveFetcher:     TunedArchiveFetcher(duckTuning),
		// Same resolved --ultrafast/--duckdb-* budget as ArchiveFetcher above,
		// but for the merge/baseline DuckDB sessions ReconstructTables opens
		// directly (#842) — those previously ignored these flags entirely and
		// defaulted to ~80% of host RAM regardless of what the operator set.
		DuckDBTuning: duckTuning,
	}
	reports, err := reconstruct.ReconstructTables(cmd.Context(), cfg)
	if err != nil {
		// Same CLI-layer hint as single-row reconstruct (the %w chain
		// survives ReconstructTables' errors.Join, so errors.As still
		// unwraps the library type).
		var emptyErr *query.SourceEmptyError
		if errors.As(err, &emptyErr) {
			return fmt.Errorf("full-table reconstruct: %w; run `bintrail archive reconcile` to re-sync archive_state with storage, or pass --allow-gaps to proceed without that source", err)
		}
		return fmt.Errorf("full-table reconstruct: %w", err)
	}

	// ── Summary ────────────────────────────────────────────────────────────
	var totalRows, totalEvents int64
	for _, rep := range reports {
		totalRows += rep.BaselineRows + rep.UpdatesApplied + rep.InsertsEmitted
		totalEvents += rep.EventsApplied
		slog.Info("table dump complete",
			"schema", rep.Schema, "table", rep.Table,
			"baseline_rows", rep.BaselineRows,
			"updates_applied", rep.UpdatesApplied,
			"inserts_emitted", rep.InsertsEmitted,
			"deletes_skipped", rep.DeletesSkipped,
			"events_applied", rep.EventsApplied,
			"files", len(rep.Files),
			"duration_ms", rep.Duration.Milliseconds())
	}
	// One event per reconstructed TABLE, not one per run: the audit trail's
	// unit is "who read which table's historical rows", and a --tables run
	// materializes each of them in full.
	for _, rep := range reports {
		auditReconstruct(cmd.Context(), "full-table", rep.Schema, rep.Table, map[string]string{
			"at":         recAt,
			"rows":       strconv.FormatInt(rep.BaselineRows+rep.UpdatesApplied+rep.InsertsEmitted, 10),
			"events":     strconv.FormatInt(rep.EventsApplied, 10),
			"output_dir": recOutputDir,
		})
	}

	slog.Info("full-table reconstruct complete",
		"tables", len(reports),
		"total_rows", totalRows,
		"total_events_applied", totalEvents,
		"output_dir", recOutputDir,
		"duration_ms", time.Since(start).Milliseconds())
	return nil
}

// auditReconstruct reports a completed point-in-time reconstruction to the
// audit seam. reconstruct reads a baseline snapshot's row images and folds
// binlog deltas onto them — historical data access in the same class as
// query/shim time-travel, which is exactly what ext/audit.go's contract
// names. ext.Record is a no-op unless an embedding distribution installed a
// sink, and it cannot fail the command (see ext/audit.go).
//
// mode distinguishes the five shapes the one command serves: "row"
// (single-row AS OF), "history" (--history), "baseline-only"
// (--baseline-only: the raw baseline row, no deltas applied — it prints a
// row image straight out of the Parquet snapshot, so it is audited like
// the others; it was the unaudited fifth mode of #1123), "full-table"
// (--output-format mydumper) and "sql" (--sql, a direct read of the
// baseline Parquet).
// reconstruct has no --profile flag, so the actor carries no RBAC profile.
func auditReconstruct(ctx context.Context, mode, schema, table string, detail map[string]string) {
	if detail == nil {
		detail = map[string]string{}
	}
	detail["mode"] = mode
	ext.Record(ctx, ext.AuditEvent{
		Surface: "cli",
		Action:  "reconstruct.run",
		Actor:   ext.ProcessActor(""),
		Schema:  schema,
		Table:   table,
		Detail:  detail,
	})
}

// splitAndTrim splits s on sep and strips whitespace from each entry,
// dropping empty results.
func splitAndTrim(s, sep string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// resolveGapCheck computes the flavor-dependent decisions of runReconstruct's
// baseline↔first-event gap check. effectiveFlavor is what
// reconstruct.GapDetected must be called with; lineageGuard reports that the
// stream_state flavor read came back empty while the baseline carries an LSN
// anchor — the baseline itself proves a PostgreSQL lineage, so PG semantics
// are forced rather than ever falling through to the MySQL lexical compare on
// LSN text (that path being unreachable today only because PG baselines leave
// BinlogFile empty is a convention, not an invariant — this guard makes it
// structural, #593). anchorPresent reports whether the baseline has a
// comparable anchor for its flavor (binlog file vs LSN); eventPosMissing
// whether the first event lacks one (NULL binlog_file for MySQL; a zero
// StartPos for PG — 0 is not a valid WAL position).
func resolveGapCheck(flavor string, bmeta baseline.DumpMetadata, firstFile string, firstStartPos uint64) (effectiveFlavor string, lineageGuard, anchorPresent, eventPosMissing bool) {
	if flavor == "" && bmeta.LSN != 0 {
		flavor = "postgres"
		lineageGuard = true
	}
	anchorPresent = bmeta.BinlogFile != ""
	eventPosMissing = firstFile == ""
	if flavor == "postgres" {
		anchorPresent = bmeta.LSN != 0
		eventPosMissing = firstStartPos == 0
	}
	return flavor, lineageGuard, anchorPresent, eventPosMissing
}

// pgReconstructBeta reports whether a single-row reconstruct is running against
// a PostgreSQL source — gate for the beta warning. Either the index records the
// source flavor as "postgres", or the baseline carries an LSN anchor. Both
// clauses are load-bearing: the LSN clause catches any PG baseline carrying an
// anchor (post-#593, local or S3 — metadata is read from S3 too since #916); the
// flavor clause catches a pre-#593 PG baseline with LSN==0 (no anchor) and
// backstops a baseline whose metadata read failed.
func pgReconstructBeta(flavor string, baselineLSN uint64) bool {
	return flavor == "postgres" || baselineLSN != 0
}
