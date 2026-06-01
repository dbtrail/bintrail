package shim

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/query"
	"github.com/dbtrail/bintrail/internal/reconstruct"
)

// runSnapshot resolves a _snapshot query. _snapshot is the
// baseline-aware sibling of _flashback (#355): on top of the
// binlog-only point-in-time logic, it seeds the row state from a
// `bintrail baseline` Parquet snapshot so a row that existed at q.AsOf
// but was never touched within the retained binlog window still
// resolves. _flashback (runPointInTime) deliberately stays binlog-only
// so the two virtual schemas have distinct, documented semantics.
//
// Full-table _snapshot (q.PKColumn == "") merges the baseline snapshot with
// post-snapshot binlog deltas across the whole table (#362), so a never-touched
// row appears in the resultset; single-row _snapshot (#355) does the same for
// one PK. Both degrade to the binlog-only path when no baseline is usable.
func (h *Handler) runSnapshot(q TimeTravelQuery) (*mysql.Result, error) {
	if q.PKColumn == "" {
		return h.runSnapshotFullTable(q)
	}
	return h.runSnapshotPointInTime(q)
}

// baselineSource returns the configured baseline location, preferring
// the S3 prefix over the local directory when both are set. Empty means
// no baseline source is configured — _snapshot then degrades to the
// binlog-only _flashback behaviour.
func (h *Handler) baselineSource() string {
	if h.cfg.BaselineS3 != "" {
		return h.cfg.BaselineS3
	}
	return h.cfg.BaselineDir
}

// errFullTableCapExceeded short-circuits the baseline merge once the buffered
// resultset would exceed the row cap. It never escapes runSnapshotFullTable —
// it is converted to ER_TOO_BIG_SELECT, mirroring runFullTable's cap path.
var errFullTableCapExceeded = errors.New("full-table snapshot row cap exceeded")

// runSnapshotFullTable resolves a full-table (no-WHERE) _snapshot query by
// merging the baseline snapshot at-or-before AsOf with the post-snapshot
// binlog deltas across the whole table (#362). The result is the table's true
// row state at AsOf: never-touched baseline rows pass through, rows updated
// after the baseline take their latest event image, rows deleted after the
// baseline drop out, and rows inserted after the baseline appear — exactly what
// the offline `bintrail reconstruct` produces, but streamed into an in-memory
// resultset.
//
// It falls back to the binlog-only full-table path (runFullTable) — preserving
// the documented "_snapshot degrades to _flashback" contract so a full-table
// _snapshot never fails where _flashback would succeed — whenever a baseline
// merge isn't possible:
//   - no baseline source configured,
//   - the table's PK can't be resolved from the schema snapshot, or it has no
//     single/declared PK to canonicalize against,
//   - a PK column's type isn't supported by the baseline canonicalizer,
//   - no baseline exists for this table at-or-before AsOf.
//
// Real faults (baseline source unreadable, fetch failure, a PK value the
// canonicalizer can't translate) surface as errors, the same user-vs-server
// split the single-row path preserves.
func (h *Handler) runSnapshotFullTable(q TimeTravelQuery) (*mysql.Result, error) {
	src := h.baselineSource()
	if src == "" {
		h.logger.Debug("shim: full-table _snapshot has no baseline source configured; using binlog-only path",
			"schema", q.Schema, "table", q.Table)
		return h.runFullTable(q)
	}

	pkCols, ok := h.pkColumnMetas(q.Schema, q.Table)
	if !ok || len(pkCols) == 0 {
		// A baseline source IS configured, so the operator opted into
		// full-table completeness — but we can't determine the table's PK to
		// canonicalize baseline rows against, so the result silently loses
		// every never-touched baseline row. Warn (not Debug) so the
		// degradation is visible; the usual cause is a stale/absent schema
		// snapshot, fixable with `bintrail snapshot`.
		h.logger.Warn("shim: full-table _snapshot cannot resolve a PK for the table; degrading to binlog-only (never-touched rows omitted)",
			"schema", q.Schema, "table", q.Table)
		return h.runFullTable(q)
	}
	for _, c := range pkCols {
		if !reconstruct.SupportedPKType(c.DataType) {
			h.logger.Warn("shim: full-table _snapshot PK type not supported by the baseline canonicalizer; using binlog-only path",
				"schema", q.Schema, "table", q.Table, "pk_column", c.Name, "pk_type", c.DataType)
			return h.runFullTable(q)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baselinePath, snapshotTime, err := reconstruct.FindBaseline(ctx, src, q.Schema, q.Table, q.AsOf)
	if err != nil {
		if errors.Is(err, reconstruct.ErrNoBaseline) {
			// Source configured but no baseline exists for this table at-or-
			// before AsOf (not yet baselined, or table created after the last
			// snapshot). The full-table result then silently omits any
			// never-touched row — Warn so the operator can tell the snapshot
			// degraded to binlog-only rather than returning a complete table.
			h.logger.Warn("shim: full-table _snapshot found no baseline at-or-before AsOf; degrading to binlog-only (never-touched rows omitted)",
				"schema", q.Schema, "table", q.Table,
				"as_of", q.AsOf.UTC().Format(time.RFC3339))
			return h.runFullTable(q)
		}
		// A real baseline-source failure is a server-side fault, same as the
		// single-row path: plain error → ER_UNKNOWN_ERROR (1105).
		return nil, err
	}

	// Fetch the latest event per PK from the snapshot instant up to AsOf.
	// LimitPerPK=1 yields exactly the change map the merge needs (last write
	// wins per PK); Since=snapshotTime drops pre-baseline events the baseline
	// already supersedes. No global Limit here — the cap is enforced on the
	// merged output below, since the table can have far more baseline rows
	// than changed rows.
	engine := query.New(h.indexDB)
	rows, _, err := query.FetchMerged(ctx, h.indexDB, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema:     q.Schema,
			Table:      q.Table,
			Since:      &snapshotTime,
			Until:      &q.AsOf,
			LimitPerPK: 1,
		},
		DBName:         h.cfg.IndexDBName,
		NoArchive:      h.cfg.NoArchive,
		AllowGaps:      h.cfg.AllowGaps,
		ArchiveFetcher: h.archiveFetcher,
	})
	if err != nil {
		return nil, wrapFetchError(q.Type, err)
	}
	changes := make(map[string]*query.ResultRow, len(rows))
	for i := range rows {
		changes[rows[i].PKValues] = &rows[i]
	}

	rowCap := h.cfg.FullTableRowCap
	if rowCap <= 0 {
		rowCap = defaultFullTableRowCap
	}

	// Buffer the merged rows, coercing every value to a uniform text cell
	// (see fullTableTextCell). This is required, not cosmetic: a baseline row
	// carries DuckDB-native types (an int column is int32 → LONGLONG) while a
	// post-baseline event image is JSON-decoded (the same column is float64 →
	// DOUBLE), and BuildSimpleTextResultset rejects a column whose non-NULL
	// rows disagree on type ("row types aren't consistent"). Rendering every
	// cell to its text bytes makes each column uniformly VAR_STRING, lossless
	// for large integers (unlike the event path's float64), and identical on
	// the wire to per-value formatting. Stop at cap+1 so overflow is
	// detectable.
	images := make([]map[string]any, 0)
	err = reconstruct.SnapshotFullTableImages(ctx, reconstruct.SnapshotFullTableInput{
		BaselinePath: baselinePath,
		Schema:       q.Schema,
		Table:        q.Table,
		PKCols:       pkCols,
		Changes:      changes,
	}, func(rowMap map[string]any) error {
		img := make(map[string]any, len(rowMap))
		for k, v := range rowMap {
			img[k] = h.fullTableTextCell(q.Schema, q.Table, k, v)
		}
		images = append(images, img)
		if len(images) > rowCap {
			return errFullTableCapExceeded
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errFullTableCapExceeded) {
			return nil, mysql.NewError(mysql.ER_TOO_BIG_SELECT, fmt.Sprintf(
				"resolve %s: %s.%s at %s would return more than %d rows; narrow the AS OF range or filter by PK",
				q.Type, q.Schema, q.Table, q.AsOf.Format("2006-01-02 15:04:05"), rowCap,
			))
		}
		return nil, err
	}

	return imagesToResult(images, h.effectiveColumnOrder(q))
}

// pkColumnMetas returns the primary-key column metas of schema.table from the
// latest schema snapshot, for canonicalizing baseline PK values. The bool is
// false when the resolver is unavailable or the table isn't in the snapshot —
// callers treat that as "can't safely attempt a baseline merge" and fall back
// to the binlog-only path.
func (h *Handler) pkColumnMetas(schema, table string) ([]metadata.ColumnMeta, bool) {
	if h.resolverFn == nil {
		return nil, false
	}
	r, err := h.resolverCache.get(time.Now, resolverCacheTTL, h.resolverFn, h.logger)
	if err != nil {
		return nil, false
	}
	tm, err := r.Resolve(schema, table)
	if err != nil {
		return nil, false
	}
	return tm.PKColumnMetas(), true
}

// fullTableTextCell renders one merged-resultset value to a uniform text cell
// so a column's type is consistent across baseline-origin and event-origin
// rows (see runSnapshotFullTable). NULL stays nil; everything else becomes
// []byte:
//   - []byte (string/JSON/blob columns from DuckDB or event images) passes
//     through verbatim, matching the single-row _snapshot path. bintrail
//     stores DECIMAL/NUMERIC as a Parquet string, so those arrive here as
//     []byte too — not DuckDB's native decimal type;
//   - time.Time (DuckDB datetime/timestamp/date scan output) is formatted UTC,
//     since bintrail stores temporal values UTC-anchored and the full-table
//     merge does not pin the DuckDB session;
//   - numerics and other scalars go through go-mysql's text formatter so the
//     bytes match what the wire protocol emits.
//
// Every type the bintrail baseline schema can produce (internal/baseline/
// schema.go) is covered by one of the branches above, so the final %v
// fallback should be unreachable. If a value ever does reach it the cell
// would be best-effort-formatted rather than aborting the whole resultset —
// but that is a silent degradation, so we Warn first to make it detectable.
func (h *Handler) fullTableTextCell(schema, table, column string, v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return x
	case string:
		return []byte(x)
	case time.Time:
		return []byte(x.UTC().Format("2006-01-02 15:04:05"))
	default:
		if b, err := mysql.FormatTextValue(v); err == nil {
			return b
		}
		h.logger.Warn("shim: full-table _snapshot cell has an unexpected Go type; best-effort formatting (value may render incorrectly)",
			"schema", schema, "table", table, "column", column, "go_type", fmt.Sprintf("%T", v))
		return []byte(fmt.Sprintf("%v", v))
	}
}

// baselinePKStringMatchable reports whether a PK column of the given
// MySQL DATA_TYPE can be matched in the baseline Parquet by binding the
// PK value as a string parameter to `pkColumn = ?` (what ReadBaselineRow
// does). The single-row path does NOT canonicalize the value, so it
// relies on DuckDB coercing the string-bound parameter to the typed
// Parquet column:
//
//   - integer (int/year → INT32/64) and string (decimal/numeric,
//     char/varchar/text, enum/set → STRING): DuckDB coerces a string
//     literal to an integer column, and a string column compares
//     byte-for-byte against the same string the indexer stored, so the
//     match round-trips.
//   - datetime/timestamp (Parquet TIMESTAMP): safe now that ReadBaselineRow
//     pins the DuckDB session to UTC (#359). The stored micros are
//     UTC-anchored, so with a UTC session the `temporal_col = 'literal'` cast
//     resolves the bound string to the same instant the indexer recorded, on
//     any host TZ. Before the pin these silently missed on non-UTC hosts and
//     were excluded here.
//   - date (Parquet DATE): always TZ-independent — DuckDB's string→DATE cast
//     is calendar-only, with no timezone component, so `date_col = '2020-01-01'`
//     matches regardless of session TimeZone. DATE was never broken on non-UTC
//     hosts; its prior exclusion was over-conservative, and the #359 UTC pin is
//     a harmless no-op for it.
//
// This set is intentionally identical to reconstruct.supportedPKType, but
// reached by a different mechanism (DuckDB string-cast here vs. Go-side
// canonicalization in the offline merge), so it is kept as a separate
// matcher: a future change to one path's type support must not silently
// move the other. Types reconstruct rejects outright (FLOAT, BLOB, BIT,
// JSON, …) are not listed and therefore fall back to the binlog-only path.
func baselinePKStringMatchable(dataType string) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "int", "integer", "smallint", "tinyint", "mediumint", "bigint",
		"year",
		"decimal", "numeric",
		"char", "varchar", "text", "tinytext", "mediumtext", "longtext",
		"enum", "set",
		"datetime", "timestamp", "date":
		return true
	default:
		return false
	}
}

// runSnapshotPointInTime resolves a single-row _snapshot query with
// baseline lookup. The pipeline mirrors `bintrail reconstruct`'s
// single-row path: find the baseline snapshot at-or-before AsOf, read
// the row's baseline image, then apply every binlog event from the
// snapshot instant up to AsOf on top of it.
//
// It falls back to the binlog-only point-lookup (runPointInTime) — with
// a log line so the degradation is visible — in four cases:
//   - no baseline source configured (the pre-#355 default),
//   - the PK column's type isn't one the baseline matcher supports
//     (a typed WHERE would silently miss the row),
//   - the schema resolver is unavailable (can't determine the PK type),
//   - no baseline exists for this table at-or-before AsOf.
//
// In every fallback case the binlog-only path is still correct for any
// row that has at least one event in the window; the only thing lost is
// the never-touched-since-before-the-window row, which is exactly what
// the baseline lookup recovers when it is available.
func (h *Handler) runSnapshotPointInTime(q TimeTravelQuery) (*mysql.Result, error) {
	src := h.baselineSource()
	if src == "" {
		h.logger.Debug("shim: _snapshot has no baseline source configured; using binlog-only path",
			"schema", q.Schema, "table", q.Table)
		return h.runPointInTime(q)
	}

	// Guard the PK type before attempting a baseline match. ReadBaselineRow
	// matches `pkColumn = ?` against the *typed* Parquet column binding
	// q.PKValue as a string and relying on DuckDB's implicit coercion — it
	// does NOT canonicalize the way the full-table path does. Only types
	// whose Parquet representation coerces cleanly from a string literal are
	// safe here (see baselinePKStringMatchable); for the rest the comparison
	// silently finds nothing and we would wrongly conclude the row never
	// existed, so we fall back to the binlog-only path with a signal.
	// validatePKColumn already confirmed q.PKColumn IS the table's
	// single-column PK (or was permissive because the resolver was
	// unavailable), so here we only need its type.
	dataType, ok := h.pkDataType(q.Schema, q.Table, q.PKColumn)
	if !ok {
		h.logger.Debug("shim: _snapshot cannot resolve PK type; using binlog-only path",
			"schema", q.Schema, "table", q.Table, "pk_column", q.PKColumn)
		return h.runPointInTime(q)
	}
	if !baselinePKStringMatchable(dataType) {
		h.logger.Warn("shim: _snapshot PK type not safe for baseline lookup; using binlog-only path",
			"schema", q.Schema, "table", q.Table, "pk_column", q.PKColumn, "pk_type", dataType)
		return h.runPointInTime(q)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baselinePath, snapshotTime, err := reconstruct.FindBaseline(ctx, src, q.Schema, q.Table, q.AsOf)
	if err != nil {
		if errors.Is(err, reconstruct.ErrNoBaseline) {
			h.logger.Debug("shim: _snapshot found no baseline at-or-before AsOf; using binlog-only path",
				"schema", q.Schema, "table", q.Table,
				"as_of", q.AsOf.UTC().Format(time.RFC3339))
			return h.runPointInTime(q)
		}
		// A real baseline-source failure (unreadable dir, S3 outage) is a
		// server-side fault: plain error → ER_UNKNOWN_ERROR (1105), the same
		// user-vs-server split runPointInTime preserves for fetch failures.
		return nil, err
	}

	baselineRow, err := reconstruct.ReadBaselineRow(ctx, baselinePath, map[string]string{q.PKColumn: q.PKValue})
	if err != nil {
		return nil, err
	}

	// Fetch every event for this PK from the snapshot instant up to AsOf.
	// Unlike the binlog-only path (LimitPerPK=1), we want the full ordered
	// sequence so ApplyAt folds them onto the baseline image in commit order
	// — matching `bintrail reconstruct`'s single-row semantics. Since=
	// snapshotTime drops pre-baseline events the baseline already supersedes,
	// making this strictly more correct than the binlog-only path for a row
	// whose only events predate the snapshot.
	//
	// PK filter: q.PKValue may legitimately be the empty string (e.g.
	// `WHERE name = ''` against a NOT-NULL string PK — the shape
	// runPointInTime documents). buildQuery DISABLES the PK filter entirely
	// when Options.PKValues == "", which would fold every event for the whole
	// table onto the single baseline row — a silent wrong answer. Route the
	// empty case through PKValuesIn, which emits `pk_values IN (?)` and keeps
	// the fetch scoped to the one row; keep the pk_hash fast path for the
	// common non-empty case.
	opts := query.Options{
		Schema: q.Schema,
		Table:  q.Table,
		Since:  &snapshotTime,
		Until:  &q.AsOf,
	}
	if q.PKValue != "" {
		opts.PKValues = q.PKValue
	} else {
		opts.PKValuesIn = []string{q.PKValue}
	}
	engine := query.New(h.indexDB)
	rows, _, err := query.FetchMerged(ctx, h.indexDB, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         h.cfg.IndexDBName,
		NoArchive:      h.cfg.NoArchive,
		AllowGaps:      h.cfg.AllowGaps,
		ArchiveFetcher: h.archiveFetcher,
	})
	if err != nil {
		return nil, wrapFetchError(q.Type, err)
	}

	state := reconstruct.ApplyAt(baselineRow, rows, q.AsOf)
	if state == nil {
		// Either the row never existed at AsOf (no baseline image and no
		// INSERT in the window) or its latest event was a DELETE.
		return emptyResult(), nil
	}

	// Same projection handling as runPointInTime: an explicit column list
	// is emitted verbatim; SELECT * uses the DDL column order.
	if q.Columns != nil {
		return imageToResultVerbatim(state, q.Columns)
	}
	return imageToResult(state, h.columnOrderFor(q.Schema, q.Table))
}

// pkDataType returns the DATA_TYPE of pkCol in schema.table from the
// latest schema snapshot. The bool is false when the resolver is
// unavailable or the column isn't found — callers treat that as "can't
// safely attempt a baseline match" and fall back to the binlog-only
// path.
func (h *Handler) pkDataType(schema, table, pkCol string) (string, bool) {
	if h.resolverFn == nil {
		return "", false
	}
	r, err := h.resolverCache.get(time.Now, resolverCacheTTL, h.resolverFn, h.logger)
	if err != nil {
		return "", false
	}
	tm, err := r.Resolve(schema, table)
	if err != nil {
		return "", false
	}
	for _, c := range tm.Columns {
		if c.Name == pkCol {
			return c.DataType, true
		}
	}
	return "", false
}
