package shim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// snapshotSincePos reads the baseline's exact recorded binlog position so a
// _snapshot delta fetch can anchor its lower bound there instead of the
// imprecise snapshotTime DATETIME (#797): a transaction whose statement
// executed just before snapshotTime but committed (and got logged) just after
// it would otherwise fall through both the dump's MVCC snapshot AND a
// Since-only fetch, silently missing from the result. Best-effort: a read
// failure or an older baseline that never recorded a position (BinlogFile==""
// or BinlogPos==0) just means nil — callers fall back to the pre-#797
// Since-only fetch.
func snapshotSincePos(ctx context.Context, baselinePath string, logger *slog.Logger, schema, table string) *query.BinlogPos {
	bmeta, err := baseline.ReadParquetMetadataAny(ctx, baselinePath)
	if err != nil {
		logger.Warn("shim: could not read baseline metadata for position-anchored delta fetch; falling back to timestamp-only Since",
			"schema", schema, "table", table, "path", baselinePath, "error", err)
		return nil
	}
	if bmeta.BinlogFile == "" || bmeta.BinlogPos <= 0 {
		return nil
	}
	return &query.BinlogPos{File: bmeta.BinlogFile, Pos: uint64(bmeta.BinlogPos)}
}

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

// errLimitReached short-circuits the buffered full-table merge once a user's
// LIMIT (#997) has been satisfied. Like errFullTableCapExceeded it never
// escapes runSnapshotFullTable — but it is a SUCCESS signal (the images slice
// is already truncated to the LIMIT), distinct from the cap error, so it must
// be checked before the cap-error translation.
var errLimitReached = errors.New("full-table snapshot LIMIT reached")

// runSnapshotFullTable resolves a full-table (no-WHERE) _snapshot query by
// merging the baseline snapshot at-or-before AsOf with the post-snapshot
// binlog deltas across the whole table (#362). The result is the table's true
// row state at AsOf: never-touched baseline rows pass through, rows updated
// after the baseline take their latest event image, rows deleted after the
// baseline drop out, and rows inserted after the baseline appear — exactly what
// the offline `bintrail reconstruct` produces, but streamed into an in-memory
// resultset.
//
// When NO baseline source is configured it falls back to the binlog-only
// full-table path (runFullTable) — the intended "_snapshot degrades to
// _flashback" default, where binlog-only completeness is exactly what was
// asked for.
//
// But when a baseline source IS configured the operator explicitly opted into
// full-table completeness, so a merge that can't run must FAIL LOUD rather than
// silently return a partial (binlog-activity-only) table indistinguishable from
// a complete one — the same fail-loud contract as the shim's strict AllowGaps
// default and the 1104 row cap (#822). These cases return an actionable wire
// error (ER_NO_PARTITION_FOR_GIVEN_VALUE / 1526 — the same code wrapFetchError
// uses for a coverage gap the index can't answer), pointing the operator at the
// fix and at _flashback for a binlog-only view:
//   - the table's PK can't be resolved from the schema snapshot (re-snapshot),
//   - a PK column's type isn't supported by the baseline canonicalizer,
//   - no baseline exists for this table at-or-before AsOf (take a baseline).
//
// Real faults (baseline source unreadable, fetch failure, a PK value the
// canonicalizer can't translate) surface as plain errors → ER_UNKNOWN_ERROR
// (1105), the same user-vs-server split the single-row path preserves.
func (h *Handler) runSnapshotFullTable(q TimeTravelQuery) (*mysql.Result, error) {
	src := h.baselineSource()
	if src == "" {
		h.logger.Debug("shim: full-table _snapshot has no baseline source configured; using binlog-only path",
			"schema", q.Schema, "table", q.Table)
		return h.runFullTable(q)
	}

	pkCols, ok := h.pkColumnMetas(q.Schema, q.Table)
	if !ok || len(pkCols) == 0 {
		// A baseline source IS configured, so the operator opted into full-table
		// completeness — but we can't determine the table's PK to canonicalize
		// baseline rows against. Degrading to runFullTable here would silently
		// drop every never-touched baseline row and return a partial table
		// indistinguishable from a complete one; that contradicts the shim's
		// fail-loud contract (strict AllowGaps, the 1104 row cap). Refuse with an
		// actionable wire error instead (#822). The usual cause is a stale/absent
		// schema snapshot, fixable with `bintrail snapshot`; _flashback stays
		// available for a binlog-only view.
		return nil, mysql.NewError(mysql.ER_NO_PARTITION_FOR_GIVEN_VALUE, fmt.Sprintf(
			"resolve %s: cannot determine the primary key of %s.%s from the schema snapshot; "+
				"_snapshot cannot return a complete table (never-touched rows would be omitted) — "+
				"run `bintrail snapshot` to refresh the schema, or use _flashback for a binlog-only view",
			q.Type, q.Schema, q.Table))
	}
	for _, c := range pkCols {
		if !reconstruct.SupportedPKType(c.DataType) {
			// Baseline configured but this PK type can't be canonicalized for the
			// merge — same fail-loud reasoning as the unresolved-PK branch (#822):
			// silently returning binlog-only rows would be a partial table the
			// operator can't distinguish from a complete one.
			return nil, mysql.NewError(mysql.ER_NO_PARTITION_FOR_GIVEN_VALUE, fmt.Sprintf(
				"resolve %s: primary key column %s of %s.%s has type %s, which the baseline merge cannot canonicalize; "+
					"_snapshot cannot return a complete table (never-touched rows would be omitted) — "+
					"use _flashback for a binlog-only view",
				q.Type, c.Name, q.Schema, q.Table, c.DataType))
		}
	}

	ctx, cancel := h.queryContext()
	defer cancel()

	// Bound concurrent full-table reconstructions (#823). Taken here —
	// AFTER the src=="" fallback above, which delegates to runFullTable
	// (that path acquires the gate itself; acquiring before the branch
	// would deadlock a cap-1 gate) and after the cheap fail-fast PK
	// refusals — so the slot is held only for the heavy part:
	// FindBaseline, the delta fetch, and the DuckDB baseline merge.
	if err := h.cfg.FullTableGate.Acquire(ctx); err != nil {
		return nil, h.fullTableGateError(q.Type, err)
	}
	defer h.cfg.FullTableGate.Release()

	// Shim handling of the stale-fallback warning (#466) is intentionally
	// minimal: FindBaseline already logs it server-side and an in-band MySQL
	// signal needs design (deferred by #466), so we discard it here.
	baselinePath, snapshotTime, _, err := reconstruct.FindBaseline(ctx, src, q.Schema, q.Table, q.AsOf)
	if err != nil {
		if errors.Is(err, reconstruct.ErrNoBaseline) {
			// Source configured but no baseline exists for this table at-or-before
			// AsOf (not yet baselined, or the table was created after the last
			// snapshot). Degrading to runFullTable would silently omit every
			// never-touched row and return a partial table the caller can't
			// distinguish from a complete one. Since the operator explicitly
			// configured a baseline for full-table completeness, fail loud with an
			// actionable wire error instead (#822) — mirroring the strict
			// AllowGaps / 1104 row-cap contract.
			return nil, mysql.NewError(mysql.ER_NO_PARTITION_FOR_GIVEN_VALUE, fmt.Sprintf(
				"resolve %s: no baseline for %s.%s at or before %s; _snapshot cannot return a complete table "+
					"(never-touched rows would be omitted) — take a baseline (`bintrail baseline`), "+
					"or use _flashback for a binlog-only view",
				q.Type, q.Schema, q.Table, q.AsOf.UTC().Format(time.RFC3339)))
		}
		// A real baseline-source failure is a server-side fault, same as the
		// single-row path: plain error → ER_UNKNOWN_ERROR (1105).
		return nil, err
	}

	// Refuse if a TRUNCATE/DROP/RENAME hit this table in the window: it emits
	// no row events, so the merge below would replay the baseline straight
	// through and silently resurrect rows the DDL actually deleted (#764).
	if err := reconstruct.CheckDestructiveDDL(ctx, h.indexDB, q.Schema, q.Table, snapshotTime, q.AsOf); err != nil {
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
			SincePos:   snapshotSincePos(ctx, baselinePath, h.logger, q.Schema, q.Table),
			Until:      &q.AsOf,
			LimitPerPK: 1,
		},
		DBName:         h.cfg.IndexDBName,
		NoArchive:      h.cfg.NoArchive,
		AllowGaps:      h.cfg.AllowGaps,
		ArchiveFetcher: h.archiveFetcher,
	})
	if err != nil {
		return nil, wrapFetchError(ctx, q.Type, err, h.logger)
	}
	// ENUM/SET ordinals → labels per event's snapshot epoch (#472/#475),
	// BEFORE the merge: the merged rowMap reaching the callback below has
	// no per-row timestamp, and fullTableTextCell would coerce a delta's
	// float64 ordinal into the text "3" — which the mapper (correctly)
	// refuses to touch. Baseline rows already carry labels and pass
	// through unchanged.
	h.mapEventImages(q.Schema, q.Table, rows)
	changes := make(map[string]*query.ResultRow, len(rows))
	for i := range rows {
		changes[rows[i].PKValues] = &rows[i]
	}

	input := reconstruct.SnapshotFullTableInput{
		BaselinePath: baselinePath,
		Schema:       q.Schema,
		Table:        q.Table,
		PKCols:       pkCols,
		Changes:      changes,
		// rows was fetched LimitPerPK=1, so it is already collapsed to the latest
		// event per PK; hand it to the #782 guard anyway (authoritative over what
		// was fetched), bounded by that fetch (see pkChangingUpdateInEvents).
		Events: rows,
	}

	// #998: with a bound connection and no LIMIT, stream the merged rows straight
	// to the wire. The baseline (potentially millions of rows) flows through the
	// DuckDB merge cursor one row at a time, so peak shim memory is O(post-baseline
	// changes) — the `changes` map above — not O(table size), and the
	// FullTableRowCap ceiling is lifted. A LIMIT keeps the bounded buffered path
	// below (cheap browse); an unbound handler (unit tests, embedders that never
	// call BindConn) also stays buffered.
	//
	// The streamed column set is fixed before the first row (the header must
	// precede any row): the user's explicit projection verbatim when given (#313,
	// matching the buffered imagesToResultVerbatim — a projected full-table query
	// streams too, since a projection doesn't change the row count), else the
	// table's newest-snapshot order. When neither is resolvable (empty projection
	// is impossible; nil snapshot), fall through to the buffered path, which
	// derives columns from the images it has.
	streamCols := q.Columns
	if streamCols == nil {
		streamCols = h.columnOrderFor(q.Schema, q.Table)
	}
	if h.conn != nil && q.Limit == 0 && len(streamCols) > 0 {
		return h.streamSnapshotFullTable(ctx, q, input, streamCols)
	}

	rowCap := h.cfg.FullTableRowCap
	if rowCap <= 0 {
		rowCap = defaultFullTableRowCap
	}

	// Buffer the merged rows, coercing every value to a uniform text cell
	// (see fullTableTextCell). This is required, not cosmetic: a baseline row
	// carries DuckDB-native types (an int column is int32 → LONGLONG) while a
	// post-baseline event image is JSON-decoded (the same column arrives as
	// json.Number), and BuildSimpleTextResultset rejects a column whose non-NULL
	// rows disagree on type ("row types aren't consistent"). Rendering every cell
	// to its text bytes makes each column uniformly VAR_STRING. For INT and DOUBLE
	// columns the two origins render byte-identically: fullTableTextCell routes
	// json.Number through numberToText, the same FormatTextValue path the default
	// branch uses for baseline values; and the #496 read-path fix keeps event-side
	// integers exact (json.Number, no float64 rounding). Known pre-existing
	// exceptions, both baseline-side and out of this path's control: FLOAT(32-bit)
	// columns diverge (DuckDB scans baseline FLOAT as float32, which FormatTextValue
	// widens — "0.10000000149011612" vs the event side's "0.1"), and a baseline
	// BIGINT UNSIGNED > 2^63 can't round-trip (baseline stores bigint as signed
	// Int64). Stop at cap+1 so overflow is detectable.
	// Residual unchanged-TOAST markers (#592) are caught upstream by
	// SnapshotFullTableImages' checkChangesToast on the delta events (baseline
	// rows can't carry a marker), NOT by buildImagesResult below: fullTableTextCell
	// coerces every cell to text first, so a marker would already be a []byte by
	// the time buildImagesResult's IsUnchangedToastMarker check runs and never
	// match. Keep the upstream guard — the streaming sibling (projectCell) checks
	// the raw value instead, which is the pattern to prefer.
	images := make([]map[string]any, 0)
	err = reconstruct.SnapshotFullTableImages(ctx, input, func(rowMap map[string]any) error {
		img := make(map[string]any, len(rowMap))
		for k, v := range rowMap {
			img[k] = h.fullTableTextCell(q.Schema, q.Table, k, v)
		}
		images = append(images, img)
		// Cap FIRST — a LIMIT never RAISES the cap (#997): overflow past rowCap
		// aborts, and only then does a LIMIT at or below rowCap stop the merge
		// early via the success sentinel, so the browse succeeds instead of
		// tripping the cap.
		if len(images) > rowCap {
			return errFullTableCapExceeded
		}
		if q.Limit > 0 && len(images) >= q.Limit {
			return errLimitReached
		}
		return nil
	})
	// Keep the errLimitReached SUCCESS sentinel filtered out FIRST: images is
	// already truncated to the LIMIT. A future refactor into sequential
	// `if errors.Is(...)` branches must preserve this ordering, or a legit LIMIT
	// success would be mistranslated into a wire error.
	if err != nil && !errors.Is(err, errLimitReached) {
		if errors.Is(err, errFullTableCapExceeded) {
			return nil, mysql.NewError(mysql.ER_TOO_BIG_SELECT, fmt.Sprintf(
				"resolve %s: %s.%s at %s would return more than %d rows; add a LIMIT (e.g. LIMIT %d) to browse, narrow the AS OF range or filter by PK",
				q.Type, q.Schema, q.Table, q.AsOf.Format("2006-01-02 15:04:05"), rowCap, rowCap,
			))
		}
		return nil, err
	}

	return h.fullTableResult(q, images)
}

// streamSnapshotFullTable streams a full-table _snapshot resultset row-by-row
// over the bound connection (#998), lifting the FullTableRowCap for the
// baseline-merge path. It projects every merged row onto cols — a FIXED column
// set decided before the first row (the streaming header must be written before
// any row is seen): the user's explicit projection when given (verbatim, like
// imagesToResultVerbatim), else the table's newest-snapshot order. Keys missing
// from a row render as NULL.
//
// SELECT * caveat: for the newest-snapshot column set, a column dropped between
// AS OF and now (present in a row image but absent from the current snapshot —
// the #600 case) is NOT surfaced on the streaming path, because the fixed set
// can't append per-row image-only keys the way the buffered fullTableColumns
// does; a LIMIT'd (buffered) SELECT * still surfaces it. An explicit projection
// is unaffected — imagesToResultVerbatim never appends image-only keys either.
//
// Errors: reconstruct.SnapshotFullTableImages runs its TOAST / PK-changing-
// UPDATE guards before materialising the baseline and emits nothing until the
// merge starts, so a setup failure returns before any wire write (a clean
// first-packet ERR). A failure once rows are already on the wire returns an
// error that go-mysql renders as an ERR packet mid-resultset — the client sees
// no terminating EOF and reads it as an unambiguous failure (see streamWriter).
func (h *Handler) streamSnapshotFullTable(ctx context.Context, q TimeTravelQuery, input reconstruct.SnapshotFullTableInput, cols []string) (*mysql.Result, error) {
	sw := newStreamWriter(h.conn, cols)
	cells := make([]any, len(cols)) // reused per row; writeRow encodes synchronously

	// SELECT * degrades LOUDLY, not silently, for the #600 dropped-column case.
	// The streamed header is fixed before the first row, so — unlike the buffered
	// fullTableColumns — it can't append a per-row image-only key (a column
	// present at AS OF but dropped from the current schema since). Rather than
	// omit it silently (the shim Warns before degrading everywhere else —
	// fullTableTextCell, tableMetaFor), Warn ONCE per query naming the column and
	// pointing at the LIMIT'd buffered path that surfaces it. Only for SELECT *; a
	// verbatim projection (q.Columns != nil) intentionally excludes unrequested
	// columns. A full union header would need the baseline Parquet schema up
	// front, i.e. a second full materialization of an S3 baseline — deferred.
	var known map[string]struct{}
	if q.Columns == nil {
		known = make(map[string]struct{}, len(cols))
		for _, c := range cols {
			known[c] = struct{}{}
		}
	}
	warnedDrop := false

	err := reconstruct.SnapshotFullTableImages(ctx, input, func(rowMap map[string]any) error {
		if known != nil && !warnedDrop {
			for k := range rowMap {
				if _, ok := known[k]; !ok {
					h.logger.Warn("shim: streaming full-table _snapshot SELECT * omits a column present at AS OF but dropped from the current schema (the streamed column set is fixed before the first row) — re-run with a LIMIT to use the buffered path that surfaces it",
						"schema", q.Schema, "table", q.Table, "omitted_column", k)
					warnedDrop = true
					break
				}
			}
		}
		for i, c := range cols {
			v, cerr := h.projectCell(q.Schema, q.Table, c, rowMap[c])
			if cerr != nil {
				return cerr
			}
			cells[i] = v
		}
		return sw.writeRow(cells)
	})
	if err != nil {
		return nil, wrapFetchError(ctx, q.Type, err, h.logger)
	}
	return sw.finish()
}

// pkColumnMetas returns the primary-key column metas of schema.table from its
// newest schema snapshot, for canonicalizing baseline PK values. The bool is
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
	case json.Number:
		// Post-baseline event images are JSON-decoded as json.Number (#496).
		// Render through numberToText (FormatTextValue) so event-origin cells emit
		// the same wire bytes as baseline-origin INT/DOUBLE cells (the default
		// branch below renders DuckDB-native int64/float64 the same way). Baseline
		// FLOAT (float32) is the known exception — see runSnapshotFullTable.
		return numberToText(x)
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

	ctx, cancel := h.queryContext()
	defer cancel()

	// Stale-fallback warning (#466) discarded here — see the full-table path.
	baselinePath, snapshotTime, _, err := reconstruct.FindBaseline(ctx, src, q.Schema, q.Table, q.AsOf)
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

	// Refuse if a TRUNCATE/DROP/RENAME hit this table in the window: same
	// blind spot as the full-table path — no row events to invalidate the
	// baseline image, so the row would silently resolve as if it still
	// existed after being truncated away (#764).
	if err := reconstruct.CheckDestructiveDDL(ctx, h.indexDB, q.Schema, q.Table, snapshotTime, q.AsOf); err != nil {
		return nil, err
	}

	// q.PKValue is passed RAW (unescaped) here — Parquet baseline rows store
	// actual column values, not event.BuildPKValues-encoded pk_values, so this
	// seam must NOT apply event.EscapePKValue (unlike the delta fetch below).
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
		Schema:   q.Schema,
		Table:    q.Table,
		Since:    &snapshotTime,
		SincePos: snapshotSincePos(ctx, baselinePath, h.logger, q.Schema, q.Table),
		Until:    &q.AsOf,
	}
	// q.PKValue is raw/unescaped (#826). Unlike ReadBaselineRow above (which
	// matches actual Parquet column values and must keep the raw value), this
	// delta fetch matches binlog_events.pk_values, which is
	// event.BuildPKValues-encoded — re-encode with event.EscapePKValue so a
	// backslash- or pipe-containing PK's post-baseline events are found
	// instead of silently missing (which would serve a stale baseline image
	// as the answer at AsOf).
	encoded := event.EscapePKValue(q.PKValue)
	if q.PKValue != "" {
		opts.PKValues = encoded
	} else {
		opts.PKValuesIn = []string{encoded}
	}
	engine := query.New(h.indexDB)
	// FetchEventsAtomic (not a plain query.FetchMerged) cuts the AsOf upper
	// bound at the transaction boundary, not the row: a multi-statement
	// transaction straddling AsOf is excluded whole rather than half-applied
	// (#783).
	rows, _, err := reconstruct.FetchEventsAtomic(ctx, h.indexDB, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         h.cfg.IndexDBName,
		NoArchive:      h.cfg.NoArchive,
		AllowGaps:      h.cfg.AllowGaps,
		ArchiveFetcher: h.archiveFetcher,
	}, q.AsOf)
	if err != nil {
		return nil, wrapFetchError(ctx, q.Type, err, h.logger)
	}

	// ENUM/SET ordinals → labels per event's snapshot epoch (#472/#475),
	// BEFORE the fold: ApplyAt replaces the image wholesale per event, so
	// pre-mapped events make the final state carry labels. Only deltas
	// need this — the baseline image already carries labels (mydumper
	// dumps strings) and string values pass through the mapper untouched.
	h.mapEventImages(q.Schema, q.Table, rows)
	state, err := reconstruct.ApplyAt(baselineRow, rows, q.AsOf)
	if err != nil {
		// A residual unchanged-TOAST marker (#592) — a capture-invariant
		// violation, i.e. a server-side data fault: plain error →
		// ER_UNKNOWN_ERROR (1105), same as a baseline-source failure above.
		// Refusing beats serving the marker's JSON as the column value.
		return nil, err
	}
	if len(state) == 0 {
		// Either the row never existed at AsOf (no baseline image and no INSERT
		// in the window) or its latest event was a DELETE. A non-DELETE tail that
		// folds to empty is a corrupt/partial row image — surfaced as a Warn, not
		// silently (mirrors runPointInTime).
		h.warnCorruptImageDrop(q.Schema, q.Table, rows)
		return emptyResult(), nil
	}

	// Same projection handling as runPointInTime: an explicit column list
	// is emitted verbatim; SELECT * uses the DDL column order.
	if q.Columns != nil {
		return imageToResultVerbatim(state, q.Columns)
	}
	return imageToResult(state, h.columnOrderFor(q.Schema, q.Table))
}

// pkDataType returns the DATA_TYPE of pkCol in schema.table from its
// newest schema snapshot. The bool is false when the resolver is
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
