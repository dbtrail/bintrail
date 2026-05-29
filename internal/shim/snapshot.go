package shim

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

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
// Full-table _snapshot (q.PKColumn == "") falls through to the existing
// binlog-only runFullTable for now — in-memory baseline merge for the
// whole-table shape is tracked as a follow-up (see #355). Single-row
// _snapshot is where the baseline lookup applies.
func (h *Handler) runSnapshot(q TimeTravelQuery) (*mysql.Result, error) {
	if q.PKColumn == "" {
		// Full-table baseline merge is deferred; whole-table _snapshot
		// behaves like the binlog-only path until that lands.
		return h.runFullTable(q)
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

// baselinePKStringMatchable reports whether a PK column of the given
// MySQL DATA_TYPE can be matched in the baseline Parquet by binding the
// PK value as a string parameter to `pkColumn = ?` (what ReadBaselineRow
// does). The single-row path does NOT canonicalize the value, so this is
// narrower than reconstruct's full-table supportedPKType:
//
//   - SAFE — the Parquet column is an integer (int/year → INT32/64) or a
//     string (decimal/numeric, char/varchar/text, enum/set → STRING).
//     DuckDB coerces a string literal to an integer column, and a string
//     column compares byte-for-byte against the same string the indexer
//     stored, so the match round-trips.
//   - UNSAFE — datetime/timestamp (Parquet TIMESTAMP) and date (Parquet
//     DATE). DuckDB's `temporal_col = 'literal'` comparison does not
//     reliably match a string-bound parameter here, so the lookup would
//     silently find nothing. These fall back to the binlog-only path with
//     a Warn rather than risk a false "row never existed". Full temporal
//     PK support needs the canonicalization the offline `bintrail
//     reconstruct` command applies and is left to the follow-up.
//
// Types reconstruct rejects outright (FLOAT, BLOB, BIT, JSON, …) are not
// listed and therefore also fall back.
func baselinePKStringMatchable(dataType string) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "int", "integer", "smallint", "tinyint", "mediumint", "bigint",
		"year",
		"decimal", "numeric",
		"char", "varchar", "text", "tinytext", "mediumtext", "longtext",
		"enum", "set":
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
//   - the PK column's type isn't one the baseline matcher canonicalizes
//     correctly (a typed WHERE would silently miss the row),
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
