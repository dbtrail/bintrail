// Package cascadebaseline implements cascade.BaselineProvider over
// internal/reconstruct — the Phase-2 fallback that lets cascade recovery
// recover a child row that has no binlog event in the lookback window by
// reading it out of a `bintrail baseline` Parquet snapshot.
//
// It is a LEAF package on purpose: the CLI (internal/cli) and the console
// (internal/console) each used to carry a private near-identical copy of this
// provider, and the copies drifted (#1101, #1102). Hosting the single
// implementation here — rather than having the console import internal/cli, or
// folding it into internal/cascade — keeps the console binary free of the whole
// CLI command layer and keeps the pure cascade engine free of the DuckDB/S3
// dependencies that baseline reads pull in.
package cascadebaseline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// FindBaselineFunc locates the baseline snapshot covering schema.table at-or-before
// at, returning its path, the snapshot's timestamp and any stale-fallback warning
// — the shape of reconstruct.FindBaseline with the source already bound.
//
// It is injected rather than called directly so each surface composes with its
// own baseline-resolution policy: the CLI binds a single --baseline-dir/--baseline-s3
// source (see Source), while the console passes its bundle's findBaseline, which
// retries the durable S3 copy when the local dir has no baseline for the table
// (#766). Before this was injectable the console's copy called
// reconstruct.FindBaseline directly and silently lost that fallback (#1102).
type FindBaselineFunc func(ctx context.Context, schema, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error)

// Source binds a single baseline source (a local directory or an s3:// prefix)
// to reconstruct.FindBaseline — the no-fallback lookup the CLI uses.
func Source(src string) FindBaselineFunc {
	return func(ctx context.Context, schema, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
		return reconstruct.FindBaseline(ctx, src, schema, table, at)
	}
}

// Provider implements cascade.BaselineProvider: it finds the child table's
// baseline snapshot, scans it for rows referencing the deleted parent, and
// encodes each row's PK to match binlog_events.pk_values so the cascade engine
// can dedup against Phase-1.
type Provider struct {
	find     FindBaselineFunc
	resolver *metadata.Resolver // for child PK columns
}

// New builds a Provider from a baseline lookup and the schema resolver used to
// encode each baseline row's PK. It never returns nil: callers assign the result
// to a cascade.BaselineProvider interface variable and test that variable for
// nil to decide whether Phase-2 ran, so a typed-nil would report an active
// baseline that does not exist.
func New(find FindBaselineFunc, resolver *metadata.Resolver) *Provider {
	return &Provider{find: find, resolver: resolver}
}

// BaselineChildren implements cascade.BaselineProvider.
func (p *Provider) BaselineChildren(ctx context.Context, schema, table, fkCol, parentPK string, at time.Time, limit int) (cascade.BaselineLookup, bool, error) {
	path, snap, stale, err := p.find(ctx, schema, table, at)
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
	// A generated PK member — the MariaDB system-versioning shape (#1266) —
	// cannot canonicalize against a baseline that omits the column; without
	// this gate every row below dies with MissingPKColumnError and its
	// misleading "run `bintrail snapshot` to refresh" remediation, which no
	// re-snapshot can ever satisfy. Fail loud with the real cause instead,
	// same stance as the fkFilterSafe refusal below.
	if c, found := reconstruct.GeneratedPKColumn(tm.PKColumnMetas()); found {
		// Classified as reconstruct.ErrGeneratedPK (#1273) so the cascade
		// engine's caveat classifier files this under the permanent
		// `generatedpk:` caveat — and skips Phase-1 for the edge too — instead
		// of the transient-sounding `baselinefail:` bucket. Built via
		// GeneratedPKRefusalError, not %w: the sentinel's own text would
		// stutter against the gate reason's opening clause.
		return cascade.BaselineLookup{}, false, reconstruct.GeneratedPKRefusalError(fmt.Sprintf(
			"baseline scan of %s.%s: %s",
			schema, table, reconstruct.GeneratedPKGateReason(c, "the cascade baseline fallback")))
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
	return cascade.BaselineLookup{SnapshotTime: snap, Rows: out, Truncated: trunc, SincePos: sincePos, StaleMessage: stale.Message}, true, nil
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
