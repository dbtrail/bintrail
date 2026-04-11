package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
)

// ArchiveFetcher fetches events from a single archive source (local Parquet
// directory or s3:// prefix). It is injected into FetchMerged as a function
// parameter so this package can orchestrate without importing parquetquery
// (which would create a cycle, since parquetquery imports query.ResultRow).
//
// parquetquery.Fetch has exactly this signature, so callers pass it directly.
type ArchiveFetcher func(ctx context.Context, opts Options, source string) ([]ResultRow, error)

// FetchMergedOptions controls the behavior of FetchMerged. It is the
// cross-source orchestrator layer on top of Engine.Fetch + an injected
// archive fetcher, used by bintrail recover, single-row bintrail reconstruct,
// and full-table reconstruct (#187) to keep their fetch semantics in lockstep.
type FetchMergedOptions struct {
	// Opts is the query filter, passed through unchanged to both engine.Fetch
	// and the archive fetcher.
	Opts Options

	// DBName is the MySQL database name used by the query planner to look up
	// live partition boundaries. When empty, the planner is skipped and gap
	// detection is disabled.
	DBName string

	// NoArchive disables both archive auto-discovery and the query planner
	// (matching the current behavior of `bintrail recover --no-archive` at
	// cmd/bintrail/recover.go:167-180). Callers asking for strict gap
	// detection should leave this false.
	NoArchive bool

	// AllowGaps controls what happens when the planner reports coverage gaps
	// (hours rotated out of MySQL with no archive). When false, FetchMerged
	// returns an error. When true, it logs an slog.Warn and proceeds with
	// partial data. `bintrail recover` uses true to preserve its existing
	// warn-and-continue behavior; `bintrail reconstruct` uses false because
	// a silently-incomplete row reconstruction is worse than a clear error.
	AllowGaps bool

	// ArchiveFetcher is the function used to fetch events from one archive
	// source. In production callers this is parquetquery.Fetch. Leaving it
	// nil together with NoArchive=false is a programming error and causes
	// FetchMerged to panic (detected early — archives would otherwise be
	// silently skipped, which is exactly the class of bug this helper
	// exists to prevent).
	ArchiveFetcher ArchiveFetcher
}

// FetchMerged fetches events from live MySQL partitions and Parquet archives,
// deduplicates and sorts them via MergeResults, and optionally enforces
// coverage gap detection.
//
// Returns the merged row set and the query plan. The plan is nil when the
// planner did not run (NoArchive, empty DBName, or nil time range); callers
// using the plan for downstream reporting must nil-check.
//
// An archive source failure is non-fatal: FetchMerged logs a warning and
// continues with the remaining sources. A planner gap with AllowGaps=false is
// returned as an error containing the gap hours.
func FetchMerged(
	ctx context.Context,
	db *sql.DB,
	engine *Engine,
	o FetchMergedOptions,
) ([]ResultRow, *QueryPlan, error) {
	if !o.NoArchive && o.ArchiveFetcher == nil {
		return nil, nil, errors.New("FetchMerged: ArchiveFetcher is required when NoArchive is false")
	}

	var archSources []string
	if !o.NoArchive {
		archSources = ResolveArchiveSources(ctx, db)
	}

	// Run the planner when we have archives OR a time range, provided the
	// caller supplied a DBName. Matches the condition at recover.go:173.
	var plan *QueryPlan
	if !o.NoArchive && o.DBName != "" && (len(archSources) > 0 || o.Opts.Since != nil || o.Opts.Until != nil) {
		p, err := Plan(ctx, db, o.DBName, o.Opts.Since, o.Opts.Until)
		if err != nil {
			slog.Warn("query planner failed; coverage gaps may not be detected", "error", err)
		} else {
			plan = p
		}
	}

	// Gap enforcement runs before any fetch so we fail fast.
	if plan != nil && len(plan.GapHours) > 0 {
		warn := FormatGapWarning(plan.GapHours)
		if !o.AllowGaps {
			return nil, plan, fmt.Errorf("%s (pass --allow-gaps to proceed with incomplete data)", warn)
		}
		slog.Warn(warn)
	}

	// Fast path: no archives → single fetch from MySQL, no merge.
	if len(archSources) == 0 {
		rows, err := engine.Fetch(ctx, o.Opts)
		if err != nil {
			return nil, plan, err
		}
		return rows, plan, nil
	}

	// Archives present: fetch from MySQL unless the planner says we can skip
	// it, then append every archive source, then MergeResults to dedupe+sort.
	var rows []ResultRow
	if plan != nil && plan.SkipMySQL() {
		slog.Debug("planner: skipping MySQL query (range fully archived)")
	} else {
		r, err := engine.Fetch(ctx, o.Opts)
		if err != nil {
			return nil, plan, err
		}
		rows = r
	}

	for _, src := range archSources {
		ar, err := o.ArchiveFetcher(ctx, o.Opts, src)
		if err != nil {
			// Non-fatal: a broken archive must not block a query/recovery
			// operation. Log and move on.
			slog.Warn("archive query failed, skipping", "source", src, "error", err)
			continue
		}
		rows = append(rows, ar...)
	}
	rows = MergeResults(rows, o.Opts.Limit)

	return rows, plan, nil
}
