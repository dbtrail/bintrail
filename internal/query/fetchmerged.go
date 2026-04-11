package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ArchiveFetcher fetches events from a single archive source (local Parquet
// directory or s3:// prefix). It is injected into FetchMerged as a function
// parameter so this package can orchestrate without importing parquetquery —
// parquetquery imports query.ResultRow, which would create a cycle. Callers
// pass parquetquery.Fetch directly, which has this exact signature.
type ArchiveFetcher func(ctx context.Context, opts Options, source string) ([]ResultRow, error)

// GapError is returned by FetchMerged when the query planner detects coverage
// gaps (hours rotated out of MySQL with no archive) and the caller set
// AllowGaps=false. It carries the gap hours so programmatic callers can
// inspect them via errors.As, e.g. to abort a multi-table reconstruct cleanly
// or render a structured error to an MCP client.
//
// The Error() string is deliberately library-neutral (no CLI flag name). CLI
// callers that want a flag-specific hint should unwrap with errors.As and
// re-wrap at the call site.
type GapError struct {
	GapHours []time.Time
}

func (e *GapError) Error() string {
	return FormatGapWarning(e.GapHours)
}

// FetchMergedOptions controls the behavior of FetchMerged. It is the
// cross-source orchestrator layer on top of Engine.Fetch + an injected
// archive fetcher, used by bintrail recover, single-row bintrail reconstruct,
// and full-table reconstruct (#187) to keep their fetch semantics in lockstep.
//
// The struct is deliberately a plain configuration bag rather than a pair of
// constructors (NewMySQLOnly / NewWithArchives) because every illegal
// combination is either caught by validation at entry (nil ArchiveFetcher
// when NoArchive=false; empty DBName when AllowGaps=false and a time range
// is set) or is a harmless unused field. The validation runs before any DB
// work happens so mistakes surface as clear errors, not silent misbehavior.
//
// Profile-mode RBAC enforcement (DenyTables/RedactColumns) is NOT plumbed
// through this struct. Archive queries do not apply those rules — that's a
// policy decision owned by the caller, not FetchMerged. A caller running
// under a profile must set NoArchive=true to avoid leaking redacted columns
// from Parquet archives; bintrail recover does this at the runRecover call
// site. See runRecover in cmd/bintrail/recover.go.
type FetchMergedOptions struct {
	// Opts is the query filter, passed through unchanged to both engine.Fetch
	// and the archive fetcher.
	Opts Options

	// DBName is the MySQL database name used by the query planner to look up
	// live partition boundaries. When empty, the planner cannot run and gap
	// detection is disabled. FetchMerged rejects an empty DBName when
	// AllowGaps=false and a time range is set, because strict-mode callers
	// cannot honor their contract without the planner.
	DBName string

	// NoArchive skips archive auto-discovery and the archive fetch loop. The
	// query planner still runs when DBName and a time range are set — the
	// planner only reads information_schema.PARTITIONS and archive_state and
	// does not touch the archives themselves, so gap detection remains
	// available under --no-archive.
	NoArchive bool

	// AllowGaps controls what happens when the planner reports coverage gaps
	// (hours rotated out of MySQL with no archive) or when the planner cannot
	// run or all archive sources fail. When false, FetchMerged returns an
	// error (a *GapError for planner gaps, a wrapped error for planner /
	// archive failures). When true, every such condition becomes an slog.Warn
	// and the function proceeds with whatever data it could fetch.
	//
	// bintrail recover uses true to preserve its existing warn-and-continue
	// behavior — it's generating reversal SQL that a human reviews. bintrail
	// reconstruct uses false because a silently incomplete row state is
	// worse than a clear error for point-in-time recovery.
	AllowGaps bool

	// ArchiveFetcher is the function used to fetch events from one archive
	// source. In production callers this is parquetquery.Fetch. Leaving it
	// nil together with NoArchive=false is a programming error and is
	// rejected with a clear error before any DB work happens — detecting
	// the misconfiguration early is why this package exists.
	ArchiveFetcher ArchiveFetcher
}

// validate checks FetchMergedOptions for illegal field combinations that would
// otherwise surface as silent failures downstream. Runs before any DB work so
// mistakes are caught with a clear error message.
func (o FetchMergedOptions) validate() error {
	if !o.NoArchive && o.ArchiveFetcher == nil {
		return errors.New("FetchMerged: ArchiveFetcher is required when NoArchive is false")
	}
	// Strict mode cannot honor its contract without the planner, and the
	// planner cannot run without a DBName. An empty DBName in strict mode
	// with a time range set would silently skip gap detection — the exact
	// class of bug this helper exists to prevent.
	if !o.AllowGaps && o.DBName == "" && (o.Opts.Since != nil || o.Opts.Until != nil) {
		return errors.New("FetchMerged: AllowGaps=false requires a non-empty DBName when a time range is set; gap detection cannot run without it")
	}
	return nil
}

// FetchMerged fetches events from live MySQL partitions and Parquet archives,
// deduplicates and sorts them via MergeResults, and enforces coverage gap
// detection according to FetchMergedOptions.AllowGaps.
//
// Returns the merged row set and the query plan. The plan is nil when the
// planner did not run (empty DBName, nil time range, or planner error under
// AllowGaps=true); callers using the plan for downstream reporting must
// nil-check.
//
// Failure modes:
//   - Options validation failure → returned immediately, zero DB work.
//   - Planner gap hours under AllowGaps=false → *GapError containing the
//     gap hours; inspect with errors.As.
//   - Planner DB error under AllowGaps=false → wrapped error.
//   - All archive sources fail under AllowGaps=false → wrapped error
//     naming the last archive source error.
//   - engine.Fetch failure → wrapped error.
//
// Under AllowGaps=true every non-fatal condition above becomes an slog.Warn
// and FetchMerged returns whatever partial data it could collect.
func FetchMerged(
	ctx context.Context,
	db *sql.DB,
	engine *Engine,
	o FetchMergedOptions,
) ([]ResultRow, *QueryPlan, error) {
	if err := o.validate(); err != nil {
		return nil, nil, err
	}

	var archSources []string
	if !o.NoArchive {
		archSources = ResolveArchiveSources(ctx, db)
	}

	// The query planner runs whenever the caller supplied a DBName and either
	// has a time range or has resolved archive sources. It runs regardless of
	// NoArchive because it only reads information_schema.PARTITIONS and
	// archive_state — no actual data fetch. This preserves gap detection for
	// --no-archive callers (observability win for recover, correctness win
	// for reconstruct under AllowGaps=false).
	var plan *QueryPlan
	if o.DBName != "" && (len(archSources) > 0 || o.Opts.Since != nil || o.Opts.Until != nil) {
		p, err := Plan(ctx, db, o.DBName, o.Opts.Since, o.Opts.Until)
		if err != nil {
			if !o.AllowGaps {
				return nil, nil, fmt.Errorf("query planner failed, cannot verify coverage: %w", err)
			}
			slog.Warn("query planner failed; coverage gaps may not be detected", "error", err)
		} else {
			plan = p
		}
	}

	// Gap enforcement runs before any fetch so we fail fast in strict mode.
	if plan != nil && len(plan.GapHours) > 0 {
		if !o.AllowGaps {
			return nil, plan, &GapError{GapHours: plan.GapHours}
		}
		slog.Warn(FormatGapWarning(plan.GapHours))
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

	successfulArchives := 0
	var lastArchiveErr error
	for _, src := range archSources {
		ar, err := o.ArchiveFetcher(ctx, o.Opts, src)
		if err != nil {
			// A broken archive must not silently block the entire query.
			// Log and move on, but remember the error so we can surface it
			// if every source fails under strict mode.
			slog.Warn("archive query failed, skipping", "source", src, "error", err)
			lastArchiveErr = err
			continue
		}
		successfulArchives++
		rows = append(rows, ar...)
	}

	// If the caller is in strict mode and every archive source we tried
	// failed, treat that as a hard error: we cannot verify that the archive
	// range is covered. Under AllowGaps=true we swallow the failure and
	// return whatever we got from live MySQL.
	if successfulArchives == 0 && lastArchiveErr != nil && !o.AllowGaps {
		return nil, plan, fmt.Errorf("all %d archive source(s) failed, cannot verify coverage: %w", len(archSources), lastArchiveErr)
	}

	rows = MergeResults(rows, o.Opts.Limit)
	return rows, plan, nil
}
