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

// SourceEmptyError is returned by an ArchiveFetcher when a REGISTERED
// archive source (an archive_state row) turns out to hold no Parquet data
// at all — distinct from a source that is healthy but has no data in the
// queried date range (which is an empty result, not an error). It signals
// stale registration: the files were deleted or moved after archive_state
// was written (#383). Under AllowGaps=false this aborts the query like any
// archive-source failure; programmatic callers can detect it with
// errors.As to attach remediation (e.g. `bintrail archive reconcile`).
//
// Like GapError, the Error() string is library-neutral (no CLI command or
// flag names) — CLI callers re-wrap with their own hint at the call site.
//
// Defined here rather than in parquetquery because parquetquery imports
// this package (ResultRow/Options) — the reverse import would cycle.
type SourceEmptyError struct {
	// Source is the archive source path (local base dir or s3:// prefix).
	Source string
}

func (e *SourceEmptyError) Error() string {
	return fmt.Sprintf("registered archive source %s contains no parquet data (stale archive_state registration?)", e.Source)
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
	// query planner still runs when DBName and a time range are set — it only
	// reads information_schema.PARTITIONS (and, when archives ARE included,
	// archive_state), never the archives themselves. Under NoArchive the
	// planner ignores archive_state coverage: hours rotated out of live MySQL
	// but present only in archives are counted as GAPS, since those archives
	// will not be fetched. This yields a *GapError under AllowGaps=false and an
	// slog.Warn under AllowGaps=true — without it a strict reconstruct would
	// silently omit the archived-only hours.
	NoArchive bool

	// AllowGaps controls what happens when the planner reports coverage gaps
	// (hours rotated out of MySQL with no archive) or when the planner cannot
	// run or any archive source fails. When false, FetchMerged returns an
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
//   - Any archive source fails under AllowGaps=false → wrapped error
//     naming the failed archive source.
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
	src, err := resolveMergeSources(ctx, db, o)
	if err != nil {
		return nil, src.plan, err
	}
	rows, _, err := fetchPage(ctx, engine, o, src)
	if err != nil {
		return nil, src.plan, err
	}
	return rows, src.plan, nil
}

// DefaultStreamBatchSize is the page size FetchMergedStream uses when the
// caller passes 0. Chosen as a compromise between resident memory (a page of
// ResultRows carries both decoded JSON row images, so roughly 1-2 KB per event
// for a narrow table) and round trips (each page costs one MySQL query plus one
// scan per archive source).
const DefaultStreamBatchSize = 100_000

// FetchMergedStream is the paginated form of FetchMerged: instead of
// materializing the whole window, it walks it in ascending (event_timestamp,
// event_id) order and hands each page to fn (#1097).
//
// Discovery, planning and gap enforcement run ONCE, before the first page —
// so a *GapError surfaces before any data is read, exactly as with FetchMerged,
// and archive_state is not re-read per page. Pagination itself is keyset, not
// OFFSET: each page resumes from the last row of the previous one via
// Options.AfterEvent, so cost per page does not grow with how far in you are,
// and archive file scoping advances with the cursor (see
// parquetquery.sinceLowerBoundHint) rather than re-listing the whole window.
//
// The page handed to fn is only valid for the duration of the call: the next
// page reuses nothing, but callers that retain individual rows past their page
// must copy them, because holding a *ResultRow into the page's backing array
// pins the ENTIRE page in memory — which would defeat the point of paging.
//
// fn returning an error stops the walk and propagates that error unchanged.
//
// Constraints (rejected up front rather than silently mis-served):
//   - Order must be ascending. A descending walk with an "after" cursor would
//     page away from the unread remainder.
//   - Opts.Limit and Opts.LimitPerPK must be unset. Both are whole-result-set
//     operations: applied per page they would cap each page instead of the
//     stream, silently returning a different set than FetchMerged would.
func FetchMergedStream(
	ctx context.Context,
	db *sql.DB,
	engine *Engine,
	o FetchMergedOptions,
	batchSize int,
	fn func([]ResultRow) error,
) (*QueryPlan, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	if OrderDirection(o.Opts.Order) == "DESC" {
		return nil, errors.New("FetchMergedStream: Order=DESC is not supported; the keyset cursor pages in ascending order only")
	}
	if o.Opts.Limit != 0 || o.Opts.LimitPerPK != 0 {
		return nil, errors.New("FetchMergedStream: Limit/LimitPerPK are whole-result-set caps and cannot be combined with paging; apply them in the caller")
	}
	if o.Opts.AfterEvent != nil {
		return nil, errors.New("FetchMergedStream: Opts.AfterEvent is managed by the stream and must not be preset")
	}
	if batchSize <= 0 {
		batchSize = DefaultStreamBatchSize
	}

	src, err := resolveMergeSources(ctx, db, o)
	if err != nil {
		return src.plan, err
	}

	pageOpts := o
	pageOpts.Opts.Limit = batchSize
	var cursor *EventCursor

	for {
		pageOpts.Opts.AfterEvent = cursor
		rows, skipped, err := fetchPage(ctx, engine, pageOpts, src)
		if err != nil {
			return src.plan, err
		}
		if len(rows) == 0 {
			return src.plan, nil
		}

		last := rows[len(rows)-1]
		next := EventCursor{Timestamp: last.EventTimestamp, EventID: last.EventID}
		// Forward-progress assertion. A page that ends at-or-before the cursor
		// it was fetched with means the engine ignored the keyset predicate, so
		// the next page would return the same rows forever. Fail loudly instead
		// of spinning — and instead of the alternative failure mode, a caller
		// folding the same events over and over while the run never ends.
		if cursor != nil && !next.After(*cursor) {
			return src.plan, fmt.Errorf(
				"FetchMergedStream: cursor did not advance past (%s, %d) — the keyset filter was not applied",
				cursor.Timestamp.UTC().Format(time.RFC3339), cursor.EventID)
		}

		if err := fn(rows); err != nil {
			return src.plan, err
		}

		// A short page proves every source is exhausted: the merged set is at
		// least as large as the largest single source's contribution, so it can
		// only fall below batchSize when NO source returned a full page. The
		// one exception is a source that failed and was skipped (AllowGaps
		// only) — that also shortens the page without proving exhaustion, and
		// stopping there would drop the remaining events of every other source
		// as well. In that case keep paging until a page comes back empty.
		if len(rows) < batchSize && !skipped {
			return src.plan, nil
		}
		cursor = &next
	}
}

// mergeSources is what the one-time prologue of a merged fetch resolves: the
// archive sources to read alongside MySQL, and the coverage plan those two were
// validated against. Split out so a paginated fetch (FetchMergedStream) runs
// source discovery, planning and gap enforcement ONCE for the whole stream
// instead of once per page — repeating them would re-read archive_state and
// re-evaluate identical coverage on every page, and would re-raise the same
// *GapError over and over.
type mergeSources struct {
	archSources []string
	plan        *QueryPlan
}

// resolveMergeSources discovers archive sources, runs the coverage planner and
// enforces gaps according to o.AllowGaps. The returned plan is non-nil whenever
// the planner ran, INCLUDING on the error path, so callers can surface it.
func resolveMergeSources(ctx context.Context, db *sql.DB, o FetchMergedOptions) (mergeSources, error) {
	var src mergeSources

	if !o.NoArchive {
		srcs, err := ResolveArchiveSources(ctx, db)
		if err != nil {
			// A failed registry read means an unknown set of sources is
			// missing while the planner (below) would still claim their
			// hours as covered. Strict mode cannot proceed on that.
			if !o.AllowGaps {
				return src, fmt.Errorf("resolve archive sources, cannot verify coverage: %w", err)
			}
			slog.Warn("archive source discovery failed; proceeding without archives", "error", err)
		}
		src.archSources = srcs
	}

	// The query planner runs whenever the caller supplied a DBName and either
	// has a time range or has resolved archive sources. It runs regardless of
	// NoArchive — no actual data fetch, just partition/archive_state metadata.
	// o.NoArchive is threaded in so that when archives are excluded, archived-
	// only hours are classified as gaps rather than covered. This preserves gap
	// detection for --no-archive callers (observability win for recover,
	// correctness win for reconstruct under AllowGaps=false).
	if o.DBName != "" && (len(src.archSources) > 0 || o.Opts.Since != nil || o.Opts.Until != nil) {
		p, err := Plan(ctx, db, o.DBName, o.Opts.Since, o.Opts.Until, o.NoArchive)
		if err != nil {
			if !o.AllowGaps {
				return src, fmt.Errorf("query planner failed, cannot verify coverage: %w", err)
			}
			slog.Warn("query planner failed; coverage gaps may not be detected", "error", err)
		} else {
			src.plan = p
		}
	}

	// Gap enforcement runs before any fetch so we fail fast in strict mode.
	if src.plan != nil && len(src.plan.GapHours) > 0 {
		if !o.AllowGaps {
			return src, &GapError{GapHours: src.plan.GapHours}
		}
		slog.Warn(FormatGapWarning(src.plan.GapHours))
	}
	return src, nil
}

// fetchPage runs ONE fetch of o.Opts against MySQL plus every resolved archive
// source and returns the merged rows. It performs no discovery, planning or gap
// enforcement — resolveMergeSources did all of that once, up front.
//
// skippedSource reports whether any archive source failed and was skipped,
// which only happens under AllowGaps=true. Paginating callers need to know:
// a short page normally proves every source is exhausted, but a skipped source
// can also shorten a page, and treating that as end-of-stream would drop the
// remaining events of every OTHER source too.
func fetchPage(
	ctx context.Context,
	engine *Engine,
	o FetchMergedOptions,
	src mergeSources,
) (rows []ResultRow, skippedSource bool, err error) {
	// Fast path: no archives → single fetch from MySQL, no merge. engine.Fetch
	// already applied ORDER BY and LIMIT in SQL, so MergeAndTrim would be a
	// no-op over a single source.
	if len(src.archSources) == 0 {
		r, ferr := engine.Fetch(ctx, o.Opts)
		if ferr != nil {
			return nil, false, ferr
		}
		return r, false, nil
	}

	// Archives present: fetch from MySQL unless the planner says we can skip
	// it, then append every archive source, then MergeResults to dedupe+sort.
	if src.plan != nil && src.plan.SkipMySQL() {
		slog.Debug("planner: skipping MySQL query (range fully archived)")
	} else {
		r, ferr := engine.Fetch(ctx, o.Opts)
		if ferr != nil {
			return nil, false, ferr
		}
		rows = r
	}

	for _, s := range src.archSources {
		ar, aerr := o.ArchiveFetcher(ctx, o.Opts, s)
		if aerr != nil {
			// In strict mode any broken archive source is fatal: each source
			// is a distinct bintrail_id whose deltas no other source carries,
			// and the planner validated archive_state coverage BEFORE this
			// fetch — so skipping the source would return an incomplete
			// result the caller has no way to detect (#377).
			if !o.AllowGaps {
				return nil, false, fmt.Errorf("archive source %s failed under strict mode, cannot verify coverage: %w", s, aerr)
			}
			// Permissive mode: a broken archive must not block the entire
			// query. Log and move on.
			slog.Warn("archive query failed, skipping", "source", s, "error", aerr)
			skippedSource = true
			continue
		}
		rows = append(rows, ar...)
	}

	return MergeAndTrim(rows, o.Opts.Limit, o.Opts.LimitPerPK, o.Opts.Order), skippedSource, nil
}
