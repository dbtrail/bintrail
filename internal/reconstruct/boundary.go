package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// FetchEventsAtomic fetches events for a point-in-time reconstruction via
// query.FetchMerged and trims a trailing PARTIAL transaction from the result
// (#783). It is the convenience entry point for callers that don't need to
// inspect the raw fetch before trimming — see TrimPartialTailTransaction for
// callers that do (e.g. the console's fetch-then-overflow-check-then-trim
// ordering).
//
// event_timestamp is a Rows_event's STATEMENT execution time, not its
// transaction's COMMIT time. A multi-statement transaction executing at T1
// and T2 (T1 < T2) that commits later is written to the binlog — and
// indexed — as a contiguous run of row events sharing one GTID. Cutting
// naively at `event_timestamp <= at` (query.Options.Until) can therefore
// include only the transaction's T1 half when T1 <= at < T2: a row state
// that never existed at any real point in time, since MySQL only makes a
// transaction's changes visible atomically, at commit. See
// TrimPartialTailTransaction for the fix.
func FetchEventsAtomic(
	ctx context.Context,
	db *sql.DB,
	engine *query.Engine,
	fm query.FetchMergedOptions,
	at time.Time,
) ([]query.ResultRow, *query.QueryPlan, error) {
	events, plan, err := query.FetchMerged(ctx, db, engine, fm)
	if err != nil {
		return nil, plan, err
	}
	trimmed, err := TrimPartialTailTransaction(ctx, db, engine, fm, events, at)
	if err != nil {
		return nil, plan, err
	}
	return trimmed, plan, nil
}

// TrimPartialTailTransaction drops a trailing PARTIAL transaction from
// events (#783): events must already be sorted ascending by
// (event_timestamp, event_id) and already cut at `event_timestamp <= at`
// (e.g. via query.Options.Until) — exactly what query.FetchMerged returns.
//
// Binlog transactions are written (and therefore indexed) as a contiguous
// run of row events sharing one GTID, in commit order. This groups the TAIL
// of events by the last event's GTID and, with one bounded follow-up query
// scoped to that exact GTID, checks whether the transaction has ANY further
// event — on any table, not just the one being reconstructed, since a
// transaction is one atomic unit server-wide — at or after the first
// representable instant past `at`. If it does, the entire trailing GTID
// group is dropped: never partially applied. Only the LAST group can
// straddle the boundary (GTIDs are assigned, and their events written, in
// monotonic commit order), so one trim pass suffices.
//
// Events with no GTID (replication without GTIDs, or archived data
// predating the gtid column) can't be grouped this way and pass through
// unchanged — the pre-#783 per-row cut is the best available answer in that
// mode.
//
// Residual limitation: the index persists event_timestamp as DATETIME(0)
// (one-second granularity) and never records true commit time. This cannot
// resolve sub-second transaction ordering, nor the symmetric case of a
// transaction whose statements ALL execute before `at` but which commits
// after it (it is still included whole, indistinguishable from one that
// committed before `at`) — resolving that would require a commit-timestamp
// column the index does not have today. See docs/query-and-recovery.md.
func TrimPartialTailTransaction(
	ctx context.Context,
	db *sql.DB,
	engine *query.Engine,
	fm query.FetchMergedOptions,
	events []query.ResultRow,
	at time.Time,
) ([]query.ResultRow, error) {
	if len(events) == 0 {
		return events, nil
	}
	last := events[len(events)-1]
	if last.GTID == nil || *last.GTID == "" {
		return events, nil
	}
	gtid := *last.GTID

	// DATETIME(0) has one-second resolution: the first stored timestamp that
	// could fall outside the primary fetch's `event_timestamp <= at` cut is
	// floor(at)+1s, regardless of any sub-second component `at` carries
	// (time.Now() and the shim's relative time literals are both fractional
	// in normal use).
	lookAfter := at.Truncate(time.Second).Add(time.Second)
	continues, err := gtidHasEventAfter(ctx, db, engine, fm, gtid, lookAfter)
	if err != nil {
		// Surface-neutral on purpose (#1286): this error reaches MCP clients
		// verbatim via ErrorResult, and the tool parameter there is `at`, not
		// `--at` — naming the CLI flag is the leak class the MCP no-flag-leak
		// rule exists to prevent. Same pattern as query.GapError /
		// SourceEmptyError: the library Error() stays flag-free and each
		// surface attaches its own wording.
		return nil, fmt.Errorf("check whether transaction %s continues past the requested cut point: %w", gtid, err)
	}
	if !continues {
		return events, nil
	}

	cut := len(events)
	for cut > 0 && events[cut-1].GTID != nil && *events[cut-1].GTID == gtid {
		cut--
	}
	slog.Warn("reconstruct: excluded a transaction whose statements straddle --at (partial apply would produce a state that never existed) — #783",
		"gtid", gtid,
		"at", at.UTC().Format(time.RFC3339),
		"dropped_events", len(events)-cut)
	return events[:cut], nil
}

// gtidHasEventAfter reports whether any row event carrying gtid — on any
// table — was captured at or after `after`. Scoped to a single GTID
// equality lookup (gtid is an indexed column) with Limit=1: an existence
// check, never an unbounded scan.
//
// The probe always runs with AllowGaps=true, regardless of the caller's
// setting: it is a best-effort lookahead, not the primary fetch, and must
// never fail (or newly fail) a reconstruction over a coverage gap in a
// window the caller never asked about. A gap that hides a real continuation
// degrades to "treat the transaction as complete" — the pre-#783 behavior
// for that rare edge, strictly no worse than before this fix. It stays
// archive-aware (NoArchive/ArchiveFetcher inherited from fm) so an old `at`
// whose continuation has already rotated into an archive is still found.
func gtidHasEventAfter(
	ctx context.Context,
	db *sql.DB,
	engine *query.Engine,
	fm query.FetchMergedOptions,
	gtid string,
	after time.Time,
) (bool, error) {
	fm.Opts = query.Options{
		GTID:  gtid,
		Since: &after,
		Limit: 1,
	}
	fm.AllowGaps = true
	rows, _, err := query.FetchMerged(ctx, db, engine, fm)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}
