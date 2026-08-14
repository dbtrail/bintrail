package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// TimeRange represents a contiguous UTC time range at hour granularity.
type TimeRange struct {
	Start time.Time // inclusive, truncated to hour
	End   time.Time // exclusive, truncated to hour
}

// QueryPlan describes how to route a query across live MySQL partitions and
// Parquet archives. It is produced by Plan().
//
// A nil plan means the planner could not run (no time range, nil DB, etc.)
// and callers should fall back to the default unoptimised path.
type QueryPlan struct {
	// MySQLRanges are time ranges that should be queried against live MySQL.
	// Empty when the entire range is covered by archives.
	MySQLRanges []TimeRange

	// GapHours are hours where data has been rotated out of MySQL and no
	// archive exists. Callers should emit a warning for these.
	GapHours []time.Time

	// OldestKnownHour is the earliest hour this index has ever held — the
	// oldest live partition or archived hour the planner saw (post-noArchive
	// filtering, so under --no-archive it is the oldest LIVE partition only).
	// Zero when no partitions exist at all. It lets a caller tell a gap hour
	// that ROTATED away from one that predates the index's existence — on an
	// index younger than the queried window, every pre-install hour is a
	// "gap" that was never captured in the first place (#1126).
	OldestKnownHour time.Time

	// MisfiledArchiveHours are the hour LABELS of archived partitions whose
	// content-derived time range (archive_state.min/max_event_ts, #1037)
	// escapes the label hour AND overlaps the queried range. Backfilled
	// events land in the oldest live RANGE partition and get archived under
	// its hour label, so a time-scoped read that prunes archive files by
	// label alone would skip the very file holding those rows. Callers must
	// copy this into Options.ExtraArchiveHours for their archive fetches so
	// file/date scoping still opens those files; row-level time filters keep
	// the result set correct.
	MisfiledArchiveHours []time.Time

	// ArchiveCoverageUnavailable reports that archive_state exists but could
	// not be read, so archive coverage was NOT evaluated (#1324). Hours the
	// archives may hold are classified as GapHours — fail-closed for data —
	// but a caller rendering completeness or naming a cause must key on this:
	// "complete" was never checked, and "rotated and not archived" was never
	// established. A genuinely missing archive_state (ER_NO_SUCH_TABLE, an
	// index that never archived) does NOT set it — there the gaps are the
	// truth — and neither does noArchive, where skipping the read is the
	// caller's decision rather than a failure.
	ArchiveCoverageUnavailable bool
}

// Plan builds a QueryPlan for the given time range by inspecting live partition
// boundaries and archive_state coverage. When since or until is nil, that bound
// is left open (no routing optimisation is applied for the open side).
//
// Returns (nil, nil) when planning is not applicable (nil DB, no time range).
// Returns (nil, error) when planning fails due to a database error.
// dbName is the MySQL database name (needed for information_schema queries).
//
// noArchive mirrors the caller's decision to exclude Parquet archives from the
// fetch (--no-archive or an active RBAC profile). When true, archive_state
// coverage is NOT read or counted: hours rotated out of live MySQL but present
// only in archives are then classified as GAPS, because those archives will not
// be fetched. Counting them as covered would let a strict (AllowGaps=false)
// reconstruct silently return incomplete state.
//
// sourceIDs names the archive sources this read will actually OPEN, as
// bintrail_ids (#1232). It is the same rule as noArchive applied at a finer
// grain: coverage recorded by an archive the fetch will not read is not
// coverage. nil means "every archive registered in the index", which is the
// only honest answer for a caller that reads them all — or that cannot
// enumerate the set. An EMPTY non-nil slice means "this read opens no
// archives", so every rotated hour is a gap.
//
// Note what this deliberately does NOT scope by: which source PRODUCED the
// events. binlog_events carries no source discriminator and ArchivePartition
// archives the whole shared partition, so one source's archive of hour H holds
// every source's events for H. archive_state.bintrail_id records who archived
// a partition, not whose rows are in it — scoping by data ownership would
// report gaps over data that is present.
func Plan(ctx context.Context, db *sql.DB, dbName string, since, until *time.Time, noArchive bool, sourceIDs []string) (*QueryPlan, error) {
	if db == nil || dbName == "" {
		return nil, nil
	}

	// If no time range is specified, we can't do partition-level routing.
	if since == nil && until == nil {
		return nil, nil
	}

	// Determine the hour-aligned query range.
	var rangeStart, rangeEnd time.Time
	if since != nil {
		rangeStart = since.Truncate(time.Hour)
	}
	if until != nil {
		// End is exclusive: the hour containing 'until' plus one.
		rangeEnd = until.Truncate(time.Hour).Add(time.Hour)
	}

	// Load live partition boundaries.
	liveHours, err := loadLivePartitionHours(ctx, db, dbName)
	if err != nil {
		return nil, fmt.Errorf("load partition info for planning: %w", err)
	}

	// Load archived partition names. A missing archive_state (ER_NO_SUCH_TABLE
	// — an index that never archived) is non-fatal and means there is no
	// archive tier; gap detection still works from live partitions. Every
	// OTHER failure is a different fact (#1324, the same conflation #816
	// retired in status.LoadCoverage): archives may exist that this plan could
	// not see, so it is recorded on the plan instead of letting "unreadable"
	// classify as "no archives". Either way cov stays nil — fail-closed: an
	// hour that could not be verified is a gap, never coverage.
	//
	// Skipped entirely when noArchive: those archives are excluded from the
	// fetch, so reading their coverage here would only mislabel rotated hours as
	// covered. Leaving archivedHours nil makes buildPlan classify them as gaps.
	var cov []archiveCoverage
	var covUnavailable bool
	if !noArchive {
		cov, err = loadArchiveCoverage(ctx, db, sourceIDs)
		if err != nil {
			if isMissingTableErr(err) {
				slog.Debug("archive_state not present; planning from live partitions only", "error", err)
			} else {
				slog.Warn("could not read archive coverage for planning; archived hours will be classified as gaps", "error", err)
				covUnavailable = true
			}
			cov = nil
		}
	}

	plan := buildPlan(liveHours, expandArchiveHours(cov), rangeStart, rangeEnd, noArchive)
	if plan != nil {
		plan.ArchiveCoverageUnavailable = covUnavailable
		if !noArchive {
			plan.MisfiledArchiveHours = misfiledHours(cov, rangeStart, rangeEnd)
		}
	}
	return plan, nil
}

// buildPlan is the pure-logic core of the planner. It classifies each hour in
// [rangeStart, rangeEnd) as live, archived, or gap, then builds a QueryPlan.
// This function is extracted from Plan() for testability.
func buildPlan(liveHours, archivedHours []time.Time, rangeStart, rangeEnd time.Time, noArchive bool) *QueryPlan {
	// When archives are excluded from the fetch (--no-archive / active profile),
	// archive_state coverage must NOT count toward classification or half-open
	// range inference: those hours are rotated out of live MySQL and will not be
	// fetched, so they are real gaps. Dropping archivedHours here is the single
	// authority for that rule (Plan also skips the archive_state read). Without
	// it a strict AllowGaps=false reconstruct would silently omit them.
	if noArchive {
		archivedHours = nil
	}

	// Build sets for fast lookup.
	liveSet := make(map[time.Time]bool, len(liveHours))
	for _, h := range liveHours {
		liveSet[h] = true
	}
	archiveSet := make(map[time.Time]bool, len(archivedHours))
	for _, h := range archivedHours {
		archiveSet[h] = true
	}

	// If we don't have a bounded range on both sides, infer the missing end.
	if rangeStart.IsZero() && rangeEnd.IsZero() {
		return nil
	}

	// For a half-open range, use live/archive partition boundaries to infer the other end.
	if rangeStart.IsZero() {
		if len(liveHours) > 0 {
			rangeStart = liveHours[0]
		} else if len(archivedHours) > 0 {
			rangeStart = archivedHours[0]
		} else {
			return nil
		}
	}
	if rangeEnd.IsZero() {
		if len(liveHours) > 0 {
			rangeEnd = liveHours[len(liveHours)-1].Add(time.Hour)
		} else {
			rangeEnd = time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
		}
	}

	// Enumerate hours in the range and classify each.
	var gaps []time.Time
	needMySQL := false

	for h := rangeStart; h.Before(rangeEnd); h = h.Add(time.Hour) {
		inLive := liveSet[h]
		inArchive := archiveSet[h]

		if inLive {
			needMySQL = true
		}
		if !inLive && !inArchive {
			gaps = append(gaps, h)
		}
	}

	plan := &QueryPlan{GapHours: gaps}
	for _, h := range liveHours {
		if plan.OldestKnownHour.IsZero() || h.Before(plan.OldestKnownHour) {
			plan.OldestKnownHour = h
		}
	}
	for _, h := range archivedHours {
		if plan.OldestKnownHour.IsZero() || h.Before(plan.OldestKnownHour) {
			plan.OldestKnownHour = h
		}
	}

	if needMySQL {
		plan.MySQLRanges = buildContiguousRanges(liveHours, rangeStart, rangeEnd)
	}

	return plan
}

// FormatGapWarning returns a human-readable warning string for gap hours,
// or "" if there are no gaps.
func FormatGapWarning(gaps []time.Time) string {
	if len(gaps) == 0 {
		return ""
	}
	first, last := GapRange(gaps)
	return fmt.Sprintf("query covers hours with no data (rotated and not archived): %s – %s", first, last)
}

// GapRange renders the first and last gap hour for callers that need to say
// something OTHER than FormatGapWarning's sentence about the same hours.
//
// It exists because that sentence names a cause — "rotated and not archived" —
// which is only true when the reader actually opens the archives. A reader
// that deliberately excludes them (console --no-archive, or a session data
// profile) sees the same hours reported as gaps by design, and telling that
// operator the data was never archived sends them to audit a rotation that is
// working fine.
func GapRange(gaps []time.Time) (first, last string) {
	if len(gaps) == 0 {
		return "", ""
	}
	const f = "2006-01-02 15:00"
	return gaps[0].Format(f), gaps[len(gaps)-1].Format(f)
}

// SkipMySQL returns true when the planner determined that MySQL can be skipped
// entirely (the full time range is covered by archives, with no gaps).
// Returns false for nil plans (fallback/early-return cases where the planner
// could not run), ensuring MySQL is always queried when routing is uncertain.
func (p *QueryPlan) SkipMySQL() bool {
	return p != nil && len(p.MySQLRanges) == 0 && len(p.GapHours) == 0
}

// RunPlanAndWarn runs the planner for the given DSN and time range, emitting a
// slog.Warn for any coverage gaps. This is the shared entry point used by
// both the query and recover commands. Returns nil when planning is not
// applicable or fails (callers should fall back to the default path).
//
// parseDSN is a function that extracts the database name from the DSN.
func RunPlanAndWarn(ctx context.Context, db *sql.DB, dbName string, since, until *time.Time, sourceIDs []string) *QueryPlan {
	// Callers that exclude archives (bintrail query --no-archive) skip planning
	// entirely, so this warn path is always archive-aware (noArchive=false).
	plan, err := Plan(ctx, db, dbName, since, until, false, sourceIDs)
	if err != nil {
		slog.Warn("query planner failed; coverage gaps may not be detected", "error", err)
		return nil
	}
	if plan != nil {
		if warn := FormatGapWarning(plan.GapHours); warn != "" {
			slog.Warn(warn)
		}
	}
	return plan
}

// buildContiguousRanges collapses sorted hours into contiguous TimeRanges,
// filtering to only hours within [rangeStart, rangeEnd).
func buildContiguousRanges(hours []time.Time, rangeStart, rangeEnd time.Time) []TimeRange {
	var ranges []TimeRange
	var curStart, curEnd time.Time

	for _, h := range hours {
		if h.Before(rangeStart) || !h.Before(rangeEnd) {
			continue
		}
		hEnd := h.Add(time.Hour)
		if curStart.IsZero() {
			curStart = h
			curEnd = hEnd
			continue
		}
		if h.Equal(curEnd) {
			curEnd = hEnd
		} else {
			ranges = append(ranges, TimeRange{Start: curStart, End: curEnd})
			curStart = h
			curEnd = hEnd
		}
	}
	if !curStart.IsZero() {
		ranges = append(ranges, TimeRange{Start: curStart, End: curEnd})
	}
	return ranges
}

// loadLivePartitionHours returns the sorted set of hours that have live
// partitions in MySQL (excluding p_future).
func loadLivePartitionHours(ctx context.Context, db *sql.DB, dbName string) ([]time.Time, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'
		  AND PARTITION_NAME IS NOT NULL
		  AND PARTITION_NAME != 'p_future'
		ORDER BY PARTITION_ORDINAL_POSITION`, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hours []time.Time
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if t, ok := ParsePartitionName(name); ok {
			hours = append(hours, t)
		}
	}
	return hours, rows.Err()
}

// archiveCoverage describes one archive_state row's time coverage: the hour
// parsed from its partition_name label plus the content-derived
// min/max_event_ts range of the archived rows (#1037). Min/Max are the zero
// time when unknown — rows written before the columns existed, or registered
// by upload/reconcile, which never scan row contents.
type archiveCoverage struct {
	Label    time.Time
	Min, Max time.Time
}

// maxCoverageSpan bounds how far a single archive's content range may expand
// its claimed hour coverage. A corrupt/zero-date min_event_ts would otherwise
// make expandArchiveHours enumerate millions of hours. Rows exceeding it fall
// back to label-only coverage (the pre-#1037 behavior).
const maxCoverageSpan = 20 * 366 * 24 * time.Hour

// sourceScopeClause builds the optional `WHERE bintrail_id IN (...)` that
// restricts coverage to the archives a read will open (#1232). A nil scope
// yields no clause at all — every registered archive counts, which is correct
// for a caller that reads them all. The empty-scope case never reaches here:
// callers return early, because `IN ()` is a syntax error, not an empty set.
func sourceScopeClause(sourceIDs []string) (string, []any) {
	if len(sourceIDs) == 0 {
		return "", nil
	}
	args := make([]any, len(sourceIDs))
	for i, id := range sourceIDs {
		args[i] = id
	}
	return " WHERE bintrail_id IN (?" + strings.Repeat(", ?", len(sourceIDs)-1) + ")", args
}

// isMissingTableErr reports whether err is MySQL's ER_NO_SUCH_TABLE (1146) —
// the shape an index that never archived shows, as opposed to a table that
// exists and would not read. Kept narrow to 1146 on purpose, mirroring
// status.LoadCoverage (#816): widening it re-creates the very conflation
// #1324 removes.
func isMissingTableErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1146
}

// loadArchiveCoverage returns per-row archive coverage from archive_state.
// On an index whose archive_state predates the min/max_event_ts columns
// (error 1054 — the migration only runs where EnsureSchema does, e.g. rotate),
// it falls back to label-only coverage so planning keeps working unchanged.
func loadArchiveCoverage(ctx context.Context, db *sql.DB, sourceIDs []string) ([]archiveCoverage, error) {
	if sourceIDs != nil && len(sourceIDs) == 0 {
		// Scoped to nothing: this read opens no archives, so no archive_state
		// row can describe coverage it will see. Returning early (rather than
		// building an `IN ()`, which is a syntax error) keeps that explicit.
		return nil, nil
	}
	where, args := sourceScopeClause(sourceIDs)
	rows, err := db.QueryContext(ctx, `
		SELECT partition_name, min_event_ts, max_event_ts
		FROM archive_state`+where+`
		ORDER BY partition_name`, args...)
	if err != nil {
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1054 {
			return loadArchiveCoverageLegacy(ctx, db, sourceIDs)
		}
		return nil, err
	}
	defer rows.Close()

	var cov []archiveCoverage
	for rows.Next() {
		var name string
		var minTS, maxTS sql.NullTime
		if err := rows.Scan(&name, &minTS, &maxTS); err != nil {
			return nil, err
		}
		t, ok := ParsePartitionName(name)
		if !ok {
			continue
		}
		c := archiveCoverage{Label: t}
		if minTS.Valid && maxTS.Valid {
			c.Min = minTS.Time.UTC()
			c.Max = maxTS.Time.UTC()
		}
		cov = append(cov, c)
	}
	return cov, rows.Err()
}

// loadArchiveCoverageLegacy is loadArchiveCoverage for pre-#1037 archive_state
// schemas: partition names only, no content range.
func loadArchiveCoverageLegacy(ctx context.Context, db *sql.DB, sourceIDs []string) ([]archiveCoverage, error) {
	if sourceIDs != nil && len(sourceIDs) == 0 {
		return nil, nil
	}
	where, args := sourceScopeClause(sourceIDs)
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT partition_name
		FROM archive_state`+where+`
		ORDER BY partition_name`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cov []archiveCoverage
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if t, ok := ParsePartitionName(name); ok {
			cov = append(cov, archiveCoverage{Label: t})
		}
	}
	return cov, rows.Err()
}

// expandArchiveHours converts archive coverage rows into the sorted, deduped
// set of hours the archives actually hold data for: each row's label hour,
// widened to every hour in its content range [Min, Max] when known (#1037).
// The widening is sound for gap classification: RANGE partitioning routes any
// captured event older than a partition's upper bound into the oldest live
// partition, so events inside a misfiled archive's content range have no
// other place they could live — an hour in that span with no events anywhere
// is exactly as empty as an unlabeled hour today.
func expandArchiveHours(cov []archiveCoverage) []time.Time {
	seen := make(map[time.Time]bool, len(cov))
	var hours []time.Time
	add := func(h time.Time) {
		if !seen[h] {
			seen[h] = true
			hours = append(hours, h)
		}
	}
	for _, c := range cov {
		add(c.Label)
		if c.Min.IsZero() || c.Max.IsZero() || c.Max.Before(c.Min) {
			continue
		}
		if c.Max.Sub(c.Min) > maxCoverageSpan {
			slog.Debug("archive content range implausibly wide; using label-only coverage",
				"partition_hour", c.Label, "min_event_ts", c.Min, "max_event_ts", c.Max)
			continue
		}
		for h := c.Min.Truncate(time.Hour); !h.After(c.Max); h = h.Add(time.Hour) {
			add(h)
		}
	}
	slices.SortFunc(hours, func(a, b time.Time) int { return a.Compare(b) })
	return hours
}

// misfiledHours returns the sorted, deduped hour LABELS of archives whose
// content range escapes their label hour AND overlaps [rangeStart, rangeEnd)
// (#1037). Zero rangeStart/rangeEnd mean an open bound. These are the files a
// label-pruning archive fetch would wrongly skip for this range, so callers
// forward them as Options.ExtraArchiveHours.
func misfiledHours(cov []archiveCoverage, rangeStart, rangeEnd time.Time) []time.Time {
	seen := make(map[time.Time]bool)
	var hours []time.Time
	for _, c := range cov {
		if c.Min.IsZero() || c.Max.IsZero() || c.Max.Before(c.Min) {
			continue
		}
		// Content within the label hour → the label already prunes correctly.
		if !c.Min.Before(c.Label) && c.Max.Before(c.Label.Add(time.Hour)) {
			continue
		}
		// Content range must overlap the queried range.
		if !rangeEnd.IsZero() && !c.Min.Before(rangeEnd) {
			continue
		}
		if !rangeStart.IsZero() && c.Max.Before(rangeStart) {
			continue
		}
		if !seen[c.Label] {
			seen[c.Label] = true
			hours = append(hours, c.Label)
		}
	}
	slices.SortFunc(hours, func(a, b time.Time) int { return a.Compare(b) })
	return hours
}

// MisfiledArchiveHours is the standalone form of QueryPlan.MisfiledArchiveHours
// for callers that fetch archives without running the full planner (e.g. the
// MCP query/recover tools). It reads archive_state coverage and returns the
// hour labels of misfiled archives overlapping [since, until]; nil bounds are
// open. Returns (nil, nil) when db is nil or no time bound is set (no pruning
// happens then, so nothing can be missed).
func MisfiledArchiveHours(ctx context.Context, db *sql.DB, since, until *time.Time, sourceIDs []string) ([]time.Time, error) {
	if db == nil || (since == nil && until == nil) {
		return nil, nil
	}
	cov, err := loadArchiveCoverage(ctx, db, sourceIDs)
	if err != nil {
		return nil, err
	}
	var rangeStart, rangeEnd time.Time
	if since != nil {
		rangeStart = since.Truncate(time.Hour)
	}
	if until != nil {
		rangeEnd = until.Truncate(time.Hour).Add(time.Hour)
	}
	return misfiledHours(cov, rangeStart, rangeEnd), nil
}

// ParsePartitionName converts a partition name like "p_2026021914" to the
// corresponding UTC hour. Returns false for "p_future" or malformed names.
func ParsePartitionName(name string) (time.Time, bool) {
	if len(name) != 12 || !strings.HasPrefix(name, "p_") {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("p_2006010215", name, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
