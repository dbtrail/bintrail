package query

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// TimeRange represents a contiguous UTC time range at hour granularity.
type TimeRange struct {
	Start time.Time // inclusive, truncated to hour
	End   time.Time // exclusive, truncated to hour
}

// ArchiveSource pairs archive source paths with the time range they cover.
type ArchiveSource struct {
	Source    string
	Range    TimeRange
}

// QueryPlan describes how to route a query across live MySQL partitions and
// Parquet archives. It is produced by Plan().
type QueryPlan struct {
	// MySQLRanges are time ranges that should be queried against live MySQL.
	// Empty when the entire range is covered by archives.
	MySQLRanges []TimeRange

	// ArchiveSources are the Parquet archive source paths to query.
	// Empty when the entire range is covered by live partitions.
	ArchiveSources []string

	// GapHours are hours where data has been rotated out of MySQL and no
	// archive exists. Callers should emit a warning for these.
	GapHours []time.Time
}

// Plan builds a QueryPlan for the given time range by inspecting live partition
// boundaries and archive_state coverage. When since or until is nil, that bound
// is left open (no routing optimisation is applied for the open side).
//
// dbName is the MySQL database name (needed for information_schema queries).
func Plan(ctx context.Context, db *sql.DB, dbName string, since, until *time.Time) (*QueryPlan, error) {
	if db == nil || dbName == "" {
		return &QueryPlan{}, nil
	}

	// If no time range is specified, we can't do partition-level routing.
	if since == nil && until == nil {
		return &QueryPlan{}, nil
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
		slog.Warn("could not load partition info for planning", "error", err)
		return &QueryPlan{}, nil
	}

	// Load archived partition names.
	archivedHours, err := loadArchivedHours(ctx, db)
	if err != nil {
		slog.Warn("could not load archive coverage for planning", "error", err)
		return &QueryPlan{}, nil
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

	// If we don't have a bounded range on both sides, we can still detect
	// gaps within the bounded portion, but we need at least one bound to
	// enumerate hours. For a fully open range, return empty plan (no routing).
	if rangeStart.IsZero() && rangeEnd.IsZero() {
		return &QueryPlan{}, nil
	}

	// For a half-open range, use live partition boundaries to infer the other end.
	if rangeStart.IsZero() {
		// Use the earliest live partition as the start.
		if len(liveHours) > 0 {
			rangeStart = liveHours[0]
		} else if len(archivedHours) > 0 {
			rangeStart = archivedHours[0]
		} else {
			return &QueryPlan{}, nil
		}
	}
	if rangeEnd.IsZero() {
		// Use the latest live partition + 1h as the end.
		if len(liveHours) > 0 {
			rangeEnd = liveHours[len(liveHours)-1].Add(time.Hour)
		} else {
			rangeEnd = time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
		}
	}

	// Enumerate hours in the range and classify each.
	var gaps []time.Time
	needMySQL := false
	needArchive := false

	for h := rangeStart; h.Before(rangeEnd); h = h.Add(time.Hour) {
		inLive := liveSet[h]
		inArchive := archiveSet[h]

		if inLive {
			needMySQL = true
		}
		if inArchive {
			needArchive = true
		}
		if !inLive && !inArchive {
			gaps = append(gaps, h)
		}
	}

	plan := &QueryPlan{GapHours: gaps}

	if needMySQL {
		// Build contiguous MySQL ranges from live hours within the query range.
		plan.MySQLRanges = buildContiguousRanges(liveHours, rangeStart, rangeEnd)
	}

	if needArchive {
		// We still return all archive sources — the caller resolves actual paths.
		// The planner's value here is knowing archives ARE needed.
	}

	return plan, nil
}

// FormatGapWarning returns a human-readable warning string for gap hours,
// or "" if there are no gaps.
func FormatGapWarning(gaps []time.Time) string {
	if len(gaps) == 0 {
		return ""
	}
	first := gaps[0]
	last := gaps[len(gaps)-1]
	return fmt.Sprintf("query covers hours with no data (rotated and not archived): %s – %s",
		first.Format("2006-01-02 15:00"),
		last.Format("2006-01-02 15:00"))
}

// NeedArchive returns true when the plan indicates archive sources should be queried.
func (p *QueryPlan) NeedArchive() bool {
	return p != nil && len(p.MySQLRanges) == 0 && len(p.GapHours) == 0
}

// SkipMySQL returns true when the plan indicates MySQL can be skipped entirely
// (the full time range is covered by archives).
func (p *QueryPlan) SkipMySQL() bool {
	return p != nil && len(p.MySQLRanges) == 0 && len(p.GapHours) == 0
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

// loadArchivedHours returns the sorted set of hours that have been archived
// to Parquet (from archive_state partition names).
func loadArchivedHours(ctx context.Context, db *sql.DB) ([]time.Time, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT partition_name
		FROM archive_state
		ORDER BY partition_name`)
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
