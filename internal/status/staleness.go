package status

import (
	"context"
	"database/sql"
	"time"
)

// Baseline staleness (#1193): full-table reconstruct = newest usable baseline
// + deltas since it. Deltas are bounded by rotation retention plus archives;
// when a baseline's anchor slides toward — or past — the oldest available
// delta, the restore window is shrinking or already broken, and the operator
// must learn that from status/console/alerts NOW, not at restore time.
type BaselineStalenessVerdict string

const (
	BaselineOK     BaselineStalenessVerdict = "ok"
	BaselineAging  BaselineStalenessVerdict = "aging"
	BaselineBroken BaselineStalenessVerdict = "broken"
	// BaselineUnknown = no evaluable delta floor (empty index, no archives).
	// Unknown is never "ok" — the same rule every other verdict here follows.
	BaselineUnknown BaselineStalenessVerdict = "unknown"
)

// baselineAgingFraction: a baseline older than this fraction of the delta
// coverage span is "aging" — the restore window is shrinking, and continued
// rotation will eventually break it.
const baselineAgingFraction = 0.8

// BaselineStalenessFor grades one snapshot anchor against the oldest instant
// deltas are available from.
func BaselineStalenessFor(snapshotTime, oldestDelta, now time.Time) BaselineStalenessVerdict {
	if snapshotTime.IsZero() || oldestDelta.IsZero() {
		return BaselineUnknown
	}
	if snapshotTime.Before(oldestDelta) {
		return BaselineBroken
	}
	span := now.Sub(oldestDelta)
	if span <= 0 {
		return BaselineOK
	}
	if float64(now.Sub(snapshotTime)) >= baselineAgingFraction*float64(span) {
		return BaselineAging
	}
	return BaselineOK
}

// OldestDelta is the earliest instant deltas are available from — archived
// partitions extend live coverage backwards. Zero = unknown.
func (c *CoverageInfo) OldestDelta() time.Time {
	if c == nil {
		return time.Time{}
	}
	var out time.Time
	if c.EarliestEvent.Valid {
		out = c.EarliestEvent.Time
	}
	if c.ArchiveEarliestHour.Valid && (out.IsZero() || c.ArchiveEarliestHour.Time.Before(out)) {
		out = c.ArchiveEarliestHour.Time
	}
	return out
}

// OldestDeltaFromDB is the lean two-query sibling of LoadCoverage for callers
// (the console baselines API, the watch staleness check) that need only the
// floor. A missing archive_state table (older indexes) degrades to live
// coverage alone, mirroring LoadCoverage's tolerance.
func OldestDeltaFromDB(ctx context.Context, db *sql.DB) (time.Time, error) {
	var c CoverageInfo
	if err := db.QueryRowContext(ctx, `SELECT MIN(event_timestamp) FROM binlog_events`).Scan(&c.EarliestEvent); err != nil {
		return time.Time{}, err
	}
	var minPartition sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT MIN(partition_name) FROM archive_state`).Scan(&minPartition); err == nil && minPartition.Valid {
		if t, ok := parsePartitionName(minPartition.String); ok {
			c.ArchiveEarliestHour = sql.NullTime{Time: t, Valid: true}
		}
	}
	return c.OldestDelta(), nil
}

// AnnotateBaselineStaleness stamps each entry's verdict in place. Every entry
// is graded on its own anchor — a superseded old snapshot showing "broken" is
// honest (it IS unusable); what decides the headline is
// OverallBaselineStaleness, which only looks at each table's newest snapshot.
func AnnotateBaselineStaleness(baselines []BaselineInfo, oldestDelta, now time.Time) {
	for i := range baselines {
		baselines[i].Staleness = BaselineStalenessFor(baselines[i].SnapshotTime, oldestDelta, now)
	}
}

// OverallBaselineStaleness is the worst verdict across each table's NEWEST
// snapshot. "" when the list is empty or unannotated.
func OverallBaselineStaleness(baselines []BaselineInfo) BaselineStalenessVerdict {
	rank := map[BaselineStalenessVerdict]int{BaselineOK: 1, BaselineUnknown: 2, BaselineAging: 3, BaselineBroken: 4}
	newest := make(map[string]BaselineInfo, len(baselines))
	for _, b := range baselines {
		k := b.Database + "." + b.Table
		if cur, ok := newest[k]; !ok || cur.SnapshotTime.Before(b.SnapshotTime) {
			newest[k] = b
		}
	}
	var out BaselineStalenessVerdict
	for _, b := range newest {
		if rank[b.Staleness] > rank[out] {
			out = b.Staleness
		}
	}
	return out
}
