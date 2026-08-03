package status

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
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
	// BaselineUnknown = no evaluable delta floor (unpartitioned/unreachable
	// index, no archives). Unknown is never "ok" — the same rule every other
	// verdict here follows.
	BaselineUnknown BaselineStalenessVerdict = "unknown"
)

// baselineAgingFraction: a baseline older than this fraction of the delta
// coverage span is "aging" — the restore window is shrinking.
//
// Known bootstrap artifact: until rotation saturates retention, the span is
// the install's age, so a young install reads "aging" within hours of its
// first baseline. That is why "aging" stays an informational verdict on
// status/console and is deliberately NOT wired to the webhook channel — an
// alert that fires on every fresh install would be cried-wolf into a mute
// before it ever mattered.
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

// OldestLivePartitionHour is the live half of the delta floor: the hour of
// the oldest non-future binlog_events partition. Partition EXISTENCE is
// coverage — the planner's own rule. MIN(event_timestamp) must NOT be used
// here: it is the first WRITE, not the coverage start, and on a quiet
// database it would fabricate a "broken" verdict (baseline taken before the
// first write) on a perfectly restorable index.
func OldestLivePartitionHour(parts []PartitionStat) time.Time {
	var out time.Time
	for _, p := range parts {
		t, ok := parsePartitionName(p.Name)
		if !ok {
			continue // p_future / malformed
		}
		if out.IsZero() || t.Before(out) {
			out = t
		}
	}
	return out
}

// DeltaFloor combines the live-partition floor with the archive floor —
// archives extend coverage backwards. Zero = unknown.
func DeltaFloor(parts []PartitionStat, cov *CoverageInfo) time.Time {
	out := OldestLivePartitionHour(parts)
	if cov != nil && cov.ArchiveEarliestHour.Valid &&
		(out.IsZero() || cov.ArchiveEarliestHour.Time.Before(out)) {
		out = cov.ArchiveEarliestHour.Time
	}
	return out
}

// OldestDeltaFromDB computes the same floor for callers without a collected
// StatusData (the console baselines API, the watch staleness check). Error
// semantics are strict in the anti-cry-wolf direction: any failure that could
// make the floor read LATER than reality — and so fabricate "broken" on
// healthy archives — is returned (the caller degrades to unknown), never
// swallowed. Only a missing archive_state table (older indexes) is tolerated.
func OldestDeltaFromDB(ctx context.Context, db *sql.DB, dbName string) (time.Time, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND PARTITION_NAME IS NOT NULL`, dbName)
	if err != nil {
		return time.Time{}, fmt.Errorf("list live partitions: %w", err)
	}
	defer rows.Close()
	var parts []PartitionStat
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return time.Time{}, err
		}
		parts = append(parts, PartitionStat{Name: name})
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, err
	}
	floor := OldestLivePartitionHour(parts)

	var minPartition sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT MIN(partition_name) FROM archive_state`).Scan(&minPartition); err != nil {
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1146 {
			return floor, nil // archive_state absent on older indexes — live floor only
		}
		return time.Time{}, fmt.Errorf("read archive floor: %w", err)
	}
	if minPartition.Valid {
		t, ok := parsePartitionName(minPartition.String)
		if !ok {
			// Our own naming scheme failing to parse is drift, and silently
			// dropping the archive floor would fabricate "broken".
			return time.Time{}, fmt.Errorf("archive_state MIN(partition_name) %q is unparseable", minPartition.String)
		}
		if floor.IsZero() || t.Before(floor) {
			floor = t
		}
	}
	return floor, nil
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
