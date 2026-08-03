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
	// index, no archives). Unknown is never "ok" — the same fail-closed rule
	// as continuity's "unknown" verdict.
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

// OldestDeltaFromDB computes the delta-coverage floor: the live-partition
// floor extended backwards by contiguous archives. It is the ONLY floor
// implementation — the CLI, console, and watcher all use it (CoverageInfo's
// ArchiveEarliestHour is a best-effort DISPLAY figure whose errors are
// swallowed, which a verdict must never be built on). Error semantics are
// strict in the anti-cry-wolf direction: any failure that could make the
// floor read LATER than reality — and so fabricate "broken" on healthy
// archives — is returned (the caller degrades to unknown), never swallowed.
// Only a missing archive_state table (older indexes) is tolerated.
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

	var minPartition, maxPartition sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT MIN(partition_name), MAX(partition_name) FROM archive_state`).Scan(&minPartition, &maxPartition); err != nil {
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1146 {
			return floor, nil // archive_state absent on older indexes — live floor only
		}
		return time.Time{}, fmt.Errorf("read archive floor: %w", err)
	}
	if minPartition.Valid {
		minT, ok := parsePartitionName(minPartition.String)
		if !ok {
			// Our own naming scheme failing to parse is drift, and silently
			// dropping the archive floor would fabricate "broken".
			return time.Time{}, fmt.Errorf("archive_state MIN(partition_name) %q is unparseable", minPartition.String)
		}
		maxT, ok := parsePartitionName(maxPartition.String)
		if !ok {
			return time.Time{}, fmt.Errorf("archive_state MAX(partition_name) %q is unparseable", maxPartition.String)
		}
		// Archives extend the floor backwards ONLY when their range reaches
		// the live partitions: if the newest archived hour ends before the
		// oldest live partition begins (archiving stopped, middle range
		// pruned), every restore anchored before the live floor crosses that
		// hole — so the live floor IS the coverage floor, and extending it
		// would grade those baselines with an unearned "ok". Interior holes
		// within the archive range are still invisible here; reconstruct's
		// planner gap check catches those at restore time.
		contiguous := floor.IsZero() || !maxT.Add(time.Hour).Before(floor)
		if contiguous && (floor.IsZero() || minT.Before(floor)) {
			floor = minT
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
