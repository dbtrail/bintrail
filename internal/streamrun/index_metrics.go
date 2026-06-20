package streamrun

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
)

// defaultIndexMetricsInterval is how often the bintrail_index_* gauges are
// refreshed from a status snapshot when no interval is configured (#351).
const defaultIndexMetricsInterval = 60 * time.Second

// startIndexMetricsScraper launches a goroutine that periodically publishes the
// bintrail_index_* gauges (recovery floor, gap hours, storage bytes, partition
// counts) for source, derived from a status snapshot of the index DB. It
// returns immediately and stops when ctx is cancelled. A scrape failure is
// logged and the previous gauge values are left in place — a slightly stale
// reading beats a gap or a misleading zero. The work is a handful of
// information_schema/aggregate queries on a timer, not a per-event path.
func startIndexMetricsScraper(ctx context.Context, db *sql.DB, indexDSN, source string, intervalSeconds int) {
	interval := time.Duration(intervalSeconds) * time.Second
	if interval <= 0 {
		interval = defaultIndexMetricsInterval
	}
	dbName := ""
	if c, err := drivermysql.ParseDSN(indexDSN); err == nil {
		dbName = c.DBName
	} else {
		slog.Warn("index metrics scraper: could not parse index DSN for schema name; metrics disabled", "error", err)
		return
	}
	m := observe.IndexForSource(source)
	go func() {
		// Scrape once promptly so the gauges populate without waiting a full
		// interval, then on the ticker.
		scrapeIndexMetrics(ctx, db, dbName, m)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				scrapeIndexMetrics(ctx, db, dbName, m)
			}
		}
	}()
}

func scrapeIndexMetrics(ctx context.Context, db *sql.DB, dbName string, m *observe.IndexMetrics) {
	data, err := status.CollectStatus(ctx, db, dbName)
	if err != nil {
		slog.Warn("index metrics scrape: could not collect status", "error", err)
		return
	}

	snap := observe.IndexSnapshot{}
	if data.Coverage != nil {
		if data.Coverage.EarliestEvent.Valid {
			snap.OldestEvent = data.Coverage.EarliestEvent.Time
		}
		if data.Coverage.LatestEvent.Valid {
			snap.NewestEvent = data.Coverage.LatestEvent.Time
		}
		snap.Events = data.Coverage.TotalEvents
		snap.MySQLBytes = data.Coverage.IndexSizeBytes
	}
	for _, p := range data.Parts {
		if p.Name == "p_future" {
			snap.FuturePartitions++
		} else {
			snap.ActivePartitions++
		}
	}
	if data.Archives != nil {
		snap.ParquetBytes = data.Archives.TotalSizeBytes
	}

	// Gap hours: hours rotated out of MySQL with no Parquet archive — holes in
	// recovery coverage. query.Plan SHORT-CIRCUITS to a nil plan when given a
	// nil range (planner.go), so we must pass a concrete window — and it has to
	// reach back to the earliest data that SHOULD exist (live or archived) for
	// the count to mean anything. An empty index has no span and no gaps.
	var archiveEarliest sql.NullTime
	if data.Coverage != nil {
		archiveEarliest = data.Coverage.ArchiveEarliestHour
	}
	if since, until, ok := gapScrapeRange(snap.OldestEvent, archiveEarliest, time.Now()); ok {
		if plan, err := query.Plan(ctx, db, dbName, &since, &until); err != nil {
			slog.Warn("index metrics scrape: could not plan gap hours", "error", err)
		} else if plan != nil { // belt-and-suspenders: Plan can still return nil
			snap.GapHours = len(plan.GapHours)
		}
	}

	m.Set(snap, time.Now())
}

// gapScrapeRange returns the [since, until] window over which to count coverage
// gap hours, or ok=false when the index has no data to span. since reaches back
// to the earliest data that should exist — the earlier of the oldest live event
// and the earliest archived hour — so a hole anywhere in the covered span is
// seen. Returning a concrete (non-nil) range is also what keeps the scraper
// from calling query.Plan with nil bounds, which it short-circuits to a nil
// plan (the cause of an earlier nil-deref panic).
func gapScrapeRange(oldest time.Time, archiveEarliest sql.NullTime, now time.Time) (since, until time.Time, ok bool) {
	since = oldest
	if archiveEarliest.Valid && (since.IsZero() || archiveEarliest.Time.Before(since)) {
		since = archiveEarliest.Time
	}
	if since.IsZero() {
		return time.Time{}, time.Time{}, false
	}
	return since, now, true
}
