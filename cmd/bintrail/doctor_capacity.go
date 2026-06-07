package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
)

// The capacity check measures the index's recent write rate from
// information_schema partition statistics and projects the steady-state live
// footprint over the configured retention window — the docs/capacity.md
// formula with live numbers instead of estimates. Disk-full on the index is
// a forensic-gap event (the stream stalls; events past the source's binlog
// retention are permanently lost), so the projection runs in `doctor` and in
// the `up` preflight (#419).

// Measurement floor and thresholds. Vars, not consts, so tests can shrink
// them (same precedent as the monitor supervisor's health thresholds).
var (
	// capMinSampleHours / capMinSampleRows: minimum recent history before a
	// write-rate measurement is trustworthy; below them the check SKIPs.
	capMinSampleHours = 3
	capMinSampleRows  = uint64(1000)
)

// capWarnFraction: WARN when the projection exceeds this fraction of the
// measured free space; FAIL when it exceeds the free space outright.
const capWarnFraction = 0.7

// capPartitionSample is one named hourly partition's measured footprint.
// Rows and bytes come from information_schema — InnoDB ESTIMATES, good for
// capacity planning, not exact (docs/capacity.md).
type capPartitionSample struct {
	hour  time.Time
	rows  uint64
	bytes uint64
}

// capacityProjection is projectCapacity's measured outcome.
type capacityProjection struct {
	eventsPerDay   float64
	bytesPerEvent  float64
	projectedBytes float64 // steady-state live size over the retain window; 0 when retain == 0
	currentBytes   uint64  // binlog_events' total footprint right now
	sampleHours    int     // recent non-empty completed hours backing the measurement
}

// projectCapacity measures the write rate from the last 24 COMPLETED hourly
// partitions (the current partial hour would understate the rate) and
// projects the steady-state size over retain. ok=false when there is not
// enough recent history to measure.
func projectCapacity(parts []capPartitionSample, retain time.Duration, now time.Time) (capacityProjection, bool) {
	var p capacityProjection
	currentHour := now.UTC().Truncate(time.Hour)
	windowStart := currentHour.Add(-24 * time.Hour)
	var rows, bytes uint64
	for _, s := range parts {
		p.currentBytes += s.bytes
		if s.rows == 0 || s.hour.Before(windowStart) || !s.hour.Before(currentHour) {
			continue
		}
		rows += s.rows
		bytes += s.bytes
		p.sampleHours++
	}
	if p.sampleHours < capMinSampleHours || rows < capMinSampleRows {
		return p, false
	}
	p.eventsPerDay = float64(rows) / float64(p.sampleHours) * 24
	p.bytesPerEvent = float64(bytes) / float64(rows)
	if retain > 0 {
		p.projectedBytes = p.eventsPerDay * p.bytesPerEvent * retain.Hours() / 24
	}
	return p, true
}

// capacityVerdict turns a measurement into the check outcome:
//   - retain == 0 (no rotation configured): WARN — the index grows unbounded
//     at the measured rate, with days-until-full when free space is known.
//   - projection >= free space: FAIL with the emergency-rotate remediation.
//   - projection >= capWarnFraction of free space: WARN.
//   - otherwise PASS; when free space is not measurable from this host the
//     PASS detail carries the projection and the headroom guidance instead.
func capacityVerdict(p capacityProjection, retain time.Duration, free uint64, freeKnown bool) checkResult {
	const name = "Index disk capacity"
	growthPerDay := p.eventsPerDay * p.bytesPerEvent

	if retain == 0 {
		detail := fmt.Sprintf("no retention window configured — the index grows unbounded at ~%s/day measured (%.0f events/day × %s/event); current size %s",
			humanBytes(growthPerDay), p.eventsPerDay, humanBytes(p.bytesPerEvent), humanBytes(float64(p.currentBytes)))
		if freeKnown && growthPerDay > 0 {
			detail += fmt.Sprintf("; ~%.0f days until the volume fills (%s free)", float64(free)/growthPerDay, humanBytes(float64(free)))
		}
		return checkResult{
			Name:   name,
			Status: statusWarn,
			Detail: detail,
			Remediation: "Configure rotation so the live index stays bounded: `bintrail up` rotates by default (--rotate-retain 30d),\n" +
				"or schedule `bintrail rotate --retain <window>` (archive to Parquet first with --archive-dir to keep history cheaply).\n" +
				"Sizing math: docs/capacity.md",
		}
	}

	projected := fmt.Sprintf("projected steady-state %s over the %s retention window (measured %.0f events/day × %s/event); current size %s",
		humanBytes(p.projectedBytes), retain, p.eventsPerDay, humanBytes(p.bytesPerEvent), humanBytes(float64(p.currentBytes)))

	if !freeKnown {
		return checkResult{
			Name:   name,
			Status: statusPass,
			Detail: projected + " — free space not measurable from this host; ensure the index volume keeps >=30% headroom above the projection (docs/capacity.md)",
		}
	}

	switch {
	case p.projectedBytes >= float64(free):
		return checkResult{
			Name:   name,
			Status: statusFail,
			Detail: fmt.Sprintf("%s EXCEEDS the %s free on the index volume — the disk will fill and stall the stream (a permanent forensic gap once the source purges its binlogs)", projected, humanBytes(float64(free))),
			Remediation: "Free space now: shorten retention and rotate immediately —\n" +
				"  bintrail rotate --index-dsn \"...\" --retain 7d --no-replace   # DROP PARTITION reclaims space instantly\n" +
				"(archive first with --archive-dir to keep the history). Then grow the volume or lower --rotate-retain.\n" +
				"Emergency recipe: docs/deployment.md §12; sizing math: docs/capacity.md",
		}
	case p.projectedBytes >= capWarnFraction*float64(free):
		return checkResult{
			Name:   name,
			Status: statusWarn,
			Detail: fmt.Sprintf("%s is over %.0f%% of the %s free on the index volume — little headroom for growth spikes", projected, capWarnFraction*100, humanBytes(float64(free))),
			Remediation: "Grow the index volume or shorten the retention window (--rotate-retain / `bintrail rotate --retain`).\n" +
				"Sizing math: docs/capacity.md",
		}
	default:
		return checkResult{
			Name:   name,
			Status: statusPass,
			Detail: fmt.Sprintf("%s; %s free", projected, humanBytes(float64(free))),
		}
	}
}

// checkIndexCapacity runs the capacity projection against the index server.
// retain is the configured rotation window (0 = no rotation / unknown). It
// connects server-level (the index database may not exist yet on first run).
func checkIndexCapacity(ctx context.Context, dsn, dbName string, retain time.Duration) checkResult {
	const name = "Index disk capacity"
	db, err := connectWithoutDB(dsn)
	if err != nil {
		return checkResult{Name: name, Status: statusSkip, Detail: "cannot connect to the index server: " + err.Error()}
	}
	defer db.Close()

	samples, err := loadPartitionSamples(ctx, db, dbName)
	if err != nil {
		return checkResult{Name: name, Status: statusSkip, Detail: "cannot read partition statistics: " + err.Error()}
	}
	proj, ok := projectCapacity(samples, retain, time.Now())
	if !ok {
		return checkResult{
			Name:   name,
			Status: statusSkip,
			Detail: fmt.Sprintf("not enough recent history to measure a write rate (%d sampled hours, need >=%d with >=%d rows) — re-run after a few hours of streaming",
				proj.sampleHours, capMinSampleHours, capMinSampleRows),
		}
	}
	free, freeKnown := indexDatadirFree(ctx, db, dsn)
	return capacityVerdict(proj, retain, free, freeKnown)
}

// loadPartitionSamples reads per-partition row/size estimates for
// binlog_events. A missing table simply yields no rows (the projection then
// SKIPs with the not-enough-history message).
func loadPartitionSamples(ctx context.Context, db *sql.DB, dbName string) ([]capPartitionSample, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, IFNULL(TABLE_ROWS, 0), IFNULL(DATA_LENGTH + INDEX_LENGTH, 0)
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND PARTITION_NAME IS NOT NULL`,
		dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var samples []capPartitionSample
	for rows.Next() {
		var name string
		var s capPartitionSample
		if err := rows.Scan(&name, &s.rows, &s.bytes); err != nil {
			return nil, err
		}
		d, ok := partitionDate(name)
		if !ok {
			continue // p_future and unrecognised names carry no hour
		}
		s.hour = d
		samples = append(samples, s)
	}
	return samples, rows.Err()
}

// indexDatadirFree probes the datadir's free space — ONLY when the index DSN
// points at this same host (loopback or unix socket). A remote MySQL's
// datadir path could coincidentally exist on this host's filesystem and
// produce a confidently wrong number; better to report "not measurable".
func indexDatadirFree(ctx context.Context, db *sql.DB, dsn string) (uint64, bool) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || !dsnTargetsLocalhost(cfg) {
		return 0, false
	}
	var varName, datadir string
	if err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'datadir'").Scan(&varName, &datadir); err != nil {
		return 0, false
	}
	if fi, err := os.Stat(datadir); err != nil || !fi.IsDir() {
		return 0, false
	}
	free, err := diskFree(datadir)
	if err != nil {
		return 0, false
	}
	return free, true
}

// dsnTargetsLocalhost reports whether the DSN targets this host: a unix
// socket or a loopback TCP address.
func dsnTargetsLocalhost(cfg *mysql.Config) bool {
	if cfg.Net == "unix" {
		return true
	}
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		host = cfg.Addr
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// humanBytes renders a byte count for check details (1024-based, one decimal).
func humanBytes(b float64) string {
	const unit = 1024.0
	if b < unit {
		return fmt.Sprintf("%.0f B", b)
	}
	suffixes := []string{"KB", "MB", "GB", "TB", "PB"}
	v := b
	for _, s := range suffixes {
		v /= unit
		if v < unit {
			return fmt.Sprintf("%.1f %s", v, s)
		}
	}
	return fmt.Sprintf("%.1f EB", v/unit)
}
