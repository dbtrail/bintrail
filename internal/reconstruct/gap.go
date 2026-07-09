package reconstruct

import (
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// GapDetected reports whether there is a gap between a baseline's recorded
// anchor and the first indexed event available for replay: events between the
// anchor and the first event are missing from the reconstruction, so callers
// warn (the CLI's --allow-gaps semantics stay untouched — this is the
// baseline-vs-first-event check, not the query planner's coverage-gap check).
//
// flavor is the stream_state.flavor value ("mysql", "mariadb", "postgres", or
// "" when unknown — see query.SourceFlavor).
//
// PostgreSQL ("postgres"): coordinates are WAL LSNs. eventFile holds the LSN's
// TEXT form ("0/1A2B3C4"), which is NOT lexically ordered ("0/10" < "0/9"
// lexically but not numerically), so the comparison must use the numeric LSN
// that PG events carry in StartPos (internal/pgcapture rowEvent) against the
// baseline's numeric delta-replay floor (baseline.MetaKeyLSN — an INCLUSIVE
// lower bound: the slot's confirmed_flush_lsn/restart_lsn as of just before
// the snapshot, not the snapshot's own live pg_current_wal_lsn(); see that
// key's doc comment and #771). baselineLSN == 0 means the baseline predates
// the LSN floor (#593 slice A) or is not a PG baseline at all: the floor is
// UNKNOWN, so no gap is flagged. That mirrors how this path already treats
// missing coordinates — the caller's "baseline lacks position metadata"
// branch skips detection with an informational message rather than failing
// (and status's continuity verdict fails closed only under an explicit
// --fail-on-gap) — and it can never silently produce wrong data because this
// check only gates a WARNING, never the reconstruction itself. Callers should
// log the skip (cli/reconstruct.go does).
//
// The strict "eventStartPos > baselineLSN" comparison below is unchanged by
// #771: a baseline floor is a value below which no coverage gap can exist by
// construction (deltas are read from at-or-after it), so "no gap" already
// means eventStartPos <= baselineLSN — the same comparison as when this
// value was (incorrectly) the live anchor.
//
// Any other flavor (MySQL, MariaDB, ""): the established two-key binlog
// compare — file names ordered lexically (equal to numeric order for MySQL's
// zero-padded binlog names within one server's sequence, see query.BinlogPos),
// byte position breaking ties within the same file.
func GapDetected(flavor string, eventFile string, eventStartPos uint64, baselineFile string, baselinePos int64, baselineLSN uint64) bool {
	if flavor == "postgres" { // canonical literal owned by recovery.DialectForFlavor
		if baselineLSN == 0 {
			return false // anchor unknown — cannot flag a gap; caller logs the skip
		}
		return eventStartPos > baselineLSN
	}
	return eventFile > baselineFile ||
		(eventFile == baselineFile && eventStartPos > uint64(baselinePos))
}

// WarnBaselineFirstEventGap emits the baseline↔first-event gap warning for
// callers that live in this package — the full-table reconstruct path (#781),
// which previously produced a dump silently missing that gap while single-row
// reconstruct at least warned. It is a DIRECT port of the check the single-row
// CLI path runs (cli/reconstruct.go: resolveGapCheck + the GapDetected switch),
// duplicated here because that logic lives in package cli, which this package
// cannot import (cli imports reconstruct).
//
// Warn-only, identical to single-row: this check gates a WARNING, never the
// reconstruction itself, so --allow-gaps is deliberately not consulted here —
// it governs the coverage-gap fetch (query.FetchMerged) and CheckCaptureGap,
// which run separately, not this baseline-vs-first-event visibility warning.
//
// flavor is query.SourceFlavor(db). first is the earliest fetched event
// (events sorted by (event_timestamp, event_id)). schema/table are added to
// every record so the warning is attributable when several tables reconstruct
// concurrently.
func WarnBaselineFirstEventGap(flavor string, bmeta baseline.DumpMetadata, first query.ResultRow, schema, table string) {
	// Mirror of cli.resolveGapCheck: force PG semantics when the flavor read
	// came back empty but the baseline carries an LSN anchor (its lineage is
	// provably PostgreSQL — LSN text must never be compared lexically), then
	// pick the flavor-correct anchor/position presence signals.
	lineageGuard := false
	if flavor == "" && bmeta.LSN != 0 {
		flavor = "postgres"
		lineageGuard = true
	}
	anchorPresent := bmeta.BinlogFile != ""
	eventPosMissing := first.BinlogFile == ""
	if flavor == "postgres" {
		anchorPresent = bmeta.LSN != 0
		eventPosMissing = first.StartPos == 0
	}

	if lineageGuard {
		slog.Warn("source flavor unknown but baseline carries an LSN anchor — treating source as postgres for gap detection (LSN text is never compared lexically)",
			"schema", schema, "table", table, "baseline_lsn", bmeta.LSN)
	}
	switch {
	case !anchorPresent && flavor == "postgres":
		slog.Warn("gap detection unavailable — this baseline predates LSN anchoring (no bintrail.baseline_lsn metadata); a gap between the baseline and the first indexed event would go undetected",
			"schema", schema, "table", table, "flavor", flavor)
	case !anchorPresent:
		slog.Info("gap detection skipped — baseline lacks position metadata; consider re-running 'bintrail baseline' to embed position data",
			"schema", schema, "table", table, "flavor", flavor)
	case eventPosMissing:
		slog.Warn("gap detection skipped — first indexed event lacks position metadata",
			"schema", schema, "table", table,
			"event_id", first.EventID,
			"baseline_file", bmeta.BinlogFile,
			"baseline_pos", bmeta.BinlogPos,
			"baseline_lsn", bmeta.LSN,
			"flavor", flavor)
	case GapDetected(flavor, first.BinlogFile, first.StartPos, bmeta.BinlogFile, bmeta.BinlogPos, bmeta.LSN):
		slog.Warn("gap between baseline and first indexed event — reconstruction may be incomplete",
			"schema", schema, "table", table,
			"baseline_file", bmeta.BinlogFile,
			"baseline_pos", bmeta.BinlogPos,
			"baseline_gtid", bmeta.GTIDSet,
			"baseline_lsn", bmeta.LSN,
			"first_event_file", first.BinlogFile,
			"first_event_pos", first.StartPos,
			"flavor", flavor)
	}
}
