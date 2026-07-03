package reconstruct

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
// baseline's numeric anchor (baseline.MetaKeyLSN). baselineLSN == 0 means the
// baseline predates the LSN anchor (#593 slice A) or is not a PG baseline at
// all: the anchor is UNKNOWN, so no gap is flagged. That mirrors how this
// path already treats missing coordinates — the caller's "baseline lacks
// position metadata" branch skips detection with an informational message
// rather than failing (and status's continuity verdict fails closed only
// under an explicit --fail-on-gap) — and it can never silently produce wrong
// data because this check only gates a WARNING, never the reconstruction
// itself. Callers should log the skip (cli/reconstruct.go does).
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
