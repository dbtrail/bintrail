package reconstruct

import (
	"log/slog"
	"strings"

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

// GTIDContainment is the three-state answer to "does the indexed GTID
// coverage (stream_state.gtid_set) contain the baseline's GTID set?" —
// consulted by DecideBaselineGap before the position heuristic (#1163), and
// "not evaluable" must stay distinct from "disproven" so it can fall back
// instead of warning.
//
// The evaluation itself is INJECTED (GTIDContainmentFunc): parsing GTID sets
// takes go-mysql, which the #528 depguard bans from this read-layer package,
// so the go-mysql-backed evaluator lives in the command layer
// (internal/cli's gtidContainment) and reaches the full-table path through
// FullTableConfig.GTIDContainment.
type GTIDContainment int

const (
	// GTIDUnknown means containment could not be evaluated: a set missing on
	// either side, a parse failure, a flavor without GTID-set semantics, or
	// no evaluator injected. Callers fall back to the position heuristic.
	GTIDUnknown GTIDContainment = iota
	// GTIDContained means both sets parsed and the indexed coverage contains
	// the baseline's set — the index covers everything from the baseline
	// point onward.
	GTIDContained
	// GTIDNotContained means both sets parsed and containment FAILED —
	// transactions the baseline reflects never entered the index's lineage.
	GTIDNotContained
)

// GTIDContainmentFunc evaluates GTIDContainment for a (flavor, baseline set,
// indexed set) triple. Implementations must be conservative: any input they
// cannot parse maps to GTIDUnknown, never a panic or an error — the result
// only gates a warning on the recovery path.
type GTIDContainmentFunc func(flavor, baselineGTID, indexedGTID string) GTIDContainment

// GapVerdict classifies the baseline↔first-event gap decision once both sides
// carry a comparable position anchor (the skip cases — missing baseline
// anchor, missing event position — are decided before this, see
// WarnBaselineFirstEventGap and the single-row switch in cli/reconstruct.go).
// Produced by DecideBaselineGap; each emitter maps a verdict to its log line.
type GapVerdict int

const (
	// GapVerdictNone means stay quiet: either GTID-set containment proved the
	// indexed coverage includes everything the baseline holds (#1163), or no
	// GTID containment was evaluable and the position heuristic found the
	// first event at-or-before the baseline anchor.
	GapVerdictNone GapVerdict = iota
	// GapVerdictGTID means GTID-set containment was disproven: the indexed
	// GTID coverage does not contain the baseline's set, so transactions the
	// baseline reflects never entered the index's lineage — a real gap,
	// regardless of what position ordering says.
	GapVerdictGTID
	// GapVerdictPosition is the pre-#1163 heuristic verdict: the baseline
	// carries no GTID set, and the first event sits strictly past the
	// baseline anchor (file/pos, or the numeric LSN for PostgreSQL).
	GapVerdictPosition
	// GapVerdictUnproven means the baseline carries a GTID set but containment
	// could not be evaluated (GTIDUnknown) and the position heuristic fired.
	// The warning for this verdict says what could NOT be proven instead of
	// asserting incompleteness: position ordering alone cannot distinguish
	// the next event from a missing one (#1163).
	GapVerdictUnproven
)

// DecideBaselineGap decides the baseline↔first-event gap question, preferring
// GTID-set containment over the position heuristic when it was evaluable
// (#1163). On a healthy GTID run the first indexed event after the baseline
// is EXPECTED to sit at a later position — the position compare cannot tell
// that apart from a hole and cries wolf on every run — while set containment
// can prove coverage: it is the same model verify's indexCovers uses, where
// stream_state.gtid_set containing a snapshot's @@gtid_executed means the
// index has indexed everything that snapshot reflects.
//
// containment is the (injected) GTID evaluation for this baseline↔index
// pair; GTIDUnknown degrades to the position heuristic rather than erroring —
// this decision only gates a warning, and the recovery path must never fail
// on a GTID string it cannot parse. Sources with no GTID on either side keep
// the position heuristic unchanged.
func DecideBaselineGap(containment GTIDContainment, flavor string, bmeta baseline.DumpMetadata, first query.ResultRow) GapVerdict {
	switch containment {
	case GTIDContained:
		return GapVerdictNone
	case GTIDNotContained:
		return GapVerdictGTID
	}
	if !GapDetected(flavor, first.BinlogFile, first.StartPos, bmeta.BinlogFile, bmeta.BinlogPos, bmeta.LSN) {
		return GapVerdictNone
	}
	if strings.TrimSpace(bmeta.GTIDSet) != "" {
		return GapVerdictUnproven
	}
	return GapVerdictPosition
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
// flavor is query.SourceFlavor(db); indexedGTID is query.StreamGTIDSet(db) —
// the index's checkpointed GTID coverage — and containment their (injected)
// GTID-set evaluation against bmeta.GTIDSet, consulted before the position
// heuristic (DecideBaselineGap, #1163). first is the earliest fetched event
// (events sorted by (event_timestamp, event_id)). schema/table are added to
// every record so the warning is attributable when several tables reconstruct
// concurrently.
func WarnBaselineFirstEventGap(flavor, indexedGTID string, containment GTIDContainment, bmeta baseline.DumpMetadata, first query.ResultRow, schema, table string) {
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
	default:
		switch DecideBaselineGap(containment, flavor, bmeta, first) {
		case GapVerdictGTID:
			slog.Warn("gap between baseline and indexed events — the indexed GTID coverage does not contain the baseline GTID set; reconstruction may be incomplete",
				"schema", schema, "table", table,
				"baseline_gtid", bmeta.GTIDSet,
				"indexed_gtid", indexedGTID,
				"baseline_file", bmeta.BinlogFile,
				"baseline_pos", bmeta.BinlogPos,
				"first_event_file", first.BinlogFile,
				"first_event_pos", first.StartPos,
				"flavor", flavor)
		case GapVerdictUnproven:
			slog.Warn("possible gap between baseline and first indexed event — baseline↔index GTID containment could not be evaluated, and position ordering alone cannot distinguish the next event from a missing one",
				"schema", schema, "table", table,
				"baseline_gtid", bmeta.GTIDSet,
				"indexed_gtid", indexedGTID,
				"baseline_file", bmeta.BinlogFile,
				"baseline_pos", bmeta.BinlogPos,
				"first_event_file", first.BinlogFile,
				"first_event_pos", first.StartPos,
				"flavor", flavor)
		case GapVerdictPosition:
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
}
