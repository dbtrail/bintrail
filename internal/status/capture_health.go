// Package status — this file builds the operator-facing account of a DEGRADED
// capture verdict (#1296). It is shared on purpose: `bintrail status` prints
// these lines and the console renders the same strings from
// /api/status (stream.capture_health.explanation), so the two surfaces cannot
// drift into telling one operator a different story than the other.
//
// What the old single sentence got wrong, and what every line here defends:
//
//   - It read as operator error. A table appearing on the source (an
//     application or a plugin creating one) is ordinary operation, not
//     misconfiguration, and the text must not imply otherwise.
//   - It named a remedy with no location. "Take a fresh snapshot" said nothing
//     about WHICH table, WHERE the button is, or that a schema snapshot is a
//     different artifact from a baseline.
//   - It implied the remedy closed the hole. It does not: a fresh snapshot
//     fixes capture from that point on, and the already-skipped events stay
//     missing unless the source's binlogs still cover the window.
//   - It sent everyone to `bintrail snapshot`, including the operators for whom
//     that can never converge (a validation-excluded table is excluded again by
//     every future snapshot — #1051/#1199).
package status

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// Capture-skip reason keys, mirroring the parser's persisted vocabulary. Kept
// as independent decls for the same reason CaptureSkipStat is: this display
// package deliberately does not import the binlog parser.
const (
	CaptureSkipReasonTableNotInSnapshot        = "table_not_in_snapshot"
	CaptureSkipReasonTableExcludedFromSnapshot = "table_excluded_from_snapshot"
	CaptureSkipReasonColumnCountMismatch       = "column_count_mismatch"
	CaptureSkipReasonNoResolver                = "no_resolver"
)

// ExplainCaptureSkips renders the DEGRADED verdict's explanation: one
// cause/remedy pair per active reason (most events first), then the two lines
// that apply to every reason — what a remedy does NOT recover, and where the
// per-event detail lives. Returns nil when nothing was skipped, so a caller can
// use a nil result as "there is nothing to explain".
//
// Pure and fixture-drivable: no clock, no IO. The caller supplies the decoded
// ledger; every claim here is derived from it.
func ExplainCaptureSkips(skips map[string]CaptureSkipStat) []string {
	active := activeReasons(skips)
	if len(active) == 0 {
		return nil
	}
	var lines []string
	for _, reason := range active {
		lines = append(lines, causeLine(reason, skips[reason]))
		lines = append(lines, remedyLine(reason))
	}
	// The scope caveat is unconditional and deliberately blunt: without it an
	// operator applies the remedy, sees capture go green, and assumes the hole
	// closed. It never did — the remedy is forward-only.
	lines = append(lines, "None of this recovers what was already skipped: those changes are absent "+
		"from the index for good unless the source still has the binlogs covering that window, in "+
		"which case `bintrail index --binlog-dir <dir> --files <file>` can re-read them.")
	// Say that the verdict itself persists. The tallies are monotonic and
	// re-seeded across restarts precisely so a skip episode cannot be laundered
	// away by a restart — which means a WORKING fix does not turn this banner
	// green, and an operator who does not know that concludes the fix failed and
	// applies it again.
	lines = append(lines, "This warning does not clear on its own: the tally counts skips that happened, "+
		"not skips still happening, so it stays after a successful fix. Confirm the fix by watching the "+
		"count stop rising; to reset the tally, stop capture for this source and clear "+
		"stream_state.capture_skips in its index.")
	lines = append(lines, logLine(active[0]))
	return lines
}

// activeReasons lists the reasons with a non-zero count, most events first
// (ties alphabetical) — the same ordering captureSkipReasons uses, so the
// summary line and this explanation agree on which reason dominates.
func activeReasons(skips map[string]CaptureSkipStat) []string {
	var reasons []string
	for r, st := range skips {
		if st.Count > 0 {
			reasons = append(reasons, r)
		}
	}
	sort.Slice(reasons, func(i, j int) bool {
		if skips[reasons[i]].Count != skips[reasons[j]].Count {
			return skips[reasons[i]].Count > skips[reasons[j]].Count
		}
		return reasons[i] < reasons[j]
	})
	return reasons
}

// causeLine states what happened for one reason, naming the tables when the
// ledger recorded them.
func causeLine(reason string, st CaptureSkipStat) string {
	subject := namedTables(st)
	switch reason {
	case CaptureSkipReasonTableNotInSnapshot:
		return subject + " changed on the source but " + isAre(st) + " missing from the schema snapshot " +
			"capture decodes against, so those row events could not be indexed. A table appearing on the " +
			"source is ordinary — an application or plugin creating one is not a misconfiguration. What is " +
			"unusual is that capture did not follow it: the stream takes its own snapshot when it sees the " +
			"CREATE/ALTER, so this points at that path not running — the DDL happened while capture was " +
			"stopped, or the automatic snapshot failed, or the table falls outside this stream's " +
			"schema/table filter."
	case CaptureSkipReasonTableExcludedFromSnapshot:
		detail := ""
		if st.LastDetail != "" {
			detail = " (" + st.LastDetail + ")"
		}
		return subject + " " + isAre(st) + " left out of the schema snapshot ON PURPOSE by snapshot " +
			"validation" + detail + ". bintrail addresses rows by primary key on an InnoDB table; without " +
			"one it cannot index or reverse a change, so those row events are dropped."
	case CaptureSkipReasonColumnCountMismatch:
		return subject + " " + hasHave(st) + " a different number of columns in the binlog than in the " +
			"schema snapshot, so values would map to the wrong column names. Capture drops the rows rather " +
			"than index them under wrong names — the snapshot is behind a schema change on the source."
	case CaptureSkipReasonNoResolver:
		return "Capture ran with no schema snapshot loaded at all, so no row event could be decoded."
	case CaptureSkipReasonStatementFormatDML:
		return "The source wrote these changes as STATEMENT/MIXED-format events, which carry no row images. " +
			"There is nothing in the binlog to index — this is a source configuration, not a bintrail fault."
	case CaptureSkipReasonUnreadablePreviousLedger:
		return "The capture ledger persisted by a previous run could not be parsed, so an earlier tally of " +
			"skipped events may have been lost. This entry preserves the fact; it is not itself a skipped event."
	default:
		return subject + " had row events read from the stream and dropped under \"" + reason + "\"."
	}
}

// remedyLine gives the action for one reason. The schema-snapshot remedies say
// what a schema snapshot IS: the console's other button creates a BASELINE (a
// full copy of the data), and an operator who confuses the two runs a dump they
// did not need while capture stays broken.
func remedyLine(reason string) string {
	switch reason {
	case CaptureSkipReasonTableNotInSnapshot, CaptureSkipReasonColumnCountMismatch, CaptureSkipReasonNoResolver:
		return "Fix: refresh the schema snapshot for this source — the record of each table's columns that " +
			"capture decodes against (not a baseline, which is a full copy of the data). In the console: " +
			"Overview → \"Refresh schema snapshot\". On the command line: `bintrail snapshot --source-dsn " +
			"<source> --index-dsn <index>`, then restart the stream so it picks the new snapshot up."
	case CaptureSkipReasonTableExcludedFromSnapshot:
		return "Fix: give each table listed above an explicit PRIMARY KEY on an InnoDB engine at the source. " +
			"Re-snapshotting is NOT the fix here — validation excludes these tables again every time, so a " +
			"fresh snapshot would leave capture exactly as it is now."
	case CaptureSkipReasonStatementFormatDML:
		return "Fix: set binlog_format=ROW server-wide on the source (a session-level override can also " +
			"produce row-less events)."
	case CaptureSkipReasonUnreadablePreviousLedger:
		return "Fix: with the capture daemon stopped, clear stream_state.capture_skips to acknowledge the " +
			"lost tally; leaving it makes every later status report stay non-clean."
	default:
		return "Fix: see the capture daemon's log for this reason's detail."
	}
}

// logLine names the log to read and the exact line to look for. "check the
// capture log" named neither a location nor a string to grep, which is what
// made it useless; anything replacing it must name both.
func logLine(reason string) string {
	needle := map[string]string{
		CaptureSkipReasonTableNotInSnapshot:        "table not in snapshot — skipping",
		CaptureSkipReasonTableExcludedFromSnapshot: "table not in snapshot — skipping",
		CaptureSkipReasonColumnCountMismatch:       "column count mismatch — skipping",
		CaptureSkipReasonNoResolver:                "no resolver available — skipping",
	}[reason]
	s := "Per-event detail is in the log of the process capturing this source — `bintrail stream` or " +
		"`bintrail-console watch` (with the bundled compose file: `docker compose logs bintrail`)"
	if needle != "" {
		s += ", on the lines reading \"" + needle + "\""
	}
	return s + "."
}

// namedTables renders the ledger's table names as the subject of a sentence, or
// a neutral subject when the ledger has none. A ledger written before per-table
// attribution has an EMPTY list, which must never render as "no tables" — the
// tables exist, this index just cannot name them.
func namedTables(st CaptureSkipStat) string {
	if len(st.Tables) == 0 {
		return "Rows of one or more tables (this index's ledger predates per-table attribution, so it cannot name them)"
	}
	names := strings.Join(st.Tables, ", ")
	if st.TablesTruncated {
		names += " and others"
	}
	return names
}

// isAre / hasHave keep the sentences grammatical for one table vs several, and
// for the unnamed-tables subject (always plural).
func isAre(st CaptureSkipStat) string {
	if len(st.Tables) == 1 {
		return "is"
	}
	return "are"
}

func hasHave(st CaptureSkipStat) string {
	if len(st.Tables) == 1 {
		return "has"
	}
	return "have"
}

// wrapAt breaks text into lines of at most width runes at word boundaries, for
// the fixed-width text status output. A word longer than width is emitted on
// its own line rather than split — breaking a table name or a command in half
// would make it uncopyable, which is worse than an over-long line.
func wrapAt(text string, width int) []string {
	var (
		out  []string
		line strings.Builder
	)
	for _, word := range strings.Fields(text) {
		switch {
		case line.Len() == 0:
			line.WriteString(word)
		case line.Len()+1+len(word) <= width:
			line.WriteString(" ")
			line.WriteString(word)
		default:
			out = append(out, line.String())
			line.Reset()
			line.WriteString(word)
		}
	}
	if line.Len() > 0 {
		out = append(out, line.String())
	}
	return out
}

// writeCaptureSkipExplanation prints the explanation into the text status
// output, indented and wrapped to the width the rest of the report uses.
func writeCaptureSkipExplanation(w io.Writer, skips map[string]CaptureSkipStat) {
	for _, para := range ExplainCaptureSkips(skips) {
		for _, line := range wrapAt(para, 76) {
			fmt.Fprintf(w, "  %s\n", line)
		}
	}
}
