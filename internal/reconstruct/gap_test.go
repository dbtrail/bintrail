package reconstruct

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The three gap-warning messages WarnBaselineFirstEventGap can emit, keyed by
// a substring unique to each (#1163 split the single pre-existing message):
// the pre-#1163 position-heuristic assertion, the GTID-containment disproof,
// and the GTID-present-but-unprovable rewording. Note msgUnproven also
// contains the words "gap between baseline and first indexed event", so
// no-warn assertions must check all three substrings, not just one.
const (
	msgPosition = "gap between baseline and first indexed event — reconstruction may be incomplete"
	msgGTIDGap  = "indexed GTID coverage does not contain the baseline GTID set"
	msgUnproven = "containment could not be evaluated"
)

// TestWarnBaselineFirstEventGap exercises the #781 full-table gap warning: the
// baseline↔first-event check ported from the single-row path, and since #1163
// the GTID-containment preference over the position heuristic. It captures
// slog output and asserts which of the three gap messages (if any) fires,
// honoring the same flavor-dependent anchor/position skip cases as the
// single-row switch. GTID containment arrives pre-evaluated (the go-mysql
// parsing lives in internal/cli's gtidContainment, outside the #528-guarded
// read layer). Not parallel: it swaps the process-global default logger
// (safe only in the sequential test phase).
func TestWarnBaselineFirstEventGap(t *testing.T) {
	cases := []struct {
		name        string
		flavor      string
		indexedGTID string
		containment GTIDContainment
		bmeta       baseline.DumpMetadata
		first       query.ResultRow
		wantMsg     string // "" = none of the three gap messages may appear
	}{
		{
			// The core #781 case: baseline pos precedes the first event by a gap,
			// and no GTID evidence exists on either side (position-mode source).
			name:   "mysql same file, first event past baseline pos, no GTIDs = position gap reported",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 1000},
			first:  query.ResultRow{BinlogFile: "binlog.000042", StartPos: 5000, EventID: 7},

			wantMsg: msgPosition,
		},
		{
			name:   "mysql later first-event file, no GTIDs = position gap reported",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 99999},
			first:  query.ResultRow{BinlogFile: "binlog.000043", StartPos: 4, EventID: 8},

			wantMsg: msgPosition,
		},
		{
			name:   "mysql first event at baseline pos = no gap",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 5000},
			first:  query.ResultRow{BinlogFile: "binlog.000042", StartPos: 5000, EventID: 7},

			wantMsg: "",
		},
		{
			// Anchor absent → the check is skipped (info), never a gap report.
			name:   "mysql baseline lacks anchor = skipped, no gap reported",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{},
			first:  query.ResultRow{BinlogFile: "binlog.000042", StartPos: 5000, EventID: 7},

			wantMsg: "",
		},
		{
			// First event lacks a comparable position (#318) → skipped, not a gap.
			name:   "mysql first event NULL binlog_file = skipped, no gap reported",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 1000},
			first:  query.ResultRow{BinlogFile: "", StartPos: 0, EventID: 7},

			wantMsg: "",
		},
		{
			// PG numeric LSN compare: event past the baseline floor = gap. PG has
			// no GTID sets, so the pre-#1163 position message (which carries the
			// baseline_lsn key) is the one that must keep firing.
			name:   "postgres first event past baseline LSN = position gap reported",
			flavor: "postgres",
			bmeta:  baseline.DumpMetadata{LSN: 0x1000},
			first:  query.ResultRow{BinlogFile: "0/2000", StartPos: 0x2000, EventID: 9},

			wantMsg: msgPosition,
		},
		{
			// PG lineage forced by the LSN anchor even with an empty flavor read.
			name:   "empty flavor but baseline LSN proves PG lineage, event past floor = position gap reported",
			flavor: "",
			bmeta:  baseline.DumpMetadata{LSN: 0x9},
			first:  query.ResultRow{BinlogFile: "0/10", StartPos: 0x10, EventID: 9},

			wantMsg: msgPosition,
		},

		// ── #1163: GTID-set containment decides before the position heuristic ──
		{
			// The issue's repro: baseline at :1-39, index checkpointed at :1-2000,
			// first event 332 bytes past the baseline pos in the same file — the
			// next event, not a hole. Proven containment: stay quiet.
			name:        "proven containment = no gap despite later first-event pos",
			flavor:      "mysql",
			indexedGTID: "c36f2244-89da-11f1-80b2-0aff43e443c1:1-2000",
			containment: GTIDContained,
			bmeta: baseline.DumpMetadata{
				BinlogFile: "mysql-bin.000003", BinlogPos: 1160964,
				GTIDSet: "c36f2244-89da-11f1-80b2-0aff43e443c1:1-39",
			},
			first: query.ResultRow{BinlogFile: "mysql-bin.000003", StartPos: 1161296, EventID: 7},

			wantMsg: "",
		},
		{
			// The index's lineage never reached the baseline point: containment
			// disproven, a real gap regardless of position ordering.
			name:        "disproven containment = GTID gap reported",
			flavor:      "mysql",
			indexedGTID: "c36f2244-89da-11f1-80b2-0aff43e443c1:1-39",
			containment: GTIDNotContained,
			bmeta: baseline.DumpMetadata{
				BinlogFile: "mysql-bin.000004", BinlogPos: 500,
				GTIDSet: "c36f2244-89da-11f1-80b2-0aff43e443c1:1-50",
			},
			first: query.ResultRow{BinlogFile: "mysql-bin.000004", StartPos: 800, EventID: 7},

			wantMsg: msgGTIDGap,
		},
		{
			// Baseline carries a GTID set but containment was not evaluable
			// (e.g. no indexed coverage, or a set that failed to parse) — the
			// position heuristic fires with the reworded message.
			name:        "unknown containment with baseline GTID present = unproven warning",
			flavor:      "mysql",
			indexedGTID: "",
			containment: GTIDUnknown,
			bmeta: baseline.DumpMetadata{
				BinlogFile: "mysql-bin.000003", BinlogPos: 1000,
				GTIDSet: "c36f2244-89da-11f1-80b2-0aff43e443c1:1-39",
			},
			first: query.ResultRow{BinlogFile: "mysql-bin.000003", StartPos: 2000, EventID: 7},

			wantMsg: msgUnproven,
		},
		{
			// Unknown containment but the position heuristic finds the first
			// event AT the baseline anchor — quiet, same as pre-#1163.
			name:        "unknown containment, first event at baseline pos = no gap",
			flavor:      "mysql",
			indexedGTID: "c36f2244-89da-11f1-80b2-0aff43e443c1:1-2000",
			containment: GTIDUnknown,
			bmeta: baseline.DumpMetadata{
				BinlogFile: "mysql-bin.000003", BinlogPos: 2000,
				GTIDSet: "c36f2244-89da-11f1-80b2-0aff43e443c1:1-39",
			},
			first: query.ResultRow{BinlogFile: "mysql-bin.000003", StartPos: 2000, EventID: 7},

			wantMsg: "",
		},
	}

	allMsgs := []string{msgPosition, msgGTIDGap, msgUnproven}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
			defer slog.SetDefault(prev)

			WarnBaselineFirstEventGap(tc.flavor, tc.indexedGTID, tc.containment, tc.bmeta, tc.first, "mydb", "orders")

			out := buf.String()
			if tc.wantMsg == "" {
				for _, msg := range allMsgs {
					if strings.Contains(out, msg) {
						t.Errorf("unexpected gap message %q; log output:\n%s", msg, out)
					}
				}
				return
			}
			if !strings.Contains(out, tc.wantMsg) {
				t.Errorf("expected gap message %q; log output:\n%s", tc.wantMsg, out)
			}
			if !strings.Contains(out, "level=WARN") {
				t.Errorf("expected the gap report at WARN level; log output:\n%s", out)
			}
			for _, msg := range allMsgs {
				if msg != tc.wantMsg && strings.Contains(out, msg) {
					t.Errorf("unexpected extra gap message %q; log output:\n%s", msg, out)
				}
			}
		})
	}
}

// TestDecideBaselineGap pins the verdict logic: an evaluated GTID containment
// decides outright (proof stays quiet, disproof warns regardless of position
// ordering); GTIDUnknown degrades to the position heuristic, whose firing is
// then attributed as unproven when the baseline carried a GTID set and as the
// pre-#1163 position verdict when it did not (#1163).
func TestDecideBaselineGap(t *testing.T) {
	const (
		uuid    = "c36f2244-89da-11f1-80b2-0aff43e443c1"
		binlog3 = "mysql-bin.000003"
	)
	pastBaseline := query.ResultRow{BinlogFile: binlog3, StartPos: 2000}
	atBaseline := query.ResultRow{BinlogFile: binlog3, StartPos: 1000}
	anchored := func(gtid string) baseline.DumpMetadata {
		return baseline.DumpMetadata{BinlogFile: binlog3, BinlogPos: 1000, GTIDSet: gtid}
	}

	cases := []struct {
		name        string
		containment GTIDContainment
		flavor      string
		bmeta       baseline.DumpMetadata
		first       query.ResultRow
		want        GapVerdict
	}{
		{
			name:        "no GTIDs, first past baseline = position verdict",
			containment: GTIDUnknown, flavor: "mysql",
			bmeta: anchored(""), first: pastBaseline,
			want: GapVerdictPosition,
		},
		{
			name:        "no GTIDs, first at baseline = none",
			containment: GTIDUnknown, flavor: "mysql",
			bmeta: anchored(""), first: atBaseline,
			want: GapVerdictNone,
		},
		{
			name:        "proven containment overrides later position = none",
			containment: GTIDContained, flavor: "mysql",
			bmeta: anchored(uuid + ":1-39"), first: pastBaseline,
			want: GapVerdictNone,
		},
		{
			// Disproof wins even when position ordering looks clean — the
			// baseline reflects transactions the index never saw.
			name:        "disproven containment overrides clean position = gtid verdict",
			containment: GTIDNotContained, flavor: "mysql",
			bmeta: anchored(uuid + ":1-50"), first: atBaseline,
			want: GapVerdictGTID,
		},
		{
			name:        "unknown containment, baseline GTID present, first past baseline = unproven",
			containment: GTIDUnknown, flavor: "mysql",
			bmeta: anchored(uuid + ":1-39"), first: pastBaseline,
			want: GapVerdictUnproven,
		},
		{
			name:        "unknown containment, baseline GTID present, first at baseline = none",
			containment: GTIDUnknown, flavor: "mysql",
			bmeta: anchored(uuid + ":1-39"), first: atBaseline,
			want: GapVerdictNone,
		},
		{
			// A whitespace-only baseline set is absence, not unprovable GTID
			// evidence: the pre-#1163 position message keeps firing.
			name:        "whitespace-only baseline GTID = position verdict",
			containment: GTIDUnknown, flavor: "mysql",
			bmeta: anchored("  \n\t"), first: pastBaseline,
			want: GapVerdictPosition,
		},
		{
			// PG never has GTID sets; the numeric LSN compare decides and the
			// verdict stays the pre-#1163 position one.
			name:        "postgres LSN compare = position verdict",
			containment: GTIDUnknown, flavor: "postgres",
			bmeta: baseline.DumpMetadata{LSN: 0x1000},
			first: query.ResultRow{BinlogFile: "0/2000", StartPos: 0x2000},
			want:  GapVerdictPosition,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DecideBaselineGap(tc.containment, tc.flavor, tc.bmeta, tc.first)
			if got != tc.want {
				t.Errorf("DecideBaselineGap(%v, %q, %+v, %+v) = %v, want %v",
					tc.containment, tc.flavor, tc.bmeta, tc.first, got, tc.want)
			}
		})
	}
}

func TestGapDetected(t *testing.T) {
	cases := []struct {
		name          string
		flavor        string
		eventFile     string
		eventStartPos uint64
		baselineFile  string
		baselinePos   int64
		baselineLSN   uint64
		want          bool
	}{
		// ── MySQL two-key compare ───────────────────────────────────────────
		{
			name:      "mysql same file, event past baseline pos = gap",
			flavor:    "mysql",
			eventFile: "binlog.000042", eventStartPos: 5000,
			baselineFile: "binlog.000042", baselinePos: 1000,
			want: true,
		},
		{
			name:      "mysql same file, event at baseline pos = no gap",
			flavor:    "mysql",
			eventFile: "binlog.000042", eventStartPos: 1000,
			baselineFile: "binlog.000042", baselinePos: 1000,
			want: false,
		},
		{
			name:      "mysql later file = gap",
			flavor:    "mysql",
			eventFile: "binlog.000043", eventStartPos: 4,
			baselineFile: "binlog.000042", baselinePos: 99999,
			want: true, // later file always wins regardless of pos
		},
		{
			name:      "mysql earlier file = no gap",
			flavor:    "mysql",
			eventFile: "binlog.000041", eventStartPos: 99999,
			baselineFile: "binlog.000042", baselinePos: 4,
			want: false,
		},
		{
			name:      "mariadb uses mysql semantics",
			flavor:    "mariadb",
			eventFile: "mariadb-bin.000007", eventStartPos: 900,
			baselineFile: "mariadb-bin.000007", baselinePos: 100,
			want: true,
		},
		{
			name:      "empty flavor falls back to mysql semantics",
			flavor:    "",
			eventFile: "binlog.000042", eventStartPos: 5000,
			baselineFile: "binlog.000042", baselinePos: 1000,
			want: true,
		},
		{
			name:      "empty flavor, MySQL baselineLSN is ignored",
			flavor:    "",
			eventFile: "binlog.000041", eventStartPos: 1,
			baselineFile: "binlog.000042", baselinePos: 1000,
			baselineLSN: 999, // must not be consulted outside the postgres branch
			want:        false,
		},

		// ── PostgreSQL numeric LSN compare ──────────────────────────────────
		{
			name:      "pg event past baseline LSN = gap",
			flavor:    "postgres",
			eventFile: "0/2000", eventStartPos: 0x2000,
			baselineLSN: 0x1000,
			want:        true,
		},
		{
			name:      "pg event at baseline LSN = no gap",
			flavor:    "postgres",
			eventFile: "0/1000", eventStartPos: 0x1000,
			baselineLSN: 0x1000,
			want:        false,
		},
		{
			name:      "pg event before baseline LSN = no gap",
			flavor:    "postgres",
			eventFile: "0/800", eventStartPos: 0x800,
			baselineLSN: 0x1000,
			want:        false,
		},
		{
			// The lexical trap: "0/10" > "0/9" numerically (0x10=16 > 0x9=9)
			// but "0/10" < "0/9" lexically. A file-text compare would report
			// NO gap here; the numeric compare correctly reports one.
			name:      "pg lexical trap: 0/10 event after 0/9 baseline = gap",
			flavor:    "postgres",
			eventFile: "0/10", eventStartPos: 0x10,
			baselineFile: "0/9", baselinePos: 0, // file text present but must be ignored
			baselineLSN: 0x9,
			want:        true,
		},
		{
			// Mirror of the trap: "0/9" event vs "0/10" baseline. Lexical
			// compare on file text would FLAG a gap ("0/9" > "0/10"); the
			// numeric compare correctly reports none.
			name:      "pg lexical trap mirror: 0/9 event before 0/10 baseline = no gap",
			flavor:    "postgres",
			eventFile: "0/9", eventStartPos: 0x9,
			baselineFile: "0/10", baselinePos: 0,
			baselineLSN: 0x10,
			want:        false,
		},
		{
			// Pre-slice-A PG baseline: no LSN key → anchor unknown → never
			// flag a gap (the check gates a warning only; callers log the skip).
			name:      "pg baselineLSN zero = unknown anchor, no gap",
			flavor:    "postgres",
			eventFile: "0/2000", eventStartPos: 0x2000,
			baselineLSN: 0,
			want:        false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := GapDetected(tc.flavor, tc.eventFile, tc.eventStartPos, tc.baselineFile, tc.baselinePos, tc.baselineLSN)
			if got != tc.want {
				t.Errorf("GapDetected(%q, %q, %d, %q, %d, %d) = %v, want %v",
					tc.flavor, tc.eventFile, tc.eventStartPos, tc.baselineFile, tc.baselinePos, tc.baselineLSN, got, tc.want)
			}
		})
	}
}
