package reconstruct

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

const gapReportedMsg = "gap between baseline and first indexed event"

// TestWarnBaselineFirstEventGap exercises the #781 full-table gap warning: the
// baseline↔first-event check ported from the single-row path. It captures slog
// output and asserts the "gap between baseline and first indexed event" warning
// fires exactly when GapDetected would, honoring the same flavor-dependent
// anchor/position skip cases as the single-row switch. Not parallel: it swaps
// the process-global default logger (safe only in the sequential test phase).
func TestWarnBaselineFirstEventGap(t *testing.T) {
	cases := []struct {
		name            string
		flavor          string
		bmeta           baseline.DumpMetadata
		first           query.ResultRow
		wantGapReported bool
	}{
		{
			// The core #781 case: baseline pos precedes the first event by a gap.
			name:   "mysql same file, first event past baseline pos = gap reported",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 1000},
			first:  query.ResultRow{BinlogFile: "binlog.000042", StartPos: 5000, EventID: 7},

			wantGapReported: true,
		},
		{
			name:   "mysql later first-event file = gap reported",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 99999},
			first:  query.ResultRow{BinlogFile: "binlog.000043", StartPos: 4, EventID: 8},

			wantGapReported: true,
		},
		{
			name:   "mysql first event at baseline pos = no gap",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 5000},
			first:  query.ResultRow{BinlogFile: "binlog.000042", StartPos: 5000, EventID: 7},

			wantGapReported: false,
		},
		{
			// Anchor absent → the check is skipped (info), never a gap report.
			name:   "mysql baseline lacks anchor = skipped, no gap reported",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{},
			first:  query.ResultRow{BinlogFile: "binlog.000042", StartPos: 5000, EventID: 7},

			wantGapReported: false,
		},
		{
			// First event lacks a comparable position (#318) → skipped, not a gap.
			name:   "mysql first event NULL binlog_file = skipped, no gap reported",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 1000},
			first:  query.ResultRow{BinlogFile: "", StartPos: 0, EventID: 7},

			wantGapReported: false,
		},
		{
			// PG numeric LSN compare: event past the baseline floor = gap.
			name:   "postgres first event past baseline LSN = gap reported",
			flavor: "postgres",
			bmeta:  baseline.DumpMetadata{LSN: 0x1000},
			first:  query.ResultRow{BinlogFile: "0/2000", StartPos: 0x2000, EventID: 9},

			wantGapReported: true,
		},
		{
			// PG lineage forced by the LSN anchor even with an empty flavor read.
			name:   "empty flavor but baseline LSN proves PG lineage, event past floor = gap reported",
			flavor: "",
			bmeta:  baseline.DumpMetadata{LSN: 0x9},
			first:  query.ResultRow{BinlogFile: "0/10", StartPos: 0x10, EventID: 9},

			wantGapReported: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
			defer slog.SetDefault(prev)

			WarnBaselineFirstEventGap(tc.flavor, tc.bmeta, tc.first, "mydb", "orders")

			out := buf.String()
			reported := strings.Contains(out, gapReportedMsg)
			if reported != tc.wantGapReported {
				t.Errorf("gap reported = %v, want %v; log output:\n%s", reported, tc.wantGapReported, out)
			}
			if tc.wantGapReported && !strings.Contains(out, "level=WARN") {
				t.Errorf("expected the gap report at WARN level; log output:\n%s", out)
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
			name:   "mysql same file, event past baseline pos = gap",
			flavor: "mysql",
			eventFile: "binlog.000042", eventStartPos: 5000,
			baselineFile: "binlog.000042", baselinePos: 1000,
			want: true,
		},
		{
			name:   "mysql same file, event at baseline pos = no gap",
			flavor: "mysql",
			eventFile: "binlog.000042", eventStartPos: 1000,
			baselineFile: "binlog.000042", baselinePos: 1000,
			want: false,
		},
		{
			name:   "mysql later file = gap",
			flavor: "mysql",
			eventFile: "binlog.000043", eventStartPos: 4,
			baselineFile: "binlog.000042", baselinePos: 99999,
			want: true, // later file always wins regardless of pos
		},
		{
			name:   "mysql earlier file = no gap",
			flavor: "mysql",
			eventFile: "binlog.000041", eventStartPos: 99999,
			baselineFile: "binlog.000042", baselinePos: 4,
			want: false,
		},
		{
			name:   "mariadb uses mysql semantics",
			flavor: "mariadb",
			eventFile: "mariadb-bin.000007", eventStartPos: 900,
			baselineFile: "mariadb-bin.000007", baselinePos: 100,
			want: true,
		},
		{
			name:   "empty flavor falls back to mysql semantics",
			flavor: "",
			eventFile: "binlog.000042", eventStartPos: 5000,
			baselineFile: "binlog.000042", baselinePos: 1000,
			want: true,
		},
		{
			name:   "empty flavor, MySQL baselineLSN is ignored",
			flavor: "",
			eventFile: "binlog.000041", eventStartPos: 1,
			baselineFile: "binlog.000042", baselinePos: 1000,
			baselineLSN: 999, // must not be consulted outside the postgres branch
			want:        false,
		},

		// ── PostgreSQL numeric LSN compare ──────────────────────────────────
		{
			name:   "pg event past baseline LSN = gap",
			flavor: "postgres",
			eventFile: "0/2000", eventStartPos: 0x2000,
			baselineLSN: 0x1000,
			want:        true,
		},
		{
			name:   "pg event at baseline LSN = no gap",
			flavor: "postgres",
			eventFile: "0/1000", eventStartPos: 0x1000,
			baselineLSN: 0x1000,
			want:        false,
		},
		{
			name:   "pg event before baseline LSN = no gap",
			flavor: "postgres",
			eventFile: "0/800", eventStartPos: 0x800,
			baselineLSN: 0x1000,
			want:        false,
		},
		{
			// The lexical trap: "0/10" > "0/9" numerically (0x10=16 > 0x9=9)
			// but "0/10" < "0/9" lexically. A file-text compare would report
			// NO gap here; the numeric compare correctly reports one.
			name:   "pg lexical trap: 0/10 event after 0/9 baseline = gap",
			flavor: "postgres",
			eventFile: "0/10", eventStartPos: 0x10,
			baselineFile: "0/9", baselinePos: 0, // file text present but must be ignored
			baselineLSN: 0x9,
			want:        true,
		},
		{
			// Mirror of the trap: "0/9" event vs "0/10" baseline. Lexical
			// compare on file text would FLAG a gap ("0/9" > "0/10"); the
			// numeric compare correctly reports none.
			name:   "pg lexical trap mirror: 0/9 event before 0/10 baseline = no gap",
			flavor: "postgres",
			eventFile: "0/9", eventStartPos: 0x9,
			baselineFile: "0/10", baselinePos: 0,
			baselineLSN: 0x10,
			want:        false,
		},
		{
			// Pre-slice-A PG baseline: no LSN key → anchor unknown → never
			// flag a gap (the check gates a warning only; callers log the skip).
			name:   "pg baselineLSN zero = unknown anchor, no gap",
			flavor: "postgres",
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
