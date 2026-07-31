package reconstruct

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The one gap message WarnBaselineFirstEventGap can emit since the #1163
// redesign, keyed by a substring unique to it. The pre-#1163 assertive
// per-table message is gone: a per-table first event past the anchor is the
// EXPECTED healthy shape, so it can never carry an assertion by itself; the
// verdict is decided against the index's earliest surviving event and the
// unprovable case is worded as exactly that.
const msgUnproven = "cannot be proven"

// TestDecideBaselineGap pins the #1163 decision matrix: the per-table first
// event alone never proves a gap; the index's earliest surviving event either
// proves coverage (quiet) or leaves it unproven (hedged warning). The
// discriminating regression case is the START-SEEDED one: capture began AFTER
// the baseline (oldest indexed event past the anchor) — the GTID-containment
// design this replaces silenced it, because stream_state.gtid_set is seeded
// with the stream's start set and therefore "contains" the baseline's set in
// that exact scenario.
func TestDecideBaselineGap(t *testing.T) {
	// Coordinates from the #1163 report's healthy repro: baseline at
	// (mysql-bin.000003, 1160964), first per-table event 332 bytes later.
	bmeta := baseline.DumpMetadata{BinlogFile: "mysql-bin.000003", BinlogPos: 1160964}
	firstPast := query.ResultRow{BinlogFile: "mysql-bin.000003", StartPos: 1161296}

	cases := []struct {
		name    string
		flavor  string
		bmeta   baseline.DumpMetadata
		first   query.ResultRow
		start   query.IndexStart
		startOK bool
		want    GapVerdict
	}{
		{
			// The #1163 healthy repro: stream started before the baseline, so
			// the oldest surviving event precedes the anchor — proven, quiet,
			// even though this table's first event sits past the anchor.
			name:    "healthy: oldest indexed event precedes the anchor = quiet",
			flavor:  "mysql",
			bmeta:   bmeta,
			first:   firstPast,
			start:   query.IndexStart{BinlogFile: "mysql-bin.000001", StartPos: 4},
			startOK: true,
			want:    GapVerdictNone,
		},
		{
			// THE regression case: capture started after the baseline (#781).
			// Oldest surviving event is past the anchor → coverage unprovable
			// → warn. The replaced GTID-containment design silenced this.
			name:    "capture started after the baseline = unproven, warns",
			flavor:  "mysql",
			bmeta:   bmeta,
			first:   firstPast,
			start:   query.IndexStart{BinlogFile: "mysql-bin.000003", StartPos: 1161296},
			startOK: true,
			want:    GapVerdictUnproven,
		},
		{
			name:    "first event at the anchor = quiet regardless of index start",
			flavor:  "mysql",
			bmeta:   bmeta,
			first:   query.ResultRow{BinlogFile: "mysql-bin.000003", StartPos: 1160964},
			start:   query.IndexStart{BinlogFile: "mysql-bin.000004", StartPos: 4},
			startOK: true,
			want:    GapVerdictNone,
		},
		{
			name:    "oldest event unavailable (empty table / read failed) = unproven",
			flavor:  "mysql",
			bmeta:   bmeta,
			first:   firstPast,
			startOK: false,
			want:    GapVerdictUnproven,
		},
		{
			// #318 shape: a positionless oldest row must never silently read
			// as "at-or-before the anchor" (empty string sorts before every
			// file name lexically — that would be a free false proof).
			name:    "positionless oldest event is not a proof = unproven",
			flavor:  "mysql",
			bmeta:   bmeta,
			first:   firstPast,
			start:   query.IndexStart{BinlogFile: "", StartPos: 0},
			startOK: true,
			want:    GapVerdictUnproven,
		},
		{
			name:    "mariadb: same semantics, oldest precedes anchor = quiet",
			flavor:  "mariadb",
			bmeta:   baseline.DumpMetadata{BinlogFile: "mariadb-bin.000213", BinlogPos: 11149},
			first:   query.ResultRow{BinlogFile: "mariadb-bin.000214", StartPos: 400},
			start:   query.IndexStart{BinlogFile: "mariadb-bin.000210", StartPos: 4},
			startOK: true,
			want:    GapVerdictNone,
		},
		{
			// PostgreSQL compares numeric LSNs in StartPos; a start LSN
			// at-or-below the baseline floor proves coverage.
			name:    "postgres: oldest LSN at the baseline floor = quiet",
			flavor:  "postgres",
			bmeta:   baseline.DumpMetadata{LSN: 5000},
			first:   query.ResultRow{BinlogFile: "0/1A2B", StartPos: 9000},
			start:   query.IndexStart{BinlogFile: "0/1388", StartPos: 5000},
			startOK: true,
			want:    GapVerdictNone,
		},
		{
			name:    "postgres: oldest LSN past the floor = unproven",
			flavor:  "postgres",
			bmeta:   baseline.DumpMetadata{LSN: 5000},
			first:   query.ResultRow{BinlogFile: "0/1A2B", StartPos: 9000},
			start:   query.IndexStart{BinlogFile: "0/1B00", StartPos: 6912},
			startOK: true,
			want:    GapVerdictUnproven,
		},
		{
			// A zero start LSN is the PG positionless shape — not a proof.
			name:    "postgres: zero oldest LSN is not a proof = unproven",
			flavor:  "postgres",
			bmeta:   baseline.DumpMetadata{LSN: 5000},
			first:   query.ResultRow{BinlogFile: "0/1A2B", StartPos: 9000},
			start:   query.IndexStart{StartPos: 0},
			startOK: true,
			want:    GapVerdictUnproven,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DecideBaselineGap(tc.flavor, tc.bmeta, tc.first, tc.start, tc.startOK)
			if got != tc.want {
				t.Errorf("DecideBaselineGap = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestWarnBaselineFirstEventGap exercises the #781 full-table gap warning: the
// baseline↔first-event check ported from the single-row path, with the #1163
// earliest-surviving-event proof. It captures slog output and asserts whether
// the hedged gap message fires, honoring the same flavor-dependent
// anchor/position skip cases as the single-row switch. Not parallel: it swaps
// the process-global default logger (safe only in the sequential test phase).
func TestWarnBaselineFirstEventGap(t *testing.T) {
	bmeta := baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 1000}
	firstPast := query.ResultRow{BinlogFile: "binlog.000042", StartPos: 5000, EventID: 7}
	startBefore := query.IndexStart{BinlogFile: "binlog.000041", StartPos: 4}
	startAfter := query.IndexStart{BinlogFile: "binlog.000042", StartPos: 5000}

	cases := []struct {
		name     string
		flavor   string
		bmeta    baseline.DumpMetadata
		first    query.ResultRow
		start    query.IndexStart
		startOK  bool
		wantWarn bool
	}{
		{
			name:     "proven coverage stays quiet",
			flavor:   "mysql",
			bmeta:    bmeta,
			first:    firstPast,
			start:    startBefore,
			startOK:  true,
			wantWarn: false,
		},
		{
			name:     "unprovable coverage warns hedged",
			flavor:   "mysql",
			bmeta:    bmeta,
			first:    firstPast,
			start:    startAfter,
			startOK:  true,
			wantWarn: true,
		},
		{
			name:     "no index-start evidence warns hedged",
			flavor:   "mysql",
			bmeta:    bmeta,
			first:    firstPast,
			startOK:  false,
			wantWarn: true,
		},
		{
			// Anchor absent → the check is skipped (info), never a gap report.
			name:     "baseline lacks anchor = skipped",
			flavor:   "mysql",
			bmeta:    baseline.DumpMetadata{},
			first:    firstPast,
			startOK:  false,
			wantWarn: false,
		},
		{
			// First event lacks a comparable position (#318) → skipped.
			name:     "first event positionless = skipped",
			flavor:   "mysql",
			bmeta:    bmeta,
			first:    query.ResultRow{EventID: 9},
			start:    startAfter,
			startOK:  true,
			wantWarn: false,
		},
		{
			name:     "postgres proven by LSN floor stays quiet",
			flavor:   "postgres",
			bmeta:    baseline.DumpMetadata{LSN: 5000},
			first:    query.ResultRow{BinlogFile: "0/1A2B", StartPos: 9000, EventID: 3},
			start:    query.IndexStart{BinlogFile: "0/0FA0", StartPos: 4000},
			startOK:  true,
			wantWarn: false,
		},
		{
			name:     "postgres unprovable warns hedged",
			flavor:   "postgres",
			bmeta:    baseline.DumpMetadata{LSN: 5000},
			first:    query.ResultRow{BinlogFile: "0/1A2B", StartPos: 9000, EventID: 3},
			start:    query.IndexStart{BinlogFile: "0/1B00", StartPos: 6912},
			startOK:  true,
			wantWarn: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
			defer slog.SetDefault(prev)

			WarnBaselineFirstEventGap(tc.flavor, tc.bmeta, tc.first, tc.start, tc.startOK, "s", "t")

			got := strings.Contains(buf.String(), msgUnproven)
			if got != tc.wantWarn {
				t.Errorf("gap warning emitted = %v, want %v; log output:\n%s", got, tc.wantWarn, buf.String())
			}
		})
	}
}
