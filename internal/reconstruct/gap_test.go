package reconstruct

import "testing"

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
