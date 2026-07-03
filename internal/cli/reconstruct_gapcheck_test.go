package cli

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func TestResolveGapCheck(t *testing.T) {
	cases := []struct {
		name          string
		flavor        string
		bmeta         baseline.DumpMetadata
		firstFile     string
		firstStartPos uint64

		wantFlavor          string
		wantLineageGuard    bool
		wantAnchorPresent   bool
		wantEventPosMissing bool
	}{
		{
			name:   "mysql with binlog anchor and positioned event",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 12345},
			firstFile: "binlog.000042", firstStartPos: 20000,
			wantFlavor: "mysql", wantAnchorPresent: true, wantEventPosMissing: false,
		},
		{
			name:   "mysql missing anchor",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{},
			firstFile: "binlog.000042", firstStartPos: 20000,
			wantFlavor: "mysql", wantAnchorPresent: false, wantEventPosMissing: false,
		},
		{
			name:   "mysql NULL binlog_file on first event (#318)",
			flavor: "mysql",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 12345},
			firstFile: "", firstStartPos: 0,
			wantFlavor: "mysql", wantAnchorPresent: true, wantEventPosMissing: true,
		},
		{
			name:   "empty flavor, no LSN = mysql semantics (no guard)",
			flavor: "",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 12345},
			firstFile: "binlog.000042", firstStartPos: 20000,
			wantFlavor: "", wantAnchorPresent: true, wantEventPosMissing: false,
		},
		{
			// The Slice-C trap the guard exists for: flavor read failed but the
			// baseline carries an LSN anchor — its lineage is provably PG, so PG
			// semantics are forced and the lexical compare on LSN text is
			// structurally unreachable (even if a future producer also fills
			// BinlogFile with LSN text).
			name:   "empty flavor but baseline LSN proves PG lineage = guard fires, postgres semantics",
			flavor: "",
			bmeta:  baseline.DumpMetadata{BinlogFile: "0/9", LSN: 0x9},
			firstFile: "0/10", firstStartPos: 0x10,
			wantFlavor: "postgres", wantLineageGuard: true, wantAnchorPresent: true, wantEventPosMissing: false,
		},
		{
			name:   "postgres with LSN anchor and positioned event",
			flavor: "postgres",
			bmeta:  baseline.DumpMetadata{LSN: 0x1000},
			firstFile: "0/2000", firstStartPos: 0x2000,
			wantFlavor: "postgres", wantAnchorPresent: true, wantEventPosMissing: false,
		},
		{
			// Pre-slice-C PG baseline: no LSN key. Anchor absent even when the
			// MySQL-shaped BinlogFile field is populated — PG must never anchor
			// on file text.
			name:   "postgres without LSN = anchor absent regardless of BinlogFile",
			flavor: "postgres",
			bmeta:  baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 12345},
			firstFile: "0/2000", firstStartPos: 0x2000,
			wantFlavor: "postgres", wantAnchorPresent: false, wantEventPosMissing: false,
		},
		{
			// 0 is not a valid WAL position: a PG event with zero StartPos has
			// no comparable position even when its file text is non-empty.
			name:   "postgres zero StartPos on first event = position missing",
			flavor: "postgres",
			bmeta:  baseline.DumpMetadata{LSN: 0x1000},
			firstFile: "0/2000", firstStartPos: 0,
			wantFlavor: "postgres", wantAnchorPresent: true, wantEventPosMissing: true,
		},
		{
			name:   "mariadb keeps mysql semantics and ignores a stray LSN",
			flavor: "mariadb",
			bmeta:  baseline.DumpMetadata{BinlogFile: "mariadb-bin.000007", BinlogPos: 100, LSN: 42},
			firstFile: "mariadb-bin.000007", firstStartPos: 900,
			wantFlavor: "mariadb", wantAnchorPresent: true, wantEventPosMissing: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flavor, guard, anchor, posMissing := resolveGapCheck(tc.flavor, tc.bmeta, tc.firstFile, tc.firstStartPos)
			if flavor != tc.wantFlavor {
				t.Errorf("effectiveFlavor = %q, want %q", flavor, tc.wantFlavor)
			}
			if guard != tc.wantLineageGuard {
				t.Errorf("lineageGuard = %v, want %v", guard, tc.wantLineageGuard)
			}
			if anchor != tc.wantAnchorPresent {
				t.Errorf("anchorPresent = %v, want %v", anchor, tc.wantAnchorPresent)
			}
			if posMissing != tc.wantEventPosMissing {
				t.Errorf("eventPosMissing = %v, want %v", posMissing, tc.wantEventPosMissing)
			}
		})
	}
}
