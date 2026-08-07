package cli

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// resetViewsFlags clears the package-level flag vars between subtests. The
// command's flags are package globals (the convention throughout this package),
// so a test that sets one leaks into the next without this.
func resetViewsFlags() {
	vIndexDSN, vArchiveDir, vArchiveS3, vBintrailID = "", "", "", ""
	vRegion, vBaselineDir, vBaselineS3, vOut = "", "", "", "views.sql"
	vNoBaselines = false
}

// TestRunViews_flagValidation covers the refusals that must happen BEFORE any
// connection is opened — each one names a combination that would otherwise
// produce a silently wrong file rather than an error.
func TestRunViews_flagValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		set     func()
		wantErr string
	}{
		{
			// Without the id the glob would span every server that ever archived
			// into this root, and the events view would mix their binlog
			// coordinates under one name.
			name:    "archive dir without bintrail-id",
			set:     func() { vArchiveDir = "/data/archives" },
			wantErr: "--bintrail-id is required",
		},
		{
			name:    "archive s3 without bintrail-id",
			set:     func() { vArchiveS3 = "s3://bucket/archives/" },
			wantErr: "--bintrail-id is required",
		},
		{
			name:    "both baseline sources",
			set:     func() { vIndexDSN = "u:p@tcp(h:3306)/i"; vBaselineDir = "/b"; vBaselineS3 = "s3://b/" },
			wantErr: "mutually exclusive",
		},
		{
			// Nothing to discover from and nothing named: the file would be an
			// empty shell of comments.
			name:    "no index and no explicit archives",
			set:     func() { vBaselineDir = "/b" },
			wantErr: "--index-dsn is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetViewsFlags()
			tc.set()
			err := runViews(&cobra.Command{}, nil)
			if err == nil {
				t.Fatalf("expected an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestExplicitArchiveSources pins the bintrail_id scoping for operator-named
// roots, matching what `query` does with the same pair of flags.
func TestExplicitArchiveSources(t *testing.T) {
	resetViewsFlags()
	vArchiveDir = "/data/archives"
	vArchiveS3 = "s3://bucket/archives/"
	vBintrailID = "abc-123"

	got := explicitArchiveSources()
	want := []string{
		"/data/archives/bintrail_id=abc-123",
		"s3://bucket/archives/bintrail_id=abc-123",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("source %d = %q, want %q", i, got[i], want[i])
		}
	}
	resetViewsFlags()
}

// TestViewsCmd_registered guards the wiring: a command defined but never added
// to the root is invisible, and nothing else in the build would notice.
func TestViewsCmd_registered(t *testing.T) {
	root := &cobra.Command{Use: "bintrail"}
	AddReadCommands(root)
	for _, c := range root.Commands() {
		if c.Name() == "views" {
			return
		}
	}
	t.Fatal("`views` is not registered by AddReadCommands")
}
