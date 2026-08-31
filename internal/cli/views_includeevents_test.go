package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// saveViewsFlags restores every package-level flag var this file touches. They
// are globals cobra binds to, so a test that sets one and does not put it back
// changes what the NEXT test observes.
func saveViewsFlags(t *testing.T) {
	t.Helper()
	strs := []*string{&vIndexDSN, &vArchiveDir, &vArchiveS3, &vBintrailID, &vBaselineDir, &vBaselineS3, &vOut}
	strVals := make([]string, len(strs))
	for i, p := range strs {
		strVals[i] = *p
	}
	bools := []*bool{&vNoBaselines, &vIncludeLive, &vIncludeEvents}
	boolVals := make([]bool, len(bools))
	for i, p := range bools {
		boolVals[i] = *p
	}
	t.Cleanup(func() {
		for i, p := range strs {
			*p = strVals[i]
		}
		for i, p := range bools {
			*p = boolVals[i]
		}
	})
}

func runViewsToString(t *testing.T) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetContext(context.Background())
	err := runViews(cmd, nil)
	return out.String(), err
}

// TestRunViews_eventsViewIsOptIn is THE assertion for #1535: the default file
// is the cheap one. Defining the events view opens one Parquet footer per
// archived file before it returns a row, so a default that includes it charges
// every reader for the whole change log to get their tables.
//
// Driven through runViews rather than views.Generate: the package-level test
// covers the renderer, and what can silently regress is the CLI forgetting to
// set the field at all.
func TestRunViews_eventsViewIsOptIn(t *testing.T) {
	saveViewsFlags(t)
	vIndexDSN, vBaselineDir, vBaselineS3 = "", "", ""
	vArchiveDir, vArchiveS3, vBintrailID = t.TempDir(), "", "aaaa"
	vNoBaselines, vIncludeLive, vIncludeEvents, vOut = true, false, true, "-"

	withEvents, err := runViewsToString(t)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(withEvents, `CREATE OR REPLACE VIEW "events"`) {
		t.Fatalf("--include-events did not produce the events view:\n%s", withEvents)
	}

	// Same layout, without the flag. --no-baselines has to go with it, or the
	// file would define nothing and the command refuses.
	vNoBaselines, vIncludeEvents = false, false
	vBaselineDir = t.TempDir()
	byDefault, err := runViewsToString(t)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(byDefault, `CREATE OR REPLACE VIEW "events"`) {
		t.Errorf("the DEFAULT file still defines the events view, so every reader pays "+
			"one Parquet footer read per archived file to get their tables:\n%s", byDefault)
	}
	if !strings.Contains(byDefault, "--include-events") {
		t.Errorf("the default file never says how to ask for the events view:\n%s", byDefault)
	}
}

// TestRunViews_refusesTheCombinationsThatDefineNothing: both refusals exist
// because the alternative is a command that succeeds and hands back something
// useless — an empty file, or a live leg with no view to hang on. Refused
// rather than quietly adjusted: turning the expensive view on because the
// operator asked for its leg is a cost they pay on every query afterwards.
func TestRunViews_refusesTheCombinationsThatDefineNothing(t *testing.T) {
	for _, tc := range []struct {
		name        string
		noBaselines bool
		includeLive bool
		wantSays    string
	}{
		{"no baselines and no events defines nothing", true, false, "leaves nothing to define"},
		{"a live leg with no view to hang on", false, true, "adds a leg to the events view"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saveViewsFlags(t)
			vIndexDSN, vBaselineDir, vBaselineS3 = "", t.TempDir(), ""
			vArchiveDir, vArchiveS3, vBintrailID = t.TempDir(), "", "aaaa"
			vNoBaselines, vIncludeLive, vIncludeEvents, vOut = tc.noBaselines, tc.includeLive, false, "-"

			out, err := runViewsToString(t)
			if err == nil {
				t.Fatalf("the command succeeded and produced:\n%s", out)
			}
			if !strings.Contains(err.Error(), tc.wantSays) {
				t.Errorf("the refusal does not say why: %v", err)
			}
			// A refusal that does not name the way out is a dead end.
			if !strings.Contains(err.Error(), "--include-events") {
				t.Errorf("the refusal never names the flag that fixes it: %v", err)
			}
		})
	}

	// The positive control: with --include-events both combinations are legal,
	// so the refusals above are about the missing view and not about the flags
	// themselves.
	t.Run("both are fine with the events view", func(t *testing.T) {
		saveViewsFlags(t)
		vIndexDSN, vBaselineDir, vBaselineS3 = "", "", ""
		vArchiveDir, vArchiveS3, vBintrailID = t.TempDir(), "", "aaaa"
		vNoBaselines, vIncludeLive, vIncludeEvents, vOut = true, false, true, "-"
		if _, err := runViewsToString(t); err != nil {
			t.Errorf("--no-baselines --include-events was refused: %v", err)
		}
	})
}
