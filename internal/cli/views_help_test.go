package cli

import (
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

// TestIncludeLiveHelp_saysWhatItQueries is the flag-help half of #1483.
//
// The help described COVERAGE ("events the index holds but rotation has not
// archived yet") and stopped there. Reading Parquet off disk and opening a
// connection to the index a running bintrail captures into are very different
// operational acts, and the operator choosing this flag is the one who can
// still decide not to.
func TestIncludeLiveHelp_saysWhatItQueries(t *testing.T) {
	f := viewsCmd.Flags().Lookup("include-live")
	if f == nil {
		t.Fatal("no --include-live flag")
	}
	for _, want := range []string{
		"queries the index server directly",
		"competes with capture",
		"read replica",
	} {
		if !strings.Contains(f.Usage, want) {
			t.Errorf("--include-live help does not say %q:\n%s", want, f.Usage)
		}
	}
}

// TestViewsLongHelp_namesTheRefreshInterval is the command-help half of #1484.
//
// The Long help said to regenerate "after taking or refreshing a baseline",
// which reads as an action the operator takes. With the refresh on a timer
// nobody takes it, and nothing regenerates the file.
func TestViewsLongHelp_namesTheRefreshInterval(t *testing.T) {
	for _, want := range []string{"--baseline-refresh-interval", "regenerate"} {
		if !strings.Contains(strings.ToLower(viewsCmd.Long), strings.ToLower(want)) {
			t.Errorf("the views help does not say %q:\n%s", want, viewsCmd.Long)
		}
	}
}

// TestViewsHelp_noEmDashes: fixed copy rule, over every string this command
// puts in front of an operator.
func TestViewsHelp_noEmDashes(t *testing.T) {
	texts := map[string]string{"Short": viewsCmd.Short, "Long": viewsCmd.Long}
	viewsCmd.Flags().VisitAll(func(f *pflag.Flag) { texts["--"+f.Name] = f.Usage })
	for name, text := range texts {
		for _, dash := range []string{"\u2014", "\u2013"} {
			if strings.Contains(text, dash) {
				t.Errorf("%s contains %q:\n%s", name, dash, text)
			}
		}
	}
}
