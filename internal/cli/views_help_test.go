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

// TestViewsLongHelp_saysTheStateViewsFollow replaces the #1484 guard, whose
// premise this command has now outgrown.
//
// That guard required the Long help to name --baseline-refresh-interval,
// because a timer-published snapshot left the file behind and the operator had
// to regenerate on the same schedule. The state views now reach a later
// snapshot on their own from either root shape (#1547 local, #1550 S3), so
// repeating that advice would not merely be redundant, it would send an
// operator to do work the product already does. What still needs saying is
// which half does NOT follow, and how to opt out of the half that does.
func TestViewsLongHelp_saysTheStateViewsFollow(t *testing.T) {
	long := viewsCmd.Long
	for _, want := range []string{
		"current/",       // how a local root follows
		"_SUCCESS",       // how an S3 root follows, having no pointer
		"--pin-snapshot", // the opt-out, which is the only reason to mention either
		"Regenerate after a table is added or dropped", // the half that never follows
	} {
		if !strings.Contains(long, want) {
			t.Errorf("the views help does not say %q:\n%s", want, long)
		}
	}
	// Named rather than left to the positive checks above: the sentence this
	// replaces is a specific false claim, and a rewrite that reinstated it
	// alongside the new ones would satisfy every check above.
	if strings.Contains(long, "does not follow it") {
		t.Error("the views help still claims the file does not follow a refreshed baseline")
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
