package mcptools

import (
	"strings"
	"testing"
)

// TestEventDivergenceNotice pins the #1325 query-tool warning surface: zero
// divergences render NOTHING (agreeing duplicates are the normal
// archived-but-not-dropped overlap — warning on them is the cry-wolf failure
// the merge comparison was designed around), a positive count renders one
// tagged line, and no flag-shaped token reaches the client text (an MCP
// client cannot pass a CLI flag).
func TestEventDivergenceNotice(t *testing.T) {
	if got := EventDivergenceNotice(0); got != "" {
		t.Errorf("no divergence must render nothing, got %q", got)
	}

	got := EventDivergenceNotice(3)
	for _, want := range []string{
		"Warning: event_divergence: 3 duplicate event(s) disagreed",
		"byte-for-byte copy of the index rows",
		"server log",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
	if strings.Contains(got, "--") {
		t.Errorf("notice must carry no flag-shaped tokens, got %q", got)
	}
}
