package mcptools

import (
	"strings"
	"testing"
)

// TestArchiveSkipNotice pins the #1285 query-tool warning surface: the
// wording is the reconstruct tool's wording (one shared helper, no drift), a
// discovery failure renders its own line, and nothing beyond the source name
// (no fetch-error detail, no flag-shaped tokens) reaches the client text.
func TestArchiveSkipNotice(t *testing.T) {
	if got := ArchiveSkipNotice(false, nil); got != "" {
		t.Errorf("no archive trouble must render nothing, got %q", got)
	}

	got := ArchiveSkipNotice(true, []string{"/data/archive/bintrail_id=a", "s3://bkt/bintrail_id=b"})
	for _, want := range []string{
		"Warning: archive_discovery_failed: no archives were read",
		"Warning: archive_source_skipped: events held only by this source are missing from the result: /data/archive/bintrail_id=a",
		"Warning: archive_source_skipped: events held only by this source are missing from the result: s3://bkt/bintrail_id=b",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
	if strings.Contains(got, "--") {
		t.Errorf("notice must carry no flag-shaped tokens, got %q", got)
	}
}