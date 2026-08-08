package mcptools

import (
	"strings"
	"testing"
)

// TestArchiveSkipNotice pins the #1285 warning surface: wording mirrors the
// reconstruct tool's archive_source_skipped entries, the recover variant is a
// valid SQL comment, and nothing beyond the source name (no fetch-error
// detail, no CLI flags) reaches the client-visible text.
func TestArchiveSkipNotice(t *testing.T) {
	if got := ArchiveSkipNotice("", nil); got != "" {
		t.Errorf("no skips must render nothing, got %q", got)
	}

	got := ArchiveSkipNotice("", []string{"/data/archive/bintrail_id=a", "s3://bkt/bintrail_id=b"})
	for _, want := range []string{
		"Warning: archive_source_skipped: events held only by this source are missing from the result: /data/archive/bintrail_id=a",
		"Warning: archive_source_skipped: events held only by this source are missing from the result: s3://bkt/bintrail_id=b",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
	if strings.Contains(got, "--") {
		t.Errorf("plain variant must carry no SQL-comment prefix (and no flag-shaped tokens), got %q", got)
	}

	sql := ArchiveSkipNotice("-- ", []string{"/data/archive/bintrail_id=a"})
	for _, line := range strings.Split(strings.TrimSpace(sql), "\n") {
		if line != "" && !strings.HasPrefix(line, "-- ") {
			t.Errorf("every recover warning line must be a SQL comment, got %q", line)
		}
	}
}
