package mcptools

import (
	"context"
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

// TestEnvArchiveSourcesSignal pins the standalone surface's discovery signal
// (#1288 review: a mutation discarding stateArchiveSources' bool at the
// fallthrough would recreate the #1285 bug verbatim with the suite green). A
// nil DB makes state discovery a clean no-archives (nil, false), isolating
// the env-pair logic.
func TestEnvArchiveSourcesSignal(t *testing.T) {
	ctx := context.Background()

	t.Setenv("BINTRAIL_ARCHIVE_S3", "s3://bkt/prefix")
	t.Setenv("BINTRAIL_ID", "id-1")
	s, failed := EnvArchiveSources(ctx, nil)
	if failed || len(s) != 1 || s[0] != "s3://bkt/prefix/bintrail_id=id-1" {
		t.Errorf("full env pair must be (1 source, false), got (%v, %v)", s, failed)
	}

	t.Setenv("BINTRAIL_ID", "")
	if _, failed := EnvArchiveSources(ctx, nil); !failed {
		t.Error("half-set env pair is explicit archive intent that cannot be honored; discovery must be flagged unreliable")
	}

	t.Setenv("BINTRAIL_ARCHIVE_S3", "")
	if s, failed := EnvArchiveSources(ctx, nil); failed || s != nil {
		t.Errorf("no env + nil DB = no archives and no failure, got (%v, %v)", s, failed)
	}
}