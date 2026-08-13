package console

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #1311: a profiled session reads live MySQL only, and the response never said
// so. The exclusion is correct — the archive path runs no redaction, so it
// fails in the safe direction — but an unstated restriction turns a short or
// empty result into an answer about the DATA. Hours that rotated into archive
// storage still exist; the session simply does not open them.
func TestRestrictedFetchWarnings(t *testing.T) {
	hour := time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)
	planWithGap := &query.QueryPlan{GapHours: []time.Time{hour}}

	t.Run("no plan still warns", func(t *testing.T) {
		// THE hole this closes. The planner only runs with a time range, so
		// the default browse (newest N, no since/until) produced a nil plan
		// and therefore said NOTHING, while reading half the index.
		w := restrictedFetchWarnings(nil, archivesExcludedByProfile)
		if len(w) != 1 {
			t.Fatalf("want exactly the exclusion notice with no plan, got %#v", w)
		}
		if !strings.Contains(w[0], "LIVE INDEX ONLY") {
			t.Errorf("notice does not state the scope: %q", w[0])
		}
		// The wrong inference must be denied in words, because making it is
		// the failure mode.
		if !strings.Contains(w[0], "does not mean nothing happened") {
			t.Errorf("notice does not deny the wrong inference: %q", w[0])
		}
	})

	t.Run("gap hours are not attributed to rotation", func(t *testing.T) {
		w := restrictedFetchWarnings(planWithGap, archivesExcludedByProfile)
		joined := strings.Join(w, "\n")
		// Plan classifies archived-only hours as gaps under an exclusion, by
		// design. Explaining them as "rotated and not archived" sends the
		// operator to audit a rotation that is working fine.
		if strings.Contains(joined, "rotated and not archived") {
			t.Errorf("gap hours misattributed to rotation for an archive-excluded read:\n%s", joined)
		}
		if !strings.Contains(joined, "2026-08-01 03:00") {
			t.Errorf("the hours are not named, so the operator cannot check them:\n%s", joined)
		}
		if !strings.Contains(joined, "NOT a finding") {
			t.Errorf("the warning reads as a data-loss finding:\n%s", joined)
		}
	})

	t.Run("unrestricted keeps the original wording", func(t *testing.T) {
		w := restrictedFetchWarnings(planWithGap, archivesRead)
		if len(w) != 1 || !strings.Contains(w[0], "rotated and not archived") {
			t.Errorf("a normal read must keep the planner's own gap sentence, got %#v", w)
		}
	})

	t.Run("clean unrestricted read warns about nothing", func(t *testing.T) {
		if w := restrictedFetchWarnings(nil, archivesRead); len(w) != 0 {
			t.Errorf("an unrestricted read with no plan must warn about nothing, got %#v", w)
		}
	})

	// The asymmetry: a --no-archive console must not stamp a banner on every
	// response forever. A permanent banner is read by nobody, including on
	// the day it matters — and it would train users to skip the profile
	// notice too, which is the one they cannot otherwise discover.
	t.Run("server-wide exclusion stays quiet with nothing to report", func(t *testing.T) {
		if w := restrictedFetchWarnings(nil, archivesExcludedByServer); len(w) != 0 {
			t.Errorf("a --no-archive console warned with no gap to show, on every response: %#v", w)
		}
	})

	t.Run("server-wide exclusion speaks up once there IS a gap", func(t *testing.T) {
		w := restrictedFetchWarnings(planWithGap, archivesExcludedByServer)
		joined := strings.Join(w, "\n")
		if !strings.Contains(joined, "--no-archive") {
			t.Errorf("want the server-wide cause named, got %#v", w)
		}
		if strings.Contains(joined, "data profile") {
			t.Errorf("blamed the session profile for a server-wide setting: %q", joined)
		}
		if strings.Contains(joined, "rotated and not archived") {
			t.Errorf("gap hours misattributed to rotation for an archive-excluded read:\n%s", joined)
		}
	})
}

// The precedence rule: --no-archive makes EVERY session archive-blind, so
// naming the session's own profile would point the operator at something that
// is not the reason and whose removal would change nothing.
func TestArchiveExclusionFor(t *testing.T) {
	profiled := httptest.NewRequest("GET", "/api/events", nil)
	profiled = profiled.WithContext(context.WithValue(profiled.Context(),
		policyCtxKey{}, &ext.AccessPolicy{Profile: "analyst"}))
	plain := httptest.NewRequest("GET", "/api/events", nil)

	for _, tc := range []struct {
		name      string
		noArchive bool
		profiled  bool
		want      archiveExclusion
	}{
		{"neither", false, false, archivesRead},
		{"profile only", false, true, archivesExcludedByProfile},
		{"server only", true, false, archivesExcludedByServer},
		{"both — server wins", true, true, archivesExcludedByServer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := plain
			if tc.profiled {
				r = profiled
			}
			if got := archiveExclusionFor(r, &bundle{noArchive: tc.noArchive}); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
