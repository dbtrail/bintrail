package console

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

// The two allow_gaps blind spots (#1281): each must produce a visible payload
// warning, and the quiet path must stay quiet.
func TestCoverageWarnings(t *testing.T) {
	t.Run("nil plan under allow_gaps → coverage-unverified warning", func(t *testing.T) {
		w := coverageWarnings(nil, nil, true)
		if len(w) != 1 || !strings.Contains(w[0], "coverage could not be verified") {
			t.Fatalf("want the unverified-coverage warning, got %v", w)
		}
	})

	t.Run("nil plan WITHOUT allow_gaps → no warning (strict mode already errored loud)", func(t *testing.T) {
		if w := coverageWarnings(nil, nil, false); len(w) != 0 {
			t.Fatalf("strict mode must not warn, got %v", w)
		}
	})

	t.Run("skipped sources → one warning each, naming the source", func(t *testing.T) {
		w := coverageWarnings(&query.QueryPlan{}, []string{"s3://bkt/a", "/var/archives"}, true)
		if len(w) != 2 || !strings.Contains(w[0], "s3://bkt/a") || !strings.Contains(w[1], "/var/archives") {
			t.Fatalf("want one warning per skipped source, got %v", w)
		}
	})

	t.Run("discovery-failure sentinel → its own warning, not the per-source one", func(t *testing.T) {
		w := coverageWarnings(&query.QueryPlan{}, []string{query.DiscoveryFailedSource}, true)
		if len(w) != 1 || !strings.Contains(w[0], "discovery failed") {
			t.Fatalf("want the discovery-failure warning, got %v", w)
		}
	})

	t.Run("healthy plan, nothing skipped → quiet", func(t *testing.T) {
		if w := coverageWarnings(&query.QueryPlan{}, nil, true); len(w) != 0 {
			t.Fatalf("healthy path must stay quiet, got %v", w)
		}
	})
}

// #1325: the merge divergence finding must render once, count-first, and the
// zero case must append nothing (cry-wolf rule — the normal
// archived-but-not-dropped overlap produces agreeing duplicates, count 0).
func TestAppendDivergenceWarning(t *testing.T) {
	if w := appendDivergenceWarning(nil, 0); len(w) != 0 {
		t.Errorf("count 0 must append nothing, got %#v", w)
	}
	base := []string{"existing"}
	w := appendDivergenceWarning(base, 3)
	if len(w) != 2 || w[0] != "existing" {
		t.Fatalf("expected existing warning + one divergence entry, got %#v", w)
	}
	for _, want := range []string{"3 duplicate event(s) disagreed", "archive copy", "byte-for-byte"} {
		if !strings.Contains(w[1], want) {
			t.Errorf("warning lacks %q: %s", want, w[1])
		}
	}
	// The console DTO boundary deliberately omits row internals
	// (connection_id/query_text); the warning must not smuggle any in — it
	// carries a COUNT, and per-event detail stays in the server log.
	if strings.Contains(w[1], "connection_id") || strings.Contains(w[1], "query_text") {
		t.Errorf("warning must not name row internals: %s", w[1])
	}
}
