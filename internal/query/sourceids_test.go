package query_test

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

// SourceIDsFromPaths is what turns the resolved source PATHS a caller already
// holds into that scope, so the fix needs no second database read. The nil vs
// empty distinction is the whole safety property and is asserted directly.
func TestSourceIDsFromPaths(t *testing.T) {
	if got := query.SourceIDsFromPaths(nil); got != nil {
		t.Errorf("nil input must stay nil (unscoped), got %#v", got)
	}
	got := query.SourceIDsFromPaths([]string{})
	if got == nil {
		t.Error("an empty input must yield an empty NON-nil scope; collapsing it to nil turns 'I resolved nothing' into 'I resolved everything'")
	}
	for _, tc := range []struct {
		name  string
		paths []string
		want  []string
	}{
		{"local base", []string{"/archives/bintrail_id=abc"}, []string{"abc"}},
		{"trailing slash", []string{"/archives/bintrail_id=abc/"}, []string{"abc"}},
		{"s3 base", []string{"s3://bucket/prefix/bintrail_id=abc"}, []string{"abc"}},
		// rotate --bintrail-id takes an arbitrary string verbatim, so a
		// reader stricter than the writer would silently drop real archives.
		{"human-named id", []string{"/a/bintrail_id=prod-eu-1"}, []string{"prod-eu-1"}},
		{"dedup", []string{"/a/bintrail_id=x", "/b/bintrail_id=x"}, []string{"x"}},
		{"multiple", []string{"/a/bintrail_id=x", "/b/bintrail_id=y"}, []string{"x", "y"}},
		// No marker: DROPPED, never widened to "all". Counting an
		// unidentifiable archive as every archive is the false OK this
		// scoping exists to remove.
		{"no marker", []string{"/archives/plain"}, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := query.SourceIDsFromPaths(tc.paths)
			if len(got) != len(tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("got %#v, want %#v", got, tc.want)
				}
			}
		})
	}
}
