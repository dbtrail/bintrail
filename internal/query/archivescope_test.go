package query

import (
	"reflect"
	"testing"
)

// The all-vs-none distinction is the whole safety property (#1327): collapsing
// "this read opens no archives" into "every archive in the index" turns "I
// resolved nothing" into "I resolved everything". These tests pin the type's
// ABSOLUTE semantics — not just that the constructors differ from each other,
// but which behaviour each one has — so swapping the two bodies fails here
// even without a database.
func TestArchiveScopeSemantics(t *testing.T) {
	if reflect.DeepEqual(AllArchives(), OnlyArchives()) {
		t.Fatal("AllArchives() and OnlyArchives() are indistinguishable — the type has collapsed 'opens everything' into 'opens none'")
	}

	// The zero value is the SAFE spelling: opens none. A forgotten scope must
	// report gaps, never credit coverage the fetch will not open — the
	// inverse of the bare-[]string default this type replaced, where an
	// unset nil meant "all".
	var zero ArchiveScope
	if !reflect.DeepEqual(zero, OnlyArchives()) {
		t.Errorf("zero-value ArchiveScope = %#v, want OnlyArchives()", zero)
	}
	if !zero.opensNone() {
		t.Error("zero-value ArchiveScope must open no archives")
	}

	// Absolute behaviour, constructor by constructor. AllArchives reads
	// coverage unrestricted; OnlyArchives() reads none; OnlyArchives(ids...)
	// restricts to exactly those ids.
	if AllArchives().opensNone() {
		t.Error("AllArchives().opensNone() = true; the unscoped read would skip the coverage read entirely")
	}
	if where, args := AllArchives().clause(); where != "" || args != nil {
		t.Errorf("AllArchives().clause() = (%q, %v), want no clause — every registered archive counts", where, args)
	}
	if OnlyArchives("a").opensNone() {
		t.Error("OnlyArchives(\"a\").opensNone() = true, want false")
	}
	where, args := OnlyArchives("a", "b").clause()
	if where != " WHERE bintrail_id IN (?, ?)" {
		t.Errorf("OnlyArchives(a, b).clause() where = %q", where)
	}
	if !reflect.DeepEqual(args, []any{"a", "b"}) {
		t.Errorf("OnlyArchives(a, b).clause() args = %v", args)
	}
}

// ScopeFromPaths turns the resolved source PATHS a caller already holds into
// the scope, so no second database read is needed. It must never widen: no
// input — nil, empty, or unidentifiable — may produce AllArchives.
func TestScopeFromPaths(t *testing.T) {
	// Resolving nothing is a read that opens NO archives. Under the old
	// []string contract a nil input faithfully mapped to nil ("all") and the
	// caller had to remember to substitute an empty slice; the type owns
	// that now.
	for _, in := range [][]string{nil, {}} {
		got := ScopeFromPaths(in)
		if !reflect.DeepEqual(got, OnlyArchives()) {
			t.Errorf("ScopeFromPaths(%#v) = %#v, want OnlyArchives()", in, got)
		}
		if reflect.DeepEqual(got, AllArchives()) {
			t.Errorf("ScopeFromPaths(%#v) widened to AllArchives()", in)
		}
	}

	for _, tc := range []struct {
		name  string
		paths []string
		want  ArchiveScope
	}{
		{"local base", []string{"/archives/bintrail_id=abc"}, OnlyArchives("abc")},
		{"trailing slash", []string{"/archives/bintrail_id=abc/"}, OnlyArchives("abc")},
		{"s3 base", []string{"s3://bucket/prefix/bintrail_id=abc"}, OnlyArchives("abc")},
		// rotate --bintrail-id takes an arbitrary string verbatim, so a
		// reader stricter than the writer would silently drop real archives.
		{"human-named id", []string{"/a/bintrail_id=prod-eu-1"}, OnlyArchives("prod-eu-1")},
		{"dedup", []string{"/a/bintrail_id=x", "/b/bintrail_id=x"}, OnlyArchives("x")},
		{"multiple", []string{"/a/bintrail_id=x", "/b/bintrail_id=y"}, OnlyArchives("x", "y")},
		// No marker: DROPPED, never widened. Counting an unidentifiable
		// archive as every archive is the false OK this scoping removes.
		{"no marker", []string{"/archives/plain"}, OnlyArchives()},
		{"no marker beside a real one", []string{"/archives/plain", "/a/bintrail_id=x"}, OnlyArchives("x")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ScopeFromPaths(tc.paths); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ScopeFromPaths(%v) = %#v, want %#v", tc.paths, got, tc.want)
			}
		})
	}
}
