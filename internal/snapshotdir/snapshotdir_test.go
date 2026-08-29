package snapshotdir

import (
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string // RFC3339, or "" when the name must be rejected
	}{
		{"2026-08-23T14-50-30Z", "2026-08-23T14:50:30Z"},
		{"2026-01-01T00-00-00Z", "2026-01-01T00:00:00Z"},
		// The date half separates with '-' too, so restoring colons across the
		// whole string would corrupt it. This is the case that pins the split
		// at the 'T'.
		{"2026-08-23", ""},
		{"", ""},
		{"not-a-snapshot", ""},
		// Already-colonised parses too. Not the form reconstruct writes, but
		// accepting it is what the parser has always done and narrowing that
		// here would change behaviour for no reason.
		{"2026-08-23T14:50:30Z", "2026-08-23T14:50:30Z"},
	} {
		got, ok := ParseTime(tc.name)
		if tc.want == "" {
			if ok {
				t.Errorf("ParseTime(%q) accepted it as %v", tc.name, got)
			}
			continue
		}
		if !ok {
			t.Errorf("ParseTime(%q) rejected a name reconstruct writes", tc.name)
			continue
		}
		if got.Format(time.RFC3339) != tc.want {
			t.Errorf("ParseTime(%q) = %v, want %s", tc.name, got, tc.want)
		}
		if got.Location() != time.UTC {
			t.Errorf("ParseTime(%q) did not return UTC: %v", tc.name, got.Location())
		}
	}
}

// The parser and the writer are two halves of one convention living in
// different packages, so the round trip is what keeps them honest. Written
// literally rather than by calling reconstruct.SnapshotDirName, which would
// import a package that imports this one.
func TestParseTime_roundTripsTheWrittenForm(t *testing.T) {
	want := time.Date(2026, 8, 23, 14, 50, 30, 0, time.UTC)
	name := "2026-08-23T14-50-30Z" // strings.ReplaceAll(RFC3339, ":", "-")
	got, ok := ParseTime(name)
	if !ok || !got.Equal(want) {
		t.Errorf("ParseTime(%q) = %v, %v; want %v", name, got, ok, want)
	}
}
