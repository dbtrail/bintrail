package cliutil

import (
	"strings"
	"testing"
	"time"
)

// badDurations are inputs neither ParseRetain nor ParseInterval may accept,
// whatever units they differ on. Shared rather than duplicated per function:
// the two are allowed to disagree about units and must not disagree about how
// the NUMBER is read, and a corpus written twice is how that guarantee rots.
// The Sscanf cases are the #817 regressions and are the reason the shared
// parser uses strconv.Atoi.
var badDurations = []string{
	"",      // too short
	"d",     // no number
	"7x",    // unknown unit
	"7",     // no unit
	"-1d",   // negative
	"0d",    // zero
	"0h",    // zero hours
	"1.5d",  // fractional; Sscanf silently truncated to 1d (#817)
	"30 0d", // embedded space; Sscanf silently truncated to 30d (#817)
	"7dd",   // trailing garbage before unit
	" 7d",   // leading whitespace
	// Overflow. time.Duration is an int64 of nanoseconds and the multiply
	// wraps silently, so these are not merely "too big": unchecked, the first
	// parsed as a valid 40s and the second as a plausible-looking 1h40m. A
	// wrapped interval drives a ticker below the floor this package sets, and
	// a wrapped --retain drops partitions early. Both must be refusals.
	"384307168202282326m",
	"153722867280913d",
	"9223372036854775807d",
}

func TestParseInterval_valid(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want time.Duration
	}{
		{"15m", 15 * time.Minute},
		{"1m", time.Minute},
		{"90m", 90 * time.Minute},
		{"6h", 6 * time.Hour},
		{"1d", 24 * time.Hour},
	} {
		got, err := ParseInterval(tc.in)
		if err != nil {
			t.Errorf("ParseInterval(%q): unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseInterval(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestParseInterval_rejectsTheSameMalformedInput(t *testing.T) {
	for _, c := range badDurations {
		if _, err := ParseInterval(c); err == nil {
			t.Errorf("ParseInterval(%q): expected an error, got nil", c)
		}
	}
}

// TestParseRetain_rejectsMinutes guards the one regression this shared parser
// makes possible, and it is not a hypothetical one: handing ParseRetain the
// interval unit set is a one-word edit, and the whole pre-existing suite stays
// green when you make it, because TestParseRetain_invalid only asserts that an
// error is non-nil and never tries a minutes value.
//
// What it would mean is the point. ParseRetain backs --retain, so accepting
// "5m" turns a retention window into "drop partitions five minutes after they
// are written". Nothing downstream would question it: five minutes is a
// perfectly well-formed duration.
func TestParseRetain_rejectsMinutes(t *testing.T) {
	for _, c := range []string{"5m", "1m", "120m"} {
		if d, err := ParseRetain(c); err == nil {
			t.Errorf("ParseRetain(%q) = %v, want an error: minutes are a valid INTERVAL "+
				"but never a valid retention window", c, d)
		}
	}
}

// The two entry points must each name their own unit set when they refuse.
// An operator who types "15m" into --retain and one who types "15x" into
// --baseline-refresh-interval are told different things on purpose, and a
// shared parser makes it easy to collapse both onto one generic sentence.
func TestUnitErrorsNameTheirOwnUnits(t *testing.T) {
	_, err := ParseRetain("15m")
	if err == nil {
		t.Fatal("ParseRetain(\"15m\"): expected an error")
	}
	if strings.Contains(err.Error(), "minutes") {
		t.Errorf("ParseRetain rejection offers minutes as a unit: %v", err)
	}

	_, err = ParseInterval("15x")
	if err == nil {
		t.Fatal("ParseInterval(\"15x\"): expected an error")
	}
	if !strings.Contains(err.Error(), "minutes") {
		t.Errorf("ParseInterval rejection does not name minutes, so it reads like "+
			"ParseRetain's: %v", err)
	}
}

// The exact overflow boundary, per unit. The values in badDurations are orders
// of magnitude past it, so they cannot tell a correct check from one that is
// off by one in either direction: both admit or refuse everything that far
// out. These are the only inputs that discriminate.
//
// bound*span is the largest duration that still fits; (bound+1)*span wraps
// negative, and a negative interval reaching time.NewTicker panics inside a
// goroutine that has no recover, on a daemon that is also the capture plane.
func TestParseInterval_overflowBoundaryPerUnit(t *testing.T) {
	for _, tc := range []struct {
		ok, over string
		want     time.Duration
	}{
		{"153722867m", "153722868m", 153722867 * time.Minute},
		{"2562047h", "2562048h", 2562047 * time.Hour},
		{"106751d", "106752d", 106751 * 24 * time.Hour},
	} {
		got, err := ParseInterval(tc.ok)
		if err != nil {
			t.Errorf("ParseInterval(%q) refused the largest value that fits: %v", tc.ok, err)
		} else if got != tc.want {
			t.Errorf("ParseInterval(%q) = %v, want %v", tc.ok, got, tc.want)
		}
		if got, err := ParseInterval(tc.over); err == nil {
			t.Errorf("ParseInterval(%q) = %v with no error; one past the bound wraps", tc.over, got)
		}
	}
}
