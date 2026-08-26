package cliutil

import (
	"fmt"
	"strconv"
	"time"
)

// ParseInterval parses a loop interval like "15m", "6h" or "1d" into a
// time.Duration. Supported units: 'm' (minutes), 'h' (hours), 'd' (days). The
// number must be a positive integer.
//
// Separate from ParseRetain on purpose, and the separation is the point.
// ParseRetain's callers are retention and lookback WINDOWS (--retain,
// --lookback, --buffer-retain): there, accepting "5m" would mean dropping
// partitions almost as fast as they are written, so its unit set staying at
// hours and days is a guardrail rather than an oversight. An INTERVAL answers
// a different question — how often does this loop run — and minutes are an
// ordinary answer to it.
//
// Both go through parseUnitDuration so the two can differ in the only way they
// should (which units they accept) and cannot drift in the way that would be a
// bug (how the number is parsed).
func ParseInterval(s string) (time.Duration, error) {
	return parseUnitDuration(s, intervalUnits,
		"expected Nm (minutes), Nh (hours) or Nd (days), e.g. 15m",
		"use 'm' for minutes, 'h' for hours or 'd' for days")
}

// unitDuration pairs an accepted suffix with what it multiplies to. A slice
// rather than a map so the accepted set has a stable order when a caller
// renders it.
type unitDuration struct {
	suffix byte
	span   time.Duration
}

var (
	retainUnits = []unitDuration{
		{'d', 24 * time.Hour},
		{'h', time.Hour},
	}
	intervalUnits = []unitDuration{
		{'d', 24 * time.Hour},
		{'h', time.Hour},
		{'m', time.Minute},
	}
)

// parseUnitDuration parses the "N<unit>" shape shared by ParseRetain and
// ParseInterval. expected and useUnits are the caller's own wording for the two
// failures, so each entry point keeps error text that names its own unit set
// rather than a generic one an operator then has to translate.
func parseUnitDuration(s string, units []unitDuration, expected, useUnits string) (time.Duration, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid format %q; %s", s, expected)
	}
	unit := s[len(s)-1]
	numStr := s[:len(s)-1]
	// strconv.Atoi (unlike fmt.Sscanf "%d") rejects unconsumed input, so
	// "1.5d" or "30 0d" fail loud instead of silently truncating the value.
	n, err := strconv.Atoi(numStr)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid format %q; %s", s, expected)
	}
	for _, u := range units {
		if u.suffix == unit {
			return time.Duration(n) * u.span, nil
		}
	}
	return 0, fmt.Errorf("invalid unit %q in %q; %s", unit, s, useUnits)
}
