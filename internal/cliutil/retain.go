package cliutil

import (
	"time"
)

// ParseRetain parses a retain string like "7d" or "24h" into a time.Duration.
// Supported units: 'd' (days), 'h' (hours). The number must be a positive integer.
//
// Minutes are deliberately NOT accepted: this backs retention and lookback
// WINDOWS (--retain, --lookback, --buffer-retain), where "5m" would mean
// dropping partitions about as fast as they are written. ParseInterval is the
// sibling for the "how often does this run" sense, and interval.go holds the
// parser both share.
func ParseRetain(s string) (time.Duration, error) {
	return parseUnitDuration(s, retainUnits,
		"expected Nd (days) or Nh (hours), e.g. 7d",
		"use 'd' for days or 'h' for hours")
}
