package cliutil

import (
	"fmt"
	"strconv"
	"time"
)

// ParseRetain parses a retain string like "7d" or "24h" into a time.Duration.
// Supported units: 'd' (days), 'h' (hours). The number must be a positive integer.
func ParseRetain(s string) (time.Duration, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid format %q; expected Nd (days) or Nh (hours), e.g. 7d", s)
	}
	unit := s[len(s)-1]
	numStr := s[:len(s)-1]
	// strconv.Atoi (unlike fmt.Sscanf "%d") rejects unconsumed input, so
	// "1.5d" or "30 0d" fail loud instead of silently truncating retention.
	n, err := strconv.Atoi(numStr)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid format %q; expected Nd (days) or Nh (hours), e.g. 7d", s)
	}
	switch unit {
	case 'd':
		return time.Duration(n) * 24 * time.Hour, nil
	case 'h':
		return time.Duration(n) * time.Hour, nil
	default:
		return 0, fmt.Errorf("invalid unit %q in %q; use 'd' for days or 'h' for hours", unit, s)
	}
}
