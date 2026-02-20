// Package cliutil provides shared filter-parsing helpers used by both
// cmd/bintrail/ commands and cmd/bintrail-mcp/.
package cliutil

import (
	"fmt"
	"strings"
	"time"

	"github.com/bintrail/bintrail/internal/parser"
)

// ParseEventType converts an event-type string to a *parser.EventType.
// Returns nil for an empty string (meaning "all types").
func ParseEventType(s string) (*parser.EventType, error) {
	switch strings.ToUpper(s) {
	case "":
		return nil, nil
	case "INSERT":
		et := parser.EventInsert
		return &et, nil
	case "UPDATE":
		et := parser.EventUpdate
		return &et, nil
	case "DELETE":
		et := parser.EventDelete
		return &et, nil
	default:
		return nil, fmt.Errorf("invalid event type %q; must be INSERT, UPDATE, or DELETE", s)
	}
}

// ParseTime parses a datetime string using the local timezone.
// Accepts three formats (tried in order):
//   - MySQL datetime: "2006-01-02 15:04:05"
//   - RFC 3339:       "2006-01-02T15:04:05Z07:00"
//   - Date-only:      "2006-01-02" (interpreted as midnight local time)
//
// Returns nil for an empty string.
func ParseTime(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	// Try MySQL datetime format first.
	t, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.Local)
	if err == nil {
		return &t, nil
	}
	// Try RFC 3339.
	t, err = time.Parse(time.RFC3339, s)
	if err == nil {
		return &t, nil
	}
	// Try date-only (midnight local time).
	t, err = time.ParseInLocation("2006-01-02", s, time.Local)
	if err == nil {
		return &t, nil
	}
	return nil, fmt.Errorf("invalid time %q; expected YYYY-MM-DD HH:MM:SS, RFC 3339, or YYYY-MM-DD", s)
}

// IsValidFormat reports whether s is a supported output format (table, json, or csv).
func IsValidFormat(s string) bool {
	switch strings.ToLower(s) {
	case "table", "json", "csv":
		return true
	}
	return false
}
