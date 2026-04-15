// Package cliutil provides shared filter-parsing helpers used by both
// cmd/bintrail/ commands and cmd/bintrail-mcp/.
package cliutil

import (
	"fmt"
	"strings"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
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
	case "SNAPSHOT":
		et := parser.EventSnapshot
		return &et, nil
	default:
		return nil, fmt.Errorf("invalid event type %q; must be INSERT, UPDATE, DELETE, or SNAPSHOT", s)
	}
}

// ParseTime parses a datetime string as UTC.
// Accepts three formats (tried in order):
//   - MySQL datetime: "2006-01-02 15:04:05"  (interpreted as UTC)
//   - RFC 3339:       "2006-01-02T15:04:05Z07:00" (timezone from string)
//   - Date-only:      "2006-01-02" (interpreted as midnight UTC)
//
// Returns nil for an empty string.
func ParseTime(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	// Try MySQL datetime format first — always UTC to match stored timestamps.
	t, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.UTC)
	if err == nil {
		return &t, nil
	}
	// Try RFC 3339 (preserves explicit timezone from the string).
	t, err = time.Parse(time.RFC3339, s)
	if err == nil {
		return &t, nil
	}
	// Try date-only (midnight UTC).
	t, err = time.ParseInLocation("2006-01-02", s, time.UTC)
	if err == nil {
		return &t, nil
	}
	return nil, fmt.Errorf("invalid time %q; expected YYYY-MM-DD HH:MM:SS, RFC 3339, or YYYY-MM-DD", s)
}

// IsValidFormat reports whether s is a supported query output format (table, json, or csv).
func IsValidFormat(s string) bool {
	switch strings.ToLower(s) {
	case "table", "json", "csv":
		return true
	}
	return false
}

// IsValidOutputFormat reports whether s is a supported general output format (text or json).
// Used by commands other than query (init, snapshot, index, status, recover, etc.).
func IsValidOutputFormat(s string) bool {
	switch strings.ToLower(s) {
	case "text", "json":
		return true
	}
	return false
}
