// Package cliutil provides shared filter-parsing and output helpers used by
// both cmd/bintrail/ commands and cmd/bintrail-mcp/.
package cliutil

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
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

// ParseSchemaList splits a comma-separated schema string into a trimmed slice,
// dropping empty entries. Returns nil if the input is empty.
func ParseSchemaList(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

// BuildIndexFilters builds a parser.Filters from comma-separated schema and
// table flag values.
func BuildIndexFilters(schemas, tables string) parser.Filters {
	var f parser.Filters
	if schemas != "" {
		f.Schemas = make(map[string]bool)
		for s := range strings.SplitSeq(schemas, ",") {
			if s = strings.TrimSpace(s); s != "" {
				f.Schemas[s] = true
			}
		}
	}
	if tables != "" {
		f.Tables = make(map[string]bool)
		for t := range strings.SplitSeq(tables, ",") {
			if t = strings.TrimSpace(t); t != "" {
				f.Tables[t] = true
			}
		}
	}
	return f
}

// OutputJSON encodes v as indented JSON to stdout.
func OutputJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
