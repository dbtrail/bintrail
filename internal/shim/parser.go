// Package shim implements an in-process MySQL-protocol server that
// answers BYOS time-travel queries (`_flashback.<table> AS OF '<ts>'
// WHERE <pk> = <value>`) by translating them into bintrail's existing
// query engine.
//
// The package is split:
//   - parser.go: SQL → FlashbackQuery (pure, easily testable).
//   - handler.go: glues the parser into go-mysql's server.Handler.
//
// The MVP recognises only a single statement shape:
//
//	SELECT * FROM _flashback.<table> AS OF '<timestamp>' WHERE <col> = <value>
//
// The schema is taken from the connection's USE'd database. Anything
// else returns ErrNotFlashback so the handler can route it elsewhere
// (currently: error to client; future: pass through to the real MySQL).
package shim

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// ErrNotFlashback indicates the input SQL is not in the form this
// package recognises. Callers can use this to distinguish "user typed a
// non-time-travel query" from "user typed a malformed time-travel
// query".
var ErrNotFlashback = errors.New("not a _flashback query")

// FlashbackQuery is the parsed form of a recognised statement.
type FlashbackQuery struct {
	Schema   string    // taken from the connection's USE'd database
	Table    string    // identifier after `_flashback.`
	AsOf     time.Time // timestamp from `AS OF '...'`, in UTC
	PKColumn string    // column name from the WHERE clause
	PKValue  string    // value as a string; numeric or quoted treated alike
}

// flashbackRE matches the documented MVP statement shape. Whitespace
// is permissive; keywords are case-insensitive (the (?i) flag).
//
// Capture groups:
//
//	1: table name
//	2: AS OF timestamp (without the surrounding quotes)
//	3: PK column name
//	4: PK value (numeric or single-quoted; quotes preserved for stripping)
var flashbackRE = regexp.MustCompile(
	`(?i)^\s*SELECT\s+\*\s+FROM\s+_flashback\.([A-Za-z_][A-Za-z0-9_]*)` +
		`\s+AS\s+OF\s+'([^']+)'` +
		`\s+WHERE\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*('[^']*'|-?\d+)\s*;?\s*$`,
)

// timeFormats are the formats accepted in the AS OF clause. Order
// matters: time.Parse stops at the first match.
var timeFormats = []string{
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02",
}

// Parse turns a raw SQL string into a FlashbackQuery. The defaultSchema
// is the connection's currently-selected database (per COM_INIT_DB);
// if the customer hasn't issued a USE statement yet, defaultSchema
// will be empty and Parse returns an error.
func Parse(sql, defaultSchema string) (FlashbackQuery, error) {
	// Quick prefix check so non-flashback queries get ErrNotFlashback
	// without paying for the regex compile-and-match every time.
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return FlashbackQuery{}, ErrNotFlashback
	}
	if !strings.Contains(strings.ToLower(trimmed), "_flashback.") {
		return FlashbackQuery{}, ErrNotFlashback
	}

	m := flashbackRE.FindStringSubmatch(trimmed)
	if m == nil {
		return FlashbackQuery{}, fmt.Errorf("malformed _flashback query; expected form: SELECT * FROM _flashback.<table> AS OF '<ts>' WHERE <col> = <value>")
	}

	if defaultSchema == "" {
		return FlashbackQuery{}, fmt.Errorf("no schema selected; issue `USE <database>;` before running a _flashback query")
	}

	asOf, err := parseAsOf(m[2])
	if err != nil {
		return FlashbackQuery{}, fmt.Errorf("invalid AS OF timestamp %q: %w", m[2], err)
	}

	return FlashbackQuery{
		Schema:   defaultSchema,
		Table:    m[1],
		AsOf:     asOf,
		PKColumn: m[3],
		PKValue:  stripQuotes(m[4]),
	}, nil
}

// parseAsOf tries the supported time formats in order. All values are
// returned in UTC so downstream comparisons against
// `binlog_events.event_timestamp` (which is stored UTC) line up.
func parseAsOf(s string) (time.Time, error) {
	for _, f := range timeFormats {
		if t, err := time.ParseInLocation(f, s, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("must be one of: %s", strings.Join(timeFormats, ", "))
}

// stripQuotes removes the surrounding single quotes from a string
// literal. Numeric values and unquoted identifiers pass through.
func stripQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}
