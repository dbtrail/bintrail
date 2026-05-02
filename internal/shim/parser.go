// Package shim implements an in-process MySQL-protocol server that
// answers BYOS time-travel queries by translating them into bintrail's
// existing query engine.
//
// Three virtual-schema statement shapes are recognised:
//
//	SELECT * FROM _flashback.<table> AS OF '<ts>'           WHERE <col> = <value>
//	SELECT * FROM _snapshot.<table>  AS OF '<ts>'           WHERE <col> = <value>
//	SELECT * FROM _diff.<table>      BETWEEN '<t1>' AND '<t2>' WHERE <col> = <value>
//
// _flashback returns the row's state at-or-before the AS OF instant.
// _snapshot is currently identical to _flashback; the distinction is
//   semantic: _snapshot is intended to integrate baseline lookups (the
//   bintrail dump/baseline pipeline) so it can answer for rows that
//   have never appeared in binlog events. For now they share an
//   implementation and the API surface is reserved.
// _diff returns every event for the PK between t1 and t2, one row per
//   event. Useful for "what changed to this row recently".
//
// The schema is taken from the connection's currently-USE'd database.
// Anything else returns ErrNotTimeTravel so the handler can route it
// elsewhere (currently: error to the client).
package shim

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// ErrNotTimeTravel indicates the input SQL is not in any of the
// virtual-schema shapes this package recognises.
var ErrNotTimeTravel = errors.New("not a time-travel query")

// QueryType discriminates between the three virtual schemas.
type QueryType int

const (
	TypeFlashback QueryType = iota
	TypeSnapshot
	TypeDiff
)

func (t QueryType) String() string {
	switch t {
	case TypeFlashback:
		return "_flashback"
	case TypeSnapshot:
		return "_snapshot"
	case TypeDiff:
		return "_diff"
	}
	return "unknown"
}

// TimeTravelQuery is the parsed form of any of the three recognised
// shapes. Only the time fields relevant to the Type are populated:
//
//	TypeFlashback / TypeSnapshot → AsOf set, Since/Until zero
//	TypeDiff                     → Since/Until set, AsOf zero
type TimeTravelQuery struct {
	Type     QueryType
	Schema   string // taken from the connection's USE'd database
	Table    string
	AsOf     time.Time // for flashback/snapshot
	Since    time.Time // for diff (inclusive lower bound)
	Until    time.Time // for diff (inclusive upper bound)
	PKColumn string
	PKValue  string
}

var (
	// flashbackRE / snapshotRE share the same shape; they differ only
	// in the schema-prefix literal.
	flashbackRE = mustCompileTT(`_flashback`, `\s+AS\s+OF\s+'([^']+)'`)
	snapshotRE  = mustCompileTT(`_snapshot`, `\s+AS\s+OF\s+'([^']+)'`)
	diffRE      = mustCompileTT(`_diff`, `\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'`)
)

func mustCompileTT(schemaPrefix, timeClause string) *regexp.Regexp {
	pattern := `(?i)^\s*SELECT\s+\*\s+FROM\s+` + schemaPrefix + `\.([A-Za-z_][A-Za-z0-9_]*)` +
		timeClause +
		`\s+WHERE\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*('[^']*'|-?\d+)\s*;?\s*$`
	return regexp.MustCompile(pattern)
}

// timeFormats are the formats accepted in time literals. Order
// matters: time.Parse stops at the first match.
var timeFormats = []string{
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02",
}

// Parse turns a raw SQL string into a TimeTravelQuery.
//
// defaultSchema is the connection's currently-selected database. If
// the customer hasn't issued a USE statement and the SQL is otherwise
// well-formed for one of the virtual schemas, Parse returns an error
// asking them to do so.
func Parse(sql, defaultSchema string) (TimeTravelQuery, error) {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return TimeTravelQuery{}, ErrNotTimeTravel
	}

	// Quick prefix screen so non-virtual queries pay only one
	// strings.Contains, not three regex matches.
	lower := strings.ToLower(trimmed)
	if !strings.Contains(lower, "_flashback.") &&
		!strings.Contains(lower, "_snapshot.") &&
		!strings.Contains(lower, "_diff.") {
		return TimeTravelQuery{}, ErrNotTimeTravel
	}

	if defaultSchema == "" {
		return TimeTravelQuery{}, fmt.Errorf("no schema selected; issue `USE <database>;` before running a time-travel query")
	}

	if m := flashbackRE.FindStringSubmatch(trimmed); m != nil {
		return parseAsOfMatch(m, TypeFlashback, defaultSchema)
	}
	if m := snapshotRE.FindStringSubmatch(trimmed); m != nil {
		return parseAsOfMatch(m, TypeSnapshot, defaultSchema)
	}
	if m := diffRE.FindStringSubmatch(trimmed); m != nil {
		return parseDiffMatch(m, defaultSchema)
	}

	return TimeTravelQuery{}, fmt.Errorf(
		"malformed time-travel query; expected one of:\n" +
			"  SELECT * FROM _flashback.<table> AS OF '<ts>' WHERE <col> = <value>\n" +
			"  SELECT * FROM _snapshot.<table>  AS OF '<ts>' WHERE <col> = <value>\n" +
			"  SELECT * FROM _diff.<table>      BETWEEN '<t1>' AND '<t2>' WHERE <col> = <value>",
	)
}

// parseAsOfMatch fills a TimeTravelQuery for the _flashback / _snapshot
// shapes (capture groups: 1 table, 2 timestamp, 3 col, 4 value).
func parseAsOfMatch(m []string, t QueryType, schema string) (TimeTravelQuery, error) {
	asOf, err := parseTimeLiteral(m[2])
	if err != nil {
		return TimeTravelQuery{}, fmt.Errorf("invalid AS OF timestamp %q: %w", m[2], err)
	}
	return TimeTravelQuery{
		Type:     t,
		Schema:   schema,
		Table:    m[1],
		AsOf:     asOf,
		PKColumn: m[3],
		PKValue:  stripQuotes(m[4]),
	}, nil
}

// parseDiffMatch fills a TimeTravelQuery for the _diff shape
// (capture groups: 1 table, 2 t1, 3 t2, 4 col, 5 value).
func parseDiffMatch(m []string, schema string) (TimeTravelQuery, error) {
	since, err := parseTimeLiteral(m[2])
	if err != nil {
		return TimeTravelQuery{}, fmt.Errorf("invalid BETWEEN lower bound %q: %w", m[2], err)
	}
	until, err := parseTimeLiteral(m[3])
	if err != nil {
		return TimeTravelQuery{}, fmt.Errorf("invalid BETWEEN upper bound %q: %w", m[3], err)
	}
	if until.Before(since) {
		return TimeTravelQuery{}, fmt.Errorf(
			"BETWEEN bounds out of order: %s is after %s",
			since.Format(time.RFC3339), until.Format(time.RFC3339),
		)
	}
	return TimeTravelQuery{
		Type:     TypeDiff,
		Schema:   schema,
		Table:    m[1],
		Since:    since,
		Until:    until,
		PKColumn: m[4],
		PKValue:  stripQuotes(m[5]),
	}, nil
}

func parseTimeLiteral(s string) (time.Time, error) {
	for _, f := range timeFormats {
		if t, err := time.ParseInLocation(f, s, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("must be one of: %s", strings.Join(timeFormats, ", "))
}

func stripQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}
