// Package shim implements an in-process MySQL-protocol server that
// answers time-travel queries by translating them into bintrail's
// existing query engine.
//
// Three virtual-schema statement shapes are recognised:
//
//	SELECT * FROM _flashback.<table> AS OF '<ts>'           WHERE <col> = <value>
//	SELECT * FROM _snapshot.<table>  AS OF '<ts>'           WHERE <col> = <value>
//	SELECT * FROM _diff.<table>      BETWEEN '<t1>' AND '<t2>' WHERE <col> = <value>
//
// A fourth, ergonomic shape — the optimizer-hint comment form —
// is also accepted and rewritten internally to _flashback:
//
//	SELECT /*+ DBTRAIL_AT='<ts>' */ * FROM [<schema>.]<table> [WHERE <col> = <value>]
//
// This is friendlier for ORMs (Hibernate, Sequelize, etc.) that
// can't easily rewrite the FROM clause from app code. ProxySQL's
// docs-advertised `DBTRAIL_AT` routing rule matches this form and
// forwards it to the shim hostgroup; the shim then transparently
// time-travels the original table. See issue #288.
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
	// in the schema-prefix literal. WHERE is optional for AS OF queries
	// (full-table reconstruction, issue #276); the _diff path keeps it
	// mandatory because a PK-less _diff against a hot row would be a
	// DoS in a wire-protocol response.
	flashbackRE = mustCompileAsOf(`_flashback`)
	snapshotRE  = mustCompileAsOf(`_snapshot`)
	diffRE      = mustCompileDiff()

	// hintRE matches the optimizer-hint comment form documented at
	// https://www.dbtrail.com/docs/guides/proxysql-time-travel/ :
	//
	//   SELECT /*+ DBTRAIL_AT='<ts>' */ * FROM [<schema>.]<table> [WHERE <col> = <val>] [;]
	//
	// ProxySQL routes any statement containing DBTRAIL_AT to the shim
	// (the rule_id 990001-990003 set written by `bintrail
	// proxysql-config`), so the shim must recognise the hint and
	// rewrite it into the canonical `_flashback.<table> AS OF '<ts>'`
	// shape before the virtual-schema dispatch fires. See issue #288.
	//
	// The hint may sit either between `SELECT` and `*` (the form the
	// docs example uses, which is also where MySQL itself expects an
	// optimizer hint) or between `*` and `FROM`. Mid-query hints
	// (between WHERE and the predicate, etc.) are intentionally not
	// supported — they're not in the docs and add ambiguity to the
	// rewrite path.
	//
	// Capture groups:
	//   1 = timestamp (between the quotes) when hint is in the
	//       `SELECT /*+...*/ *` position
	//   2 = timestamp when hint is in the `SELECT * /*+...*/ FROM`
	//       position (only one of group 1 or 2 is non-empty)
	//   3 = schema prefix (without trailing dot) or empty
	//   4 = table
	//   5 = WHERE clause column (or empty)
	//   6 = WHERE clause value, quoted or numeric (or empty)
	hintRE = regexp.MustCompile(
		`(?i)^\s*SELECT\s+` +
			`(?:/\*\+\s*DBTRAIL_AT\s*=\s*'([^']*)'\s*\*/\s+\*|` +
			`\*\s+/\*\+\s*DBTRAIL_AT\s*=\s*'([^']*)'\s*\*/)` +
			`\s+FROM\s+(?:([A-Za-z_][A-Za-z0-9_]*)\.)?([A-Za-z_][A-Za-z0-9_]*)` +
			`(?:\s+WHERE\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*('[^']*'|-?\d+))?` +
			`\s*;?\s*$`,
	)

	// hintProbeRE is a cheap, anchored test for "does this query look
	// like a SELECT whose leading optimizer-hint comment is the
	// DBTRAIL_AT form?" Anchoring to ^\s*SELECT (and requiring the
	// hint immediately after SELECT or after `*`) means a query
	// containing the literal text `DBTRAIL_AT` inside a string
	// literal — `WHERE comment = '/*+ DBTRAIL_AT=foo */'` — does
	// NOT trigger the rewrite path. Without this anchor a benign
	// query would hit parseHintForm, fail hintRE.FindStringSubmatch,
	// and return ER_PARSE_ERROR (1064) to the customer when the
	// query is perfectly valid for the upstream MySQL.
	hintProbeRE = regexp.MustCompile(`(?i)^\s*SELECT\s+(?:\*\s+)?/\*\+\s*DBTRAIL_AT\b`)
)

// mustCompileAsOf builds a regex for `_flashback` / `_snapshot` shapes.
// Capture groups: 1=table, 2=timestamp, 3=col (or empty), 4=value (or empty).
// The trailing WHERE clause is in an optional non-capturing group so the
// PK-filtered fast path and the full-table path go through the same matcher.
func mustCompileAsOf(schemaPrefix string) *regexp.Regexp {
	return regexp.MustCompile(
		`(?i)^\s*SELECT\s+\*\s+FROM\s+` + schemaPrefix + `\.([A-Za-z_][A-Za-z0-9_]*)` +
			`\s+AS\s+OF\s+'([^']+)'` +
			`(?:\s+WHERE\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*('[^']*'|-?\d+))?` +
			`\s*;?\s*$`,
	)
}

func mustCompileDiff() *regexp.Regexp {
	return regexp.MustCompile(
		`(?i)^\s*SELECT\s+\*\s+FROM\s+_diff\.([A-Za-z_][A-Za-z0-9_]*)` +
			`\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'` +
			`\s+WHERE\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*('[^']*'|-?\d+)\s*;?\s*$`,
	)
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

	// Hint-comment form (issue #288). When the docs-advertised
	//   SELECT /*+ DBTRAIL_AT='<ts>' */ * FROM [<schema>.]<table> [WHERE ...]
	// shape is detected, parse it directly into a TypeFlashback
	// TimeTravelQuery — the user-facing FROM keeps the real table
	// name, and the shim transparently time-travels it. The hint
	// form never reaches the `_flashback.` prefix screen below, so
	// detection has to fire here. We probe cheaply first
	// (case-insensitive token check) so non-hint queries pay one
	// regex check, not the full hintRE match.
	if hintProbeRE.MatchString(trimmed) {
		return parseHintForm(trimmed, defaultSchema)
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

// parseHintForm handles the optimizer-hint comment form:
//
//	SELECT /*+ DBTRAIL_AT='<ts>' */ * FROM [<schema>.]<table> [WHERE <col> = <val>]
//
// This is the form the dbtrail docs advertise (and the form
// ProxySQL is configured to route via the `DBTRAIL_AT` match
// rule). It rewrites internally to a TypeFlashback query with the
// real table name and AS OF = the hint timestamp.
//
// On any malformed input — bad timestamp, missing FROM, etc. —
// returns a non-ErrNotTimeTravel error so HandleQuery emits
// ER_PARSE_ERROR (1064), the same way it does for malformed
// `_flashback.<t> AS OF '...'` queries.
func parseHintForm(trimmed, defaultSchema string) (TimeTravelQuery, error) {
	m := hintRE.FindStringSubmatch(trimmed)
	if m == nil {
		return TimeTravelQuery{}, fmt.Errorf(
			"malformed time-travel hint; expected:\n" +
				"  SELECT /*+ DBTRAIL_AT='<ts>' */ * FROM [<schema>.]<table> [WHERE <col> = <value>]",
		)
	}
	// Group 1 or 2 holds the timestamp depending on hint position;
	// exactly one is non-empty. Same for the optional WHERE groups
	// (5/6 may be empty for the full-table shape).
	ts := m[1]
	if ts == "" {
		ts = m[2]
	}
	asOf, err := parseTimeLiteral(ts)
	if err != nil {
		return TimeTravelQuery{}, fmt.Errorf("invalid AS OF timestamp %q: %w", ts, err)
	}
	schema := m[3]
	if schema == "" {
		schema = defaultSchema
	}
	if schema == "" {
		return TimeTravelQuery{}, fmt.Errorf(
			"no schema selected; issue `USE <database>;` before running a time-travel query " +
				"(or qualify the table as <schema>.<table>)",
		)
	}
	return TimeTravelQuery{
		Type:     TypeFlashback,
		Schema:   schema,
		Table:    m[4],
		AsOf:     asOf,
		PKColumn: m[5],
		PKValue:  stripQuotes(m[6]),
	}, nil
}
