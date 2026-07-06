// Package shim implements an in-process MySQL-protocol server that
// answers time-travel queries by translating them into bintrail's
// existing query engine.
//
// Three virtual-schema statement shapes are recognised:
//
//	SELECT * FROM _flashback.<table> AS OF [TIMESTAMP] '<ts>'           [WHERE <col> = <value>]
//	SELECT * FROM _snapshot.<table>  AS OF [TIMESTAMP] '<ts>'           [WHERE <col> = <value>]
//	SELECT * FROM _diff.<table>      BETWEEN '<t1>' AND '<t2>'          WHERE <col> = <value>
//
// On the AS OF shapes, `*` may be replaced with a comma-separated list
// of column names (#313) — only those columns are projected. Missing
// columns become NULL. The `TIMESTAMP` keyword after AS OF is optional
// (#314) and matches the Oracle / SQL Server convention DBAs are
// trained on; bintrail itself never required it.
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
// And a fifth, the bare README-tagline form — time-travel syntax
// directly on a real table, with the AS OF clause ENDING the
// statement — also rewritten to _flashback. Routed by the
// end-anchored ProxySQL rule 990006; see issue #385:
//
//	SELECT * FROM [<schema>.]<table> [WHERE <col> = <value>] AS OF [TIMESTAMP] '<ts>'
//
// _flashback returns the row's state at-or-before the AS OF instant,
//   resolved purely from indexed binlog events (binlog-only).
// _snapshot is the baseline-aware sibling (#355): when `bintrail shim`
//   is started with --baseline-dir / --baseline-s3, single-row
//   _snapshot seeds the row state from the `bintrail baseline` Parquet
//   snapshot and applies post-snapshot events on top, so it can answer
//   for rows that existed at AS OF but were never touched in the
//   retained binlog window. With no baseline source configured it
//   degrades to the binlog-only _flashback behaviour. The two schemas
//   are parsed identically here; the semantic split lives in the
//   handler dispatch (runPointInTime vs runSnapshot).
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
	"strconv"
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
//
// Columns reflects the user's projection: nil means SELECT * (return
// every column in the table's DDL order). A non-nil slice means the
// user listed columns explicitly — only those columns are projected in
// the order they were listed, with NULL where the row image is missing
// a column (same semantic as MySQL after an ALTER TABLE DROP COLUMN).
// Only applies to TypeFlashback / TypeSnapshot; TypeDiff and the
// hint-comment form keep the SELECT * shape (see #313).
type TimeTravelQuery struct {
	Type     QueryType
	Schema   string // taken from the connection's USE'd database
	Table    string
	AsOf     time.Time // for flashback/snapshot
	Since    time.Time // for diff (inclusive lower bound)
	Until    time.Time // for diff (inclusive upper bound)
	PKColumn string
	PKValue  string
	Columns  []string // nil = SELECT *; otherwise user-listed projection
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
	// (the rule_id 990001-990006 set written by `bintrail
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

	// asOfRealProbeRE gates the bare-AS-OF-on-a-real-table form (#385):
	//
	//	SELECT * FROM [<schema>.]<table> [WHERE <col> = <val>] AS OF [TIMESTAMP] '<ts>'
	//
	// It is END-ANCHORED — the statement must *finish* with the AS OF
	// clause — for the same reason hintProbeRE is start-anchored
	// (#288's lesson): "AS OF" inside an arbitrary string literal in
	// the middle of a benign query must not trigger the rewrite path.
	// Deliberately the same semantic as the ProxySQL routing rule
	// (rule_id 990006), so the shim and the router always agree on
	// which statements are this shape. A probe hit with a full-matcher
	// miss IS treated as an intended-but-malformed time-travel query
	// (1064 with grammar help): a statement that *ends* in AS OF '…'
	// unambiguously wanted time travel.
	asOfRealProbeRE = regexp.MustCompile(`(?i)\bAS\s+OF\s+(?:TIMESTAMP\s+)?'[^']*'\s*;?\s*$`)

	// asOfRealRE is the full matcher for the bare form. Capture groups:
	//   1 = schema prefix (without trailing dot) or empty
	//   2 = table
	//   3 = WHERE column (or empty — full-table shape)
	//   4 = WHERE value, quoted or numeric (or empty)
	//   5 = timestamp (between the quotes)
	// Projection is hard-coded `*` (like the hint form; #313's column
	// lists are virtual-schema-only); WHERE precedes AS OF (trailing-only
	// — an AS-OF-before-WHERE variant would foreclose the end anchor,
	// the single strongest false-positive defense).
	asOfRealRE = regexp.MustCompile(
		`(?i)^\s*SELECT\s+\*\s+FROM\s+(?:([A-Za-z_][A-Za-z0-9_]*)\.)?([A-Za-z_][A-Za-z0-9_]*)` +
			`(?:\s+WHERE\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*('[^']*'|-?\d+))?` +
			`\s+AS\s+OF\s+(?:TIMESTAMP\s+)?'([^']+)'\s*;?\s*$`,
	)

	// virtualFromRE is Parse()'s virtual-schema screen, anchored to FROM
	// position so a `_<virtual>.` substring inside a string literal cannot
	// divert a non-virtual query away from the bare-AS-OF path (#385).
	virtualFromRE = regexp.MustCompile(`(?i)\bfrom\s+_(flashback|snapshot|diff)\.`)
)

// mustCompileAsOf builds a regex for `_flashback` / `_snapshot` shapes.
// Capture groups:
//
//	1 = projection: "*" OR a comma-separated identifier list (#313)
//	2 = table
//	3 = timestamp (the optional TIMESTAMP keyword preceding it is
//	    a non-capturing group, #314)
//	4 = WHERE column (or empty)
//	5 = WHERE value (or empty)
//
// The trailing WHERE clause is in an optional non-capturing group so the
// PK-filtered fast path and the full-table path go through the same matcher.
//
// The column-list pattern only accepts bare identifiers separated by
// commas (e.g. `id, email, name`) — backticked, schema-qualified, and
// aliased forms are intentionally out of scope for the demo-footgun
// remediation (#313). They fall through to the existing "malformed
// time-travel query" error.
func mustCompileAsOf(schemaPrefix string) *regexp.Regexp {
	return regexp.MustCompile(
		`(?i)^\s*SELECT\s+(\*|[A-Za-z_][A-Za-z0-9_]*(?:\s*,\s*[A-Za-z_][A-Za-z0-9_]*)*)\s+FROM\s+` +
			schemaPrefix + `\.([A-Za-z_][A-Za-z0-9_]*)` +
			`\s+AS\s+OF\s+(?:TIMESTAMP\s+)?'([^']+)'` +
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

// relativeTimeRE accepts human-friendly relative literals — `'90 seconds
// ago'`, `'5 minutes ago'`, `'2 hours ago'`, `'1 day ago'` — resolved
// against the wall clock at parse time (#350: the appliance demo would
// otherwise force evaluators to compute absolute timestamps by hand
// before their first time-travel query). Weeks/months/years are
// deliberately absent: binlog retention windows are measured in hours
// and days, and month arithmetic is calendar-ambiguous.
var relativeTimeRE = regexp.MustCompile(`(?i)^\s*(\d+)\s+(second|minute|hour|day)s?\s+ago\s*$`)

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

	// Virtual-schema screen, confined to FROM position. A plain
	// strings.Contains screen would fire on `_flashback.`/`_snapshot.`/
	// `_diff.` ANYWHERE — including inside a WHERE-clause string literal —
	// and divert a well-formed bare AS OF query (#385) into the virtual
	// matchers below, which all miss → 1064, even though ProxySQL rule
	// 990006 routed it here expecting time travel. All three virtual
	// matchers require the prefix in FROM position anyway.
	if !virtualFromRE.MatchString(trimmed) {
		// Bare AS OF on a real table (#385). Deliberately gated INSIDE
		// the non-virtual branch: `_flashback.orders AS OF '…'` would
		// otherwise also match asOfRealRE (identifiers may start with
		// `_`, and Go regexp has no negative lookahead) and be
		// misparsed with schema="_flashback". Virtual queries are
		// always claimed by the matchers below instead.
		if asOfRealProbeRE.MatchString(trimmed) {
			return parseAsOfRealTable(trimmed, defaultSchema)
		}
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
			"  SELECT (* | <col>[, <col>...]) FROM _flashback.<table> AS OF [TIMESTAMP] '<ts>' [WHERE <col> = <value>]\n" +
			"  SELECT (* | <col>[, <col>...]) FROM _snapshot.<table>  AS OF [TIMESTAMP] '<ts>' [WHERE <col> = <value>]\n" +
			"  SELECT * FROM _diff.<table>      BETWEEN '<t1>' AND '<t2>' WHERE <col> = <value>\n" +
			"\n" +
			"Notes: the TIMESTAMP keyword is optional. Column lists in the SELECT\n" +
			"clause are supported on _flashback / _snapshot (bare identifiers only;\n" +
			"backticks, schema.column, and aliases are not yet parsed).",
	)
}

// parseAsOfMatch fills a TimeTravelQuery for the _flashback / _snapshot
// shapes (capture groups: 1 projection, 2 table, 3 timestamp, 4 col, 5 value).
func parseAsOfMatch(m []string, t QueryType, schema string) (TimeTravelQuery, error) {
	asOf, err := parseTimeLiteral(m[3])
	if err != nil {
		return TimeTravelQuery{}, fmt.Errorf("invalid AS OF timestamp %q: %w", m[3], err)
	}
	return TimeTravelQuery{
		Type:     t,
		Schema:   schema,
		Table:    m[2],
		AsOf:     asOf,
		PKColumn: m[4],
		PKValue:  stripQuotes(m[5]),
		Columns:  parseColumnList(m[1]),
	}, nil
}

// parseColumnList converts the regex-captured projection group into a
// column-name slice. Returns nil for `*` (SELECT *), so downstream code
// can treat nil as "the table's DDL order, every column".
//
// The input is guaranteed to be a valid projection by the regex (either
// `*` or one or more bare identifiers separated by commas with optional
// whitespace), so we can split-and-trim without re-validation.
func parseColumnList(projection string) []string {
	if projection == "*" {
		return nil
	}
	parts := strings.Split(projection, ",")
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		cols = append(cols, strings.TrimSpace(p))
	}
	return cols
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

// parseTimeLiteral parses a time-travel literal. Zone-less formats (the
// space-separated form, the zone-less RFC-3339-shaped form, and date-only)
// are interpreted as UTC — ParseInLocation with time.UTC treats the literal
// as if it already were UTC, it does not convert from any other zone. Only
// the Z-suffixed RFC 3339 form is unambiguous by construction. Callers whose
// wall clock is not UTC should either use that form or account for the
// offset themselves; there is no per-session override (SET time_zone is
// accepted as handshake noise and has no effect here — see handler.go).
func parseTimeLiteral(s string) (time.Time, error) {
	for _, f := range timeFormats {
		if t, err := time.ParseInLocation(f, s, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	if strings.EqualFold(strings.TrimSpace(s), "now") {
		// 'now' is mainly useful as a _diff upper bound:
		//   BETWEEN '10 minutes ago' AND 'now'
		return time.Now().UTC(), nil
	}
	if m := relativeTimeRE.FindStringSubmatch(s); m != nil {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			// \d+ matched but exceeds int range ("99999999999999999999
			// hours ago") — reject rather than silently truncating.
			return time.Time{}, fmt.Errorf("relative time amount %q out of range", m[1])
		}
		var unit time.Duration
		switch strings.ToLower(m[2]) {
		case "second":
			unit = time.Second
		case "minute":
			unit = time.Minute
		case "hour":
			unit = time.Hour
		case "day":
			unit = 24 * time.Hour
		}
		return time.Now().UTC().Add(-time.Duration(n) * unit), nil
	}
	return time.Time{}, fmt.Errorf("must be one of: %s (zone-less forms are interpreted as UTC), or a relative literal like '5 minutes ago'", strings.Join(timeFormats, ", "))
}

func stripQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}

// parseAsOfRealTable handles the bare time-travel form on a real table
// name (#385) — the README-tagline shape:
//
//	SELECT * FROM [<schema>.]<table> [WHERE <col> = <val>] AS OF [TIMESTAMP] '<ts>'
//
// Like the hint form it rewrites to a TypeFlashback query (binlog-only;
// it does not gain _snapshot's baseline awareness). Only called when the
// end-anchored probe matched, so a full-matcher miss here is an
// intended-but-malformed time-travel statement → non-ErrNotTimeTravel
// error → ER_PARSE_ERROR (1064) with grammar help.
func parseAsOfRealTable(trimmed, defaultSchema string) (TimeTravelQuery, error) {
	m := asOfRealRE.FindStringSubmatch(trimmed)
	if m == nil {
		return TimeTravelQuery{}, fmt.Errorf(
			"malformed time-travel query; expected:\n" +
				"  SELECT * FROM [<schema>.]<table> [WHERE <col> = <value>] AS OF [TIMESTAMP] '<ts>'\n" +
				"\n" +
				"Notes: the projection must be `*` (column lists are supported only on\n" +
				"the _flashback/_snapshot virtual schemas), and the AS OF clause must\n" +
				"end the statement.",
		)
	}
	asOf, err := parseTimeLiteral(m[5])
	if err != nil {
		return TimeTravelQuery{}, fmt.Errorf("invalid AS OF timestamp %q: %w", m[5], err)
	}
	schema := m[1]
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
		Table:    m[2],
		AsOf:     asOf,
		PKColumn: m[3],
		PKValue:  stripQuotes(m[4]),
	}, nil
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
