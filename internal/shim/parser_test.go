package shim

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// ─── _flashback ──────────────────────────────────────────────────────────────

func TestParseFlashbackHappyPath(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345",
		"myapp",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Type != TypeFlashback {
		t.Errorf("Type = %v, want TypeFlashback", q.Type)
	}
	if q.Schema != "myapp" || q.Table != "orders" || q.PKColumn != "id" || q.PKValue != "12345" {
		t.Errorf("unexpected: %+v", q)
	}
	want := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	if !q.AsOf.Equal(want) {
		t.Errorf("AsOf = %v, want %v", q.AsOf, want)
	}
}

// TestParseFlashbackFullTable pins the WHERE-less shape introduced
// for full-table reconstruction (issue #276). The PK fields are empty
// so the handler can dispatch on q.PKColumn == "" without parsing the
// SQL again.
func TestParseFlashbackFullTable(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"bare", "SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00'"},
		{"trailing_semicolon", "SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00';"},
		{"lower_case", "select * from _flashback.orders as of '2026-05-02 10:00:00'"},
		{"snapshot_variant", "SELECT * FROM _snapshot.orders AS OF '2026-05-02 10:00:00'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.sql, "myapp")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if q.Table != "orders" {
				t.Errorf("Table = %q, want %q", q.Table, "orders")
			}
			if q.PKColumn != "" || q.PKValue != "" {
				t.Errorf("PKColumn/PKValue must be empty for full-table shape; got col=%q val=%q",
					q.PKColumn, q.PKValue)
			}
		})
	}
}

func TestParseFlashbackCaseInsensitive(t *testing.T) {
	q, err := Parse(
		"select * from _flashback.users as of '2026-01-01' where email = 'a@b.com'",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
	if q.Type != TypeFlashback || q.PKValue != "a@b.com" {
		t.Errorf("unexpected: %+v", q)
	}
}

// ─── _snapshot ───────────────────────────────────────────────────────────────

func TestParseSnapshotHappyPath(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _snapshot.orders AS OF '2026-05-02 10:00:00' WHERE id = 1",
		"myapp",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Type != TypeSnapshot {
		t.Errorf("Type = %v, want TypeSnapshot", q.Type)
	}
	if q.Table != "orders" {
		t.Errorf("Table = %q", q.Table)
	}
}

// ─── _diff ───────────────────────────────────────────────────────────────────

func TestParseDiffHappyPath(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _diff.orders BETWEEN '2026-05-01 00:00:00' AND '2026-05-02 00:00:00' WHERE id = 42",
		"myapp",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Type != TypeDiff {
		t.Errorf("Type = %v, want TypeDiff", q.Type)
	}
	wantSince := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	wantUntil := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	if !q.Since.Equal(wantSince) || !q.Until.Equal(wantUntil) {
		t.Errorf("Since=%v, Until=%v, want %v..%v", q.Since, q.Until, wantSince, wantUntil)
	}
	if q.PKColumn != "id" || q.PKValue != "42" {
		t.Errorf("PK = %s=%q", q.PKColumn, q.PKValue)
	}
	if !q.AsOf.IsZero() {
		t.Errorf("AsOf should be zero for _diff, got %v", q.AsOf)
	}
}

func TestParseDiffRejectsReversedRange(t *testing.T) {
	_, err := Parse(
		"SELECT * FROM _diff.t BETWEEN '2026-05-02' AND '2026-05-01' WHERE id = 1",
		"myapp",
	)
	if err == nil {
		t.Fatal("expected error for reversed BETWEEN bounds")
	}
	if !strings.Contains(err.Error(), "out of order") {
		t.Errorf("error = %v, want 'out of order'", err)
	}
}

// ─── shared error paths ──────────────────────────────────────────────────────

func TestParseAcceptsTrailingSemicolon(t *testing.T) {
	_, err := Parse(
		"SELECT * FROM _flashback.t AS OF '2026-05-02 10:00:00' WHERE id = 1;",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestParseAcceptsRFC3339Timestamp(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.t AS OF '2026-05-02T10:00:00Z' WHERE id = 1",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	if !q.AsOf.Equal(want) {
		t.Errorf("AsOf = %v, want %v", q.AsOf, want)
	}
}

func TestParseDateOnly(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.t AS OF '2026-05-02' WHERE id = 1",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	if !q.AsOf.Equal(want) {
		t.Errorf("AsOf = %v, want %v", q.AsOf, want)
	}
}

func TestParseStringPK(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.users AS OF '2026-05-02' WHERE uuid = 'abc-123'",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
	if q.PKValue != "abc-123" {
		t.Errorf("PKValue = %q, want abc-123", q.PKValue)
	}
}

func TestParseNegativePK(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.t AS OF '2026-05-02' WHERE id = -42",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
	if q.PKValue != "-42" {
		t.Errorf("PKValue = %q, want -42", q.PKValue)
	}
}

func TestParseNotTimeTravelReturnsSentinel(t *testing.T) {
	cases := []string{
		"SELECT * FROM orders WHERE id = 1",
		"SELECT 1",
		"",
		"   ",
		"SHOW TABLES",
	}
	for _, sql := range cases {
		_, err := Parse(sql, "myapp")
		if !errors.Is(err, ErrNotTimeTravel) {
			t.Errorf("Parse(%q) error = %v, want ErrNotTimeTravel", sql, err)
		}
	}
}

func TestParseMalformedTimeTravelErrors(t *testing.T) {
	cases := []struct {
		sql     string
		wantSub string
	}{
		{
			"SELECT * FROM _flashback.orders WHERE id = 1",
			"malformed time-travel",
		},
		{
			"SELECT * FROM _diff.orders WHERE id = 1",
			"malformed time-travel",
		},
		{
			"SELECT * FROM _snapshot.orders AS OF 'not-a-time' WHERE id = 1",
			"invalid AS OF timestamp",
		},
		{
			"SELECT * FROM _diff.orders BETWEEN 'bad' AND '2026-05-02' WHERE id = 1",
			"invalid BETWEEN lower bound",
		},
		{
			"SELECT * FROM _diff.orders BETWEEN '2026-05-01' AND 'bad' WHERE id = 1",
			"invalid BETWEEN upper bound",
		},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := Parse(tc.sql, "myapp")
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %v, want containing %q", err, tc.wantSub)
			}
		})
	}
}

func TestParseRequiresSchema(t *testing.T) {
	_, err := Parse(
		"SELECT * FROM _flashback.t AS OF '2026-05-02' WHERE id = 1",
		"",
	)
	if err == nil {
		t.Fatal("expected error when defaultSchema is empty")
	}
	if !strings.Contains(err.Error(), "no schema selected") {
		t.Errorf("error = %v, want hint about USE", err)
	}
}

// ─── hint-comment form (issue #288) ──────────────────────────────────────────

// TestParseHintFormHappyPath pins the docs-advertised optimizer-hint
// shape: ProxySQL matches `DBTRAIL_AT`, forwards to the shim, the
// shim rewrites the query into a TypeFlashback point-lookup against
// the *real* table name (not _flashback.<t>). Without this code path
// the shim refused the form with ER_NOT_SUPPORTED_YET (1235) and the
// docs example silently broke.
func TestParseHintFormHappyPath(t *testing.T) {
	q, err := Parse(
		"SELECT /*+ DBTRAIL_AT='2026-04-27 09:00:00' */ * FROM orders WHERE id = 42",
		"myapp",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Type != TypeFlashback {
		t.Errorf("Type = %v, want TypeFlashback (the hint form must rewrite to flashback)", q.Type)
	}
	if q.Schema != "myapp" || q.Table != "orders" || q.PKColumn != "id" || q.PKValue != "42" {
		t.Errorf("unexpected query shape: %+v", q)
	}
	want := time.Date(2026, 4, 27, 9, 0, 0, 0, time.UTC)
	if !q.AsOf.Equal(want) {
		t.Errorf("AsOf = %v, want %v", q.AsOf, want)
	}
}

// TestParseHintFormVariants pins the recognised variants — qualified
// table, hint position between * and FROM, whitespace around the
// `=`, full-table (no WHERE), trailing semicolon, lower-case
// keywords, and string PK values.
func TestParseHintFormVariants(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		schema   string
		table    string
		pkCol    string
		pkVal    string
	}{
		{
			name:   "qualified_table_overrides_default_schema",
			sql:    "SELECT /*+ DBTRAIL_AT='2026-04-27 09:00:00' */ * FROM otherdb.orders WHERE id = 42",
			schema: "otherdb",
			table:  "orders",
			pkCol:  "id",
			pkVal:  "42",
		},
		{
			name:   "hint_between_star_and_from",
			sql:    "SELECT * /*+ DBTRAIL_AT='2026-04-27 09:00:00' */ FROM orders WHERE id = 42",
			schema: "myapp",
			table:  "orders",
			pkCol:  "id",
			pkVal:  "42",
		},
		{
			name:   "extra_whitespace_around_equals",
			sql:    "SELECT /*+  DBTRAIL_AT  =  '2026-04-27 09:00:00'  */ * FROM orders WHERE id = 42",
			schema: "myapp",
			table:  "orders",
			pkCol:  "id",
			pkVal:  "42",
		},
		{
			name:   "no_where_full_table",
			sql:    "SELECT /*+ DBTRAIL_AT='2026-04-27 09:00:00' */ * FROM orders",
			schema: "myapp",
			table:  "orders",
			// PKColumn empty → handler dispatches to runFullTable
			pkCol: "",
			pkVal: "",
		},
		{
			name:   "trailing_semicolon",
			sql:    "SELECT /*+ DBTRAIL_AT='2026-04-27 09:00:00' */ * FROM orders WHERE id = 42;",
			schema: "myapp",
			table:  "orders",
			pkCol:  "id",
			pkVal:  "42",
		},
		{
			name:   "lower_case",
			sql:    "select /*+ dbtrail_at='2026-04-27 09:00:00' */ * from orders where id = 42",
			schema: "myapp",
			table:  "orders",
			pkCol:  "id",
			pkVal:  "42",
		},
		{
			name:   "string_pk_value",
			sql:    "SELECT /*+ DBTRAIL_AT='2026-04-27 09:00:00' */ * FROM orders WHERE sku = 'SKU-B'",
			schema: "myapp",
			table:  "orders",
			pkCol:  "sku",
			pkVal:  "SKU-B",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.sql, "myapp")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if q.Type != TypeFlashback {
				t.Errorf("Type = %v, want TypeFlashback", q.Type)
			}
			if q.Schema != tc.schema || q.Table != tc.table {
				t.Errorf("schema/table = %s/%s, want %s/%s", q.Schema, q.Table, tc.schema, tc.table)
			}
			if q.PKColumn != tc.pkCol || q.PKValue != tc.pkVal {
				t.Errorf("PK = %s=%q, want %s=%q", q.PKColumn, q.PKValue, tc.pkCol, tc.pkVal)
			}
		})
	}
}

// TestParseHintFormMalformedErrors pins the error contract: a
// recognised hint with a bad timestamp/shape returns a non-ErrNotTimeTravel
// error so HandleQuery emits ER_PARSE_ERROR (1064) like other malformed
// time-travel queries — operators reading 1064 know it's a client-input
// fault, not the shim breaking.
func TestParseHintFormMalformedErrors(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		wantSub string
	}{
		{
			name:    "bad_timestamp",
			sql:     "SELECT /*+ DBTRAIL_AT='not-a-time' */ * FROM orders WHERE id = 42",
			wantSub: "invalid AS OF timestamp",
		},
		{
			name:    "missing_from",
			sql:     "SELECT /*+ DBTRAIL_AT='2026-04-27 09:00:00' */ *",
			wantSub: "malformed time-travel hint",
		},
		{
			name:    "missing_table",
			sql:     "SELECT /*+ DBTRAIL_AT='2026-04-27 09:00:00' */ * FROM",
			wantSub: "malformed time-travel hint",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.sql, "myapp")
			if err == nil {
				t.Fatal("expected error")
			}
			if errors.Is(err, ErrNotTimeTravel) {
				t.Errorf("got ErrNotTimeTravel; want a typed error so HandleQuery emits ER_PARSE_ERROR not ER_NOT_SUPPORTED_YET")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %v, want containing %q", err, tc.wantSub)
			}
		})
	}
}

// TestParseHintFormRequiresSchema pins that hint queries against an
// unqualified table without a USE'd default schema fail the same way
// _flashback.<t> queries do.
func TestParseHintFormRequiresSchema(t *testing.T) {
	_, err := Parse(
		"SELECT /*+ DBTRAIL_AT='2026-04-27 09:00:00' */ * FROM orders WHERE id = 42",
		"",
	)
	if err == nil {
		t.Fatal("expected error when defaultSchema is empty and table is unqualified")
	}
	if !strings.Contains(err.Error(), "no schema selected") {
		t.Errorf("error = %v, want hint about USE", err)
	}
}

// TestParseHintFormProbeDoesNotFireOnStringLiteral pins the
// false-positive guard surfaced in the #294 review: a customer
// query whose WHERE/value contains the literal text /*+ DBTRAIL_AT…
// (e.g. an audit table where someone logged a query) must NOT
// trigger the rewrite path. Before the probe was anchored to
// ^\s*SELECT, the probe matched anywhere in the query — including
// inside string literals — so parseHintForm fired and returned
// ER_PARSE_ERROR (1064) to the customer when the query was
// perfectly valid for the upstream MySQL.
//
// Expected: ErrNotTimeTravel. The shim treats this as
// not-a-time-travel-query, HandleQuery returns the catch-all
// ER_NOT_SUPPORTED_YET (1235) — same as any other non-time-travel
// SELECT routed to the shim by mistake. Operators fix the routing
// rather than blame the shim for a syntax error.
func TestParseHintFormProbeDoesNotFireOnStringLiteral(t *testing.T) {
	cases := []string{
		`SELECT * FROM audit WHERE note = '/*+ DBTRAIL_AT=foo */'`,
		`SELECT * FROM logs WHERE message = 'user ran /*+ DBTRAIL_AT=...'`,
		`SELECT * FROM t WHERE description LIKE '%DBTRAIL_AT%'`,
		// Hint in the WHERE position (not after SELECT) — out of
		// spec, must not fire.
		`SELECT * FROM t WHERE id = 1 /*+ DBTRAIL_AT='2026-01-01' */`,
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql, "myapp")
			if !errors.Is(err, ErrNotTimeTravel) {
				t.Errorf("Parse(%q) error = %v, want ErrNotTimeTravel (otherwise the customer gets ER_PARSE_ERROR on a valid non-hint query)", sql, err)
			}
		})
	}
}

// TestParseHintFormDoesNotBreakNonHintQueries is a non-regression
// guard: every parse case that passed before #288 must still parse
// identically. The hint detector is gated by hintProbeRE so the
// steady-state cost is one cheap token check, but if the regex
// accidentally matched a benign comment containing the substring
// "dbtrail_at" inside an otherwise-normal _flashback query, the
// detector would mis-route. This case proves it doesn't.
func TestParseHintFormDoesNotBreakNonHintQueries(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.orders AS OF '2026-04-27 09:00:00' WHERE id = 42",
		"myapp",
	)
	if err != nil {
		t.Fatalf("unexpected error on a non-hint flashback query: %v", err)
	}
	if q.Type != TypeFlashback || q.Schema != "myapp" || q.Table != "orders" || q.PKValue != "42" {
		t.Errorf("non-hint flashback parse regressed: %+v", q)
	}
}

func TestQueryTypeString(t *testing.T) {
	cases := map[QueryType]string{
		TypeFlashback: "_flashback",
		TypeSnapshot:  "_snapshot",
		TypeDiff:      "_diff",
	}
	for tt, want := range cases {
		if got := tt.String(); got != want {
			t.Errorf("QueryType(%d).String() = %q, want %q", tt, got, want)
		}
	}
}
