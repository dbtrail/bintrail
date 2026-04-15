package query

import (
	"strings"
	"testing"
)

func TestParseColumnEq_plainValue(t *testing.T) {
	eq, err := ParseColumnEq("status=active")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if eq.Column != "status" || eq.Value != "active" || eq.IsNull {
		t.Errorf("got %+v, want {status active false}", eq)
	}
}

func TestParseColumnEq_valueContainsEquals(t *testing.T) {
	// Must split on the FIRST '=' only, so JSON-ish values are preserved.
	eq, err := ParseColumnEq(`payload={"k":"v=v"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if eq.Column != "payload" {
		t.Errorf("column: got %q, want payload", eq.Column)
	}
	if eq.Value != `{"k":"v=v"}` {
		t.Errorf("value: got %q", eq.Value)
	}
}

func TestParseColumnEq_nullSentinel(t *testing.T) {
	eq, err := ParseColumnEq("deleted_at=NULL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !eq.IsNull {
		t.Error("expected IsNull=true for literal NULL")
	}
}

func TestParseColumnEq_quotedNullIsNotSentinel(t *testing.T) {
	// Only the bare four-character NULL triggers the sentinel.
	eq, err := ParseColumnEq(`label="NULL"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if eq.IsNull {
		t.Error(`"NULL" (quoted) must not set IsNull`)
	}
	if eq.Value != `"NULL"` {
		t.Errorf("value: got %q", eq.Value)
	}
}

func TestParseColumnEq_missingEquals(t *testing.T) {
	_, err := ParseColumnEq("status")
	if err == nil {
		t.Fatal("expected error for entry without '='")
	}
	if !strings.Contains(err.Error(), "missing '='") {
		t.Errorf("error message: %v", err)
	}
}

func TestParseColumnEq_emptyColumn(t *testing.T) {
	_, err := ParseColumnEq("=value")
	if err == nil {
		t.Fatal("expected error for empty column name")
	}
}

func TestParseColumnEq_unsafeColumn(t *testing.T) {
	cases := []string{
		"col'; DROP TABLE t; --=x",
		`col\=x`,
		"col.name=x", // dots disallowed — keeps path interpolation safe
		"col name=x", // spaces disallowed
	}
	for _, c := range cases {
		if _, err := ParseColumnEq(c); err == nil {
			t.Errorf("expected error for %q", c)
		}
	}
}

func TestParseColumnEqs_firstErrorWins(t *testing.T) {
	_, err := ParseColumnEqs([]string{"ok=1", "bad"})
	if err == nil {
		t.Fatal("expected error from second entry")
	}
}

func TestParseColumnEqs_emptyReturnsNil(t *testing.T) {
	out, err := ParseColumnEqs(nil)
	if err != nil || out != nil {
		t.Errorf("expected (nil, nil), got (%v, %v)", out, err)
	}
}

func TestBuildQuery_columnEq_singleEntry(t *testing.T) {
	opts := Options{
		Schema:   "db",
		Table:    "t",
		ColumnEq: []ColumnEq{{Column: "status", Value: "active"}},
		Limit:    10,
	}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "JSON_UNQUOTE(JSON_EXTRACT(row_after, '$.status'))") {
		t.Errorf("missing row_after clause: %s", q)
	}
	if !strings.Contains(q, "JSON_UNQUOTE(JSON_EXTRACT(row_before, '$.status'))") {
		t.Errorf("missing row_before clause: %s", q)
	}
	// The two halves must be joined by OR inside a single parenthesised group,
	// so the clause doesn't AND with the wrong neighbour after a later filter.
	if !strings.Contains(q, "= ? OR JSON_UNQUOTE") {
		t.Errorf("missing OR connector: %s", q)
	}
	// Value must appear twice (once per side).
	count := 0
	for _, a := range args {
		if s, ok := a.(string); ok && s == "active" {
			count++
		}
	}
	if count != 2 {
		t.Errorf("expected value bound twice, got %d (args=%v)", count, args)
	}
}

func TestBuildQuery_columnEq_multipleEntriesAND(t *testing.T) {
	opts := Options{
		Schema: "db",
		Table:  "t",
		ColumnEq: []ColumnEq{
			{Column: "status", Value: "active"},
			{Column: "order_id", Value: "7"},
		},
		Limit: 10,
	}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "'$.status'") {
		t.Errorf("missing status path: %s", q)
	}
	if !strings.Contains(q, "'$.order_id'") {
		t.Errorf("missing order_id path: %s", q)
	}
	// Outer join uses AND (strings.Join(where, " AND ")).
	if strings.Count(q, " AND ") < 1 {
		t.Errorf("expected multiple clauses joined by AND: %s", q)
	}
	// Two entries × 2 bound args each = 4 value slots, plus schema + table.
	values := 0
	for _, a := range args {
		if s, ok := a.(string); ok && (s == "active" || s == "7") {
			values++
		}
	}
	if values != 4 {
		t.Errorf("expected 4 value bindings, got %d (args=%v)", values, args)
	}
}

func TestBuildQuery_columnEq_nullSentinel(t *testing.T) {
	opts := Options{
		Schema:   "db",
		Table:    "t",
		ColumnEq: []ColumnEq{{Column: "deleted_at", IsNull: true}},
		Limit:    10,
	}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "JSON_TYPE(JSON_EXTRACT(row_after, '$.deleted_at')) = 'NULL'") {
		t.Errorf("missing row_after null check: %s", q)
	}
	if !strings.Contains(q, "JSON_TYPE(JSON_EXTRACT(row_before, '$.deleted_at')) = 'NULL'") {
		t.Errorf("missing row_before null check: %s", q)
	}
	// No positional arg for the null branch.
	for _, a := range args {
		if s, ok := a.(string); ok && s == "NULL" {
			t.Errorf("null sentinel must not bind the literal string: %v", args)
		}
	}
}
