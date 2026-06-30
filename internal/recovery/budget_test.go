package recovery

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

func TestEstimateScriptBytes(t *testing.T) {
	big := strings.Repeat("x", 1<<20) // 1 MiB string value (e.g. a base64 BLOB)

	cases := []struct {
		name    string
		rows    []query.ResultRow
		wantMin int64 // estimate must be at least this
		wantMax int64 // and at most this (no wild over-count)
	}{
		{name: "empty", rows: nil, wantMin: 0, wantMax: 0},
		{
			name:    "small narrow insert",
			rows:    []query.ResultRow{{EventType: event.EventInsert, PKValues: "1", RowAfter: map[string]any{"id": float64(1), "name": "ann"}}},
			wantMin: 1,    // at least the PK + "name"+"ann" bytes
			wantMax: 1024, // nowhere near a KB
		},
		{
			name:    "delete counts the before image (the rendered INSERT)",
			rows:    []query.ResultRow{{EventType: event.EventDelete, PKValues: "7", RowBefore: map[string]any{"id": float64(7), "blob": big}}},
			wantMin: 1 << 20,
			wantMax: (1 << 20) + 1<<10,
		},
		{
			name:    "insert counts the after image (conservative WHERE bound)",
			rows:    []query.ResultRow{{EventType: event.EventInsert, PKValues: "7", RowAfter: map[string]any{"id": float64(7), "blob": big}}},
			wantMin: 1 << 20,
			wantMax: (1 << 20) + 1<<10,
		},
		{
			// A reverse UPDATE renders SET(before) + WHERE(after) (buildUpdate
			// keys the WHERE on row_after), so the fat after image MUST count —
			// under-counting it would let an oversized UPDATE script slip the guard.
			name:    "update counts both images (SET before + WHERE after)",
			rows:    []query.ResultRow{{EventType: event.EventUpdate, PKValues: "1", RowBefore: map[string]any{"k": "v"}, RowAfter: map[string]any{"blob": big}}},
			wantMin: 1 << 20, // the 1 MiB after image is counted (WHERE clause)
			wantMax: (1 << 20) + 1<<10,
		},
		{
			name: "N deletes roughly N times one",
			rows: []query.ResultRow{
				{EventType: event.EventDelete, PKValues: "1", RowBefore: map[string]any{"blob": big}},
				{EventType: event.EventDelete, PKValues: "2", RowBefore: map[string]any{"blob": big}},
				{EventType: event.EventDelete, PKValues: "3", RowBefore: map[string]any{"blob": big}},
			},
			wantMin: 3 << 20,
			wantMax: (3 << 20) + 1<<10,
		},
		{
			// A fat JSON column decodes to nested maps/arrays (the #652 class);
			// the estimator must recurse into them, not fall to the 16-byte
			// scalar default, or it under-counts the exact payload #654 targets.
			name: "nested JSON column recurses (map + array)",
			rows: []query.ResultRow{{EventType: event.EventDelete, PKValues: "1", RowBefore: map[string]any{
				"doc": map[string]any{"a": big, "b": []any{big, "x"}},
			}}},
			wantMin: 2 << 20, // both nested 1 MiB values counted via recursion
			wantMax: (2 << 20) + 1<<10,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EstimateScriptBytes(tc.rows)
			if got < tc.wantMin || got > tc.wantMax {
				t.Fatalf("EstimateScriptBytes = %d, want in [%d, %d]", got, tc.wantMin, tc.wantMax)
			}
		})
	}
}

// TestNewDefaultsBudget pins the zero-config guard (#654): New / NewForDialect
// must seed maxScriptBytes with DefaultMaxScriptBytes. The MCP recover tool, the
// console, and recover-cascade never call SetMaxScriptBytes, so if a constructor
// stopped defaulting the field (zero value = unlimited) the guard would silently
// vanish for them while every SetMaxScriptBytes-using test still passed.
func TestNewDefaultsBudget(t *testing.T) {
	if got := New(nil, nil).maxScriptBytes; got != DefaultMaxScriptBytes {
		t.Errorf("New() maxScriptBytes = %d, want DefaultMaxScriptBytes (%d)", got, DefaultMaxScriptBytes)
	}
	if got := NewForDialect(nil, nil, PostgresDialect).maxScriptBytes; got != DefaultMaxScriptBytes {
		t.Errorf("NewForDialect() maxScriptBytes = %d, want DefaultMaxScriptBytes (%d)", got, DefaultMaxScriptBytes)
	}
}

// TestEstimateScriptBytes_monotonic confirms adding rows never lowers the estimate.
func TestEstimateScriptBytes_monotonic(t *testing.T) {
	row := query.ResultRow{EventType: event.EventInsert, PKValues: "1", RowAfter: map[string]any{"blob": strings.Repeat("y", 4096)}}
	var prev int64 = -1
	for n := 0; n <= 5; n++ {
		rows := make([]query.ResultRow, n)
		for i := range rows {
			rows[i] = row
		}
		got := EstimateScriptBytes(rows)
		if got < prev {
			t.Fatalf("estimate decreased: n=%d got=%d prev=%d", n, got, prev)
		}
		prev = got
	}
}

// TestGenerateSQLFromRows_budget verifies the fail-loud refusal (#654): over
// budget GenerateSQLFromRows returns a *ScriptBudgetError and writes NOTHING
// (refusal before rendering, so no truncated script can reach the writer).
func TestGenerateSQLFromRows_budget(t *testing.T) {
	big := strings.Repeat("z", 1<<20) // 1 MiB payload
	rows := []query.ResultRow{{
		EventType:  event.EventInsert, // INSERT → DELETE reversal
		SchemaName: "db",
		TableName:  "t",
		PKValues:   "1",
		RowAfter:   map[string]any{"id": float64(1), "blob": big},
	}}

	t.Run("over budget refuses before writing", func(t *testing.T) {
		gen := New(nil, nil) // nil resolver → all-columns fallback (DB-free)
		gen.SetMaxScriptBytes(1024)
		var buf bytes.Buffer
		n, err := gen.GenerateSQLFromRows(rows, &buf)
		var be *ScriptBudgetError
		if !errors.As(err, &be) {
			t.Fatalf("want *ScriptBudgetError, got %v", err)
		}
		if n != 0 {
			t.Fatalf("want 0 statements on refusal, got %d", n)
		}
		if buf.Len() != 0 {
			t.Fatalf("refusal must write nothing, wrote %d bytes", buf.Len())
		}
		if be.EstimatedBytes <= be.Budget {
			t.Fatalf("ScriptBudgetError fields look wrong: est=%d budget=%d", be.EstimatedBytes, be.Budget)
		}
	})

	t.Run("budget 0 disables the guard", func(t *testing.T) {
		gen := New(nil, nil)
		gen.SetMaxScriptBytes(0) // unlimited
		var buf bytes.Buffer
		_, err := gen.GenerateSQLFromRows(rows, &buf)
		var be *ScriptBudgetError
		if errors.As(err, &be) {
			t.Fatalf("budget 0 must not trip the guard, got %v", err)
		}
	})

	t.Run("under budget renders", func(t *testing.T) {
		small := []query.ResultRow{{
			EventType:  event.EventInsert,
			SchemaName: "db",
			TableName:  "t",
			PKValues:   "1",
			RowAfter:   map[string]any{"id": float64(1), "name": "ann"},
		}}
		gen := New(nil, nil)
		gen.SetMaxScriptBytes(DefaultMaxScriptBytes)
		var buf bytes.Buffer
		n, err := gen.GenerateSQLFromRows(small, &buf)
		if err != nil {
			t.Fatalf("under budget should render, got %v", err)
		}
		if n != 1 || buf.Len() == 0 {
			t.Fatalf("want 1 statement and non-empty output, got n=%d len=%d", n, buf.Len())
		}
	})
}

func TestScriptBudgetError_message(t *testing.T) {
	e := &ScriptBudgetError{EstimatedBytes: 3 << 30, Budget: 2 << 30}
	msg := e.Error()
	for _, want := range []string{"refusing", "3.00GB", "2.00GB", "budget"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q: %s", want, msg)
		}
	}
}
