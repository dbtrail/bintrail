package cascade

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// TestRefKeyChanged pins THE gate of the ON UPDATE cascade path (#1002): only an
// UPDATE that actually moved a referenced key can have cascaded, so an UPDATE of
// unrelated columns must synthesize nothing. NULL-ness is compared before the
// rendered values because valToString maps both nil and "" to "" — without the
// nil check a NULL → empty-string change (a real key move) would read as
// unchanged and silently skip its cascade.
func TestRefKeyChanged(t *testing.T) {
	cases := []struct {
		name           string
		oldVal, newVal any
		want           bool
	}{
		{"identical numbers", json.Number("1"), json.Number("1"), false},
		{"moved number", json.Number("1"), json.Number("99"), true},
		{"number across encodings", json.Number("1"), float64(1), false},
		{"identical strings", "A", "A", false},
		{"moved string", "A", "B", true},
		{"both NULL", nil, nil, false},
		{"NULL to value", nil, "A", true},
		{"value to NULL", "A", nil, true},
		{"NULL to empty string is a real move", nil, "", true},
		{"empty string to NULL is a real move", "", nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := refKeyChanged(c.oldVal, c.newVal); got != c.want {
				t.Errorf("refKeyChanged(%#v, %#v) = %v, want %v", c.oldVal, c.newVal, got, c.want)
			}
		})
	}
}

// TestLoadCascadeClosure pins the cross-schema FK loader (#833). Scoping the FK
// graph by the child's own schema silently dropped a child living in a different
// schema, so its cascade victims were never synthesized and the run exited 0
// "complete" — silent data-loss. loadCascadeClosure scopes by the PARENT schema
// (referenced_schema_name) and expands the referenced-schema frontier through the
// child schemas of CASCADE/SET NULL edges, so direct AND multi-level cross-schema
// children are loaded. The DB access is injected so this needs no database.
func TestLoadCascadeClosure(t *testing.T) {
	fk := func(childSchema, childTable, parentSchema, parentTable, rule string) CascadeFK {
		return CascadeFK{
			Schema: childSchema, Table: childTable, ConstraintName: "fk_" + childTable, Column: "pid",
			ReferencedSchema: parentSchema, ReferencedTable: parentTable, ReferencedColumn: "id",
			DeleteRule: rule, UpdateRule: "RESTRICT",
		}
	}
	edgeKey := func(f CascadeFK) string {
		return f.Schema + "." + f.Table + "->" + f.ReferencedSchema + "." + f.ReferencedTable
	}

	// graph indexes edges by their referenced (parent) schema — the frontier key.
	byRef := func(edges ...CascadeFK) map[string][]CascadeFK {
		m := map[string][]CascadeFK{}
		for _, e := range edges {
			m[e.ReferencedSchema] = append(m[e.ReferencedSchema], e)
		}
		return m
	}
	loaderFor := func(graph map[string][]CascadeFK) referencedSchemaLoader {
		return func(_ context.Context, refSchemas []string) ([]CascadeFK, error) {
			var out []CascadeFK
			for _, s := range refSchemas {
				out = append(out, graph[s]...)
			}
			return out, nil
		}
	}

	cases := []struct {
		name  string
		graph map[string][]CascadeFK
		want  []string // edgeKey set expected in the closure from parent schema "a"
	}{
		{
			name:  "direct cross-schema child is loaded (the #833 regression)",
			graph: byRef(fk("b", "reports", "a", "orders", "CASCADE")),
			want:  []string{"b.reports->a.orders"},
		},
		{
			name: "multi-level cross-schema grandchild in a third schema",
			graph: byRef(
				fk("b", "reports", "a", "orders", "CASCADE"),
				fk("c", "lines", "b", "reports", "CASCADE"),
			),
			want: []string{"b.reports->a.orders", "c.lines->b.reports"},
		},
		{
			name: "same-schema multi-level is loaded by the first query, unchanged",
			graph: byRef(
				fk("a", "orders", "a", "customers", "CASCADE"),
				fk("a", "lines", "a", "orders", "CASCADE"),
			),
			want: []string{"a.orders->a.customers", "a.lines->a.orders"},
		},
		{
			name: "RESTRICT cross-schema child does not widen the frontier",
			graph: byRef(
				fk("b", "reports", "a", "orders", "RESTRICT"),
				fk("c", "lines", "b", "reports", "CASCADE"), // unreachable: b is a RESTRICT child
			),
			// The RESTRICT edge is still returned (SynthesizeVictims gates on the rule),
			// but schema b is never scoped, so c.lines is not loaded.
			want: []string{"b.reports->a.orders"},
		},
		{
			name: "cycle terminates (a<->b) and dedups edges",
			graph: byRef(
				fk("b", "t1", "a", "t0", "CASCADE"),
				fk("a", "t2", "b", "t1", "CASCADE"),
			),
			want: []string{"b.t1->a.t0", "a.t2->b.t1"},
		},
	}
	for _, c := range cases {
		got, err := loadCascadeClosure(context.Background(), "a", loaderFor(c.graph))
		if err != nil {
			t.Fatalf("%s: loadCascadeClosure: %v", c.name, err)
		}
		var keys []string
		for _, e := range got {
			keys = append(keys, edgeKey(e))
		}
		sort.Strings(keys)
		want := append([]string(nil), c.want...)
		sort.Strings(want)
		if len(keys) != len(want) {
			t.Fatalf("%s: got edges %v, want %v", c.name, keys, want)
		}
		for i := range want {
			if keys[i] != want[i] {
				t.Fatalf("%s: got edges %v, want %v", c.name, keys, want)
			}
		}
	}
}

// TestLoadCascadeClosure_updateRuleWidensFrontier is #833 one rule over (#1002):
// an ON UPDATE CASCADE child can itself have its key rewritten and cascade
// further, so its schema must widen the referenced-schema frontier exactly like
// an ON DELETE one. Scoping the walk on delete_rule alone left a multi-level
// cross-schema ON UPDATE cascade unloaded — the same silent under-recovery.
func TestLoadCascadeClosure_updateRuleWidensFrontier(t *testing.T) {
	edge := func(childSchema, childTable, parentSchema, parentTable, delRule, updRule string) CascadeFK {
		return CascadeFK{
			Schema: childSchema, Table: childTable, ConstraintName: "fk_" + childTable, Column: "pid",
			ReferencedSchema: parentSchema, ReferencedTable: parentTable, ReferencedColumn: "id",
			DeleteRule: delRule, UpdateRule: updRule,
		}
	}
	graph := map[string][]CascadeFK{
		// b.reports cascades ONLY on update; c.lines hangs off it.
		"a": {edge("b", "reports", "a", "orders", "RESTRICT", "CASCADE")},
		"b": {edge("c", "lines", "b", "reports", "RESTRICT", "SET NULL")},
	}
	load := func(_ context.Context, refSchemas []string) ([]CascadeFK, error) {
		var out []CascadeFK
		for _, s := range refSchemas {
			out = append(out, graph[s]...)
		}
		return out, nil
	}
	got, err := loadCascadeClosure(context.Background(), "a", load)
	if err != nil {
		t.Fatalf("loadCascadeClosure: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("an ON UPDATE cascading child must widen the frontier to its schema; got %d edges: %+v", len(got), got)
	}
	if got[1].Schema != "c" || got[1].Table != "lines" {
		t.Errorf("the second-level ON UPDATE edge was not loaded: %+v", got)
	}
}

// TestLoadCascadeClosureLoaderError surfaces a loader failure instead of returning a
// partial closure — a cascade recovery must never silently under-load its FK graph.
func TestLoadCascadeClosureLoaderError(t *testing.T) {
	boom := errors.New("index unreachable")
	_, err := loadCascadeClosure(context.Background(), "a",
		func(context.Context, []string) ([]CascadeFK, error) { return nil, boom })
	if !errors.Is(err, boom) {
		t.Fatalf("want loader error propagated, got %v", err)
	}
}

// TestValToString covers every branch of the value renderer that backs the
// re-parented comparison. The index read path decodes numbers as json.Number
// (UseNumber, #496) — that is the production type, and a plain float64 would
// lose precision on a BIGINT > 2^53, so the json.Number branch is load-bearing.
func TestValToString(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, ""},
		{"string", "abc", "abc"},
		{"json.Number int", json.Number("42"), "42"},
		{"json.Number bigint > 2^53", json.Number("9007199254740993"), "9007199254740993"},
		{"json.Number negative", json.Number("-7"), "-7"},
		{"float64 integral", float64(1), "1"},
		{"float64 fractional", 1.5, "1.5"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"bytes", []byte("xy"), "xy"},
	}
	for _, c := range cases {
		if got := valToString(c.in); got != c.want {
			t.Errorf("%s: valToString(%#v) = %q, want %q", c.name, c.in, got, c.want)
		}
	}
}

// TestFKColumnAbsentFromAll covers the child-side DDL-skew detector (#832). When
// a cascade older than a child FK-column rename is recovered, the candidate scan
// (ColumnEq on the LATEST snapshot's column name) matches 0 rows against events
// keyed by the OLD name — an outcome indistinguishable from "no children
// existed". The probe samples the child images without the FK filter and flags
// skew only when the snapshot's column name is absent from every sampled image.
func TestFKColumnAbsentFromAll(t *testing.T) {
	rowAfter := func(m map[string]any) query.ResultRow { return query.ResultRow{RowAfter: m} }
	rowBefore := func(m map[string]any) query.ResultRow { return query.ResultRow{RowBefore: m} }

	cases := []struct {
		name   string
		col    string
		sample []query.ResultRow
		want   bool
	}{
		{
			name:   "empty sample is inconclusive (no children, not skew)",
			col:    "parent_id",
			sample: nil,
			want:   false,
		},
		{
			name: "renamed FK column absent from all after-images → skew",
			col:  "parent_id", // snapshot name; events use the old "pid"
			sample: []query.ResultRow{
				rowAfter(map[string]any{"id": json.Number("1"), "pid": json.Number("9")}),
				rowAfter(map[string]any{"id": json.Number("2"), "pid": json.Number("9")}),
			},
			want: true,
		},
		{
			name: "renamed FK column absent from delete before-images → skew",
			col:  "parent_id",
			sample: []query.ResultRow{
				rowBefore(map[string]any{"id": json.Number("1"), "pid": json.Number("9")}),
			},
			want: true,
		},
		{
			name: "column present in after-image → not skew",
			col:  "parent_id",
			sample: []query.ResultRow{
				rowAfter(map[string]any{"id": json.Number("1"), "parent_id": json.Number("9")}),
			},
			want: false,
		},
		{
			name: "column present in before-image → not skew",
			col:  "parent_id",
			sample: []query.ResultRow{
				rowBefore(map[string]any{"id": json.Number("1"), "parent_id": json.Number("9")}),
			},
			want: false,
		},
		{
			name: "mixed: at least one image carries the column → not skew",
			col:  "parent_id",
			sample: []query.ResultRow{
				rowAfter(map[string]any{"id": json.Number("1"), "pid": json.Number("9")}),
				rowAfter(map[string]any{"id": json.Number("2"), "parent_id": json.Number("9")}),
			},
			want: false,
		},
		{
			name: "column present but NULL-valued still counts as present (not skew)",
			col:  "parent_id",
			sample: []query.ResultRow{
				rowAfter(map[string]any{"id": json.Number("1"), "parent_id": nil}),
			},
			want: false,
		},
		{
			name:   "sample rows with no images at all is inconclusive",
			col:    "parent_id",
			sample: []query.ResultRow{{}, {}},
			want:   false,
		},
	}
	for _, c := range cases {
		if got := fkColumnAbsentFromAll(c.col, c.sample); got != c.want {
			t.Errorf("%s: fkColumnAbsentFromAll(%q, …) = %v, want %v", c.name, c.col, got, c.want)
		}
	}
}

// TestDedupVictimsNewest pins the cross-root collapse (#831): the same
// (schema, table, pk) emitted under two roots keeps ONE victim carrying the
// newest-timestamp image, in first-seen order; distinct keys are untouched.
func TestDedupVictimsNewest(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	mk := func(table, pk string, ts time.Time, val string) query.ResultRow {
		return query.ResultRow{SchemaName: "s", TableName: table, PKValues: pk,
			EventTimestamp: ts, RowBefore: map[string]any{"v": val}}
	}
	got := dedupVictimsNewest([]query.ResultRow{
		mk("a", "1", t0, "stale"),
		mk("a", "2", t0, "only"),
		mk("a", "1", t0.Add(time.Hour), "newest"), // same key, newer -> replaces in place
		mk("b", "1", t0.Add(-time.Hour), "keep"),  // same pk, different table -> distinct
		mk("a", "2", t0.Add(-time.Hour), "older"), // same key, older -> dropped
	})
	want := []struct{ table, pk, v string }{
		{"a", "1", "newest"}, {"a", "2", "only"}, {"b", "1", "keep"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d victims, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		if g.TableName != w.table || g.PKValues != w.pk || g.RowBefore["v"] != w.v {
			t.Errorf("victim[%d] = %s:%s v=%v, want %s:%s v=%s",
				i, g.TableName, g.PKValues, g.RowBefore["v"], w.table, w.pk, w.v)
		}
	}
	one := []query.ResultRow{mk("a", "1", t0, "x")}
	if out := dedupVictimsNewest(one); len(out) != 1 {
		t.Fatalf("single victim must pass through, got %d", len(out))
	}
}
