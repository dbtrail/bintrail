package metadata

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestOrdinalValue_jsonNumber(t *testing.T) {
	// Row images decoded by query.UnmarshalRowImage carry numbers as json.Number
	// (#496); ENUM/SET label resolution must still extract the ordinal.
	if u, ok := ordinalValue(json.Number("3")); !ok || u != 3 {
		t.Errorf("ordinalValue(json.Number(\"3\")) = (%d,%v), want (3,true)", u, ok)
	}
	// Full 64-bit range (a SET mask using a high bit) — no 2^53 float cap.
	if u, ok := ordinalValue(json.Number("9223372036854775808")); !ok || u != 9223372036854775808 {
		t.Errorf("ordinalValue(high-bit) = (%d,%v), want (9223372036854775808,true)", u, ok)
	}
	// Negative, fractional, and non-numeric → pass-through (!ok).
	for _, bad := range []json.Number{"-1", "1.5", "abc"} {
		if _, ok := ordinalValue(bad); ok {
			t.Errorf("ordinalValue(json.Number(%q)) reported ok, want !ok", bad)
		}
	}
}

func TestParseEnumSetLabels(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		wantLabels []string // nil → expect !ok
		wantIsSet  bool
	}{
		{
			name:       "simple enum",
			columnType: "enum('pending','processing','shipped')",
			wantLabels: []string{"pending", "processing", "shipped"},
		},
		{
			name:       "simple set",
			columnType: "set('a','b','c')",
			wantLabels: []string{"a", "b", "c"},
			wantIsSet:  true,
		},
		{
			name:       "uppercase declaration",
			columnType: "ENUM('A','B')",
			wantLabels: []string{"A", "B"},
		},
		{
			name:       "comma inside a member",
			columnType: "enum('a,b','c')",
			wantLabels: []string{"a,b", "c"},
		},
		{
			name:       "doubled quote escape",
			columnType: "enum('it''s','plain')",
			wantLabels: []string{"it's", "plain"},
		},
		{
			name:       "empty member is legal",
			columnType: "enum('','active')",
			wantLabels: []string{"", "active"},
		},
		{
			name:       "single member",
			columnType: "enum('only')",
			wantLabels: []string{"only"},
		},
		{
			// MySQL renders a literal backslash in a member as \\ —
			// COLUMN_TYPE bytes: enum('a\\b','c')
			name:       "backslash escape decodes",
			columnType: `enum('a\\b','c')`,
			wantLabels: []string{`a\b`, "c"},
		},
		{
			// A newline in a member renders as the two bytes \n.
			name:       "newline escape decodes",
			columnType: `enum('line1\nline2')`,
			wantLabels: []string{"line1\nline2"},
		},
		{
			name:       "carriage-return escape decodes",
			columnType: `enum('a\rb')`,
			wantLabels: []string{"a\rb"},
		},
		{
			name:       "NUL escape decodes",
			columnType: `enum('a\0b')`,
			wantLabels: []string{"a\x00b"},
		},
		{name: "not an enum", columnType: "int unsigned"},
		{name: "varchar with parens", columnType: "varchar(20)"},
		{name: "pre-#212 empty column_type", columnType: ""},
		{name: "unterminated quote", columnType: "enum('a"},
		{name: "unknown escape bails to honest ordinal", columnType: `enum('a\xb')`},
		{name: "dangling backslash at end", columnType: `enum('a\`},
		{name: "empty member list", columnType: "enum()"},
		{name: "trailing comma", columnType: "enum('a',)"},
		{name: "garbage between members", columnType: "enum('a' x 'b')"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels, isSet, ok := parseEnumSetLabels(tt.columnType)
			if wantOK := tt.wantLabels != nil; ok != wantOK {
				t.Fatalf("ok = %v, want %v (labels %v)", ok, wantOK, labels)
			}
			if !ok {
				return
			}
			if !reflect.DeepEqual(labels, tt.wantLabels) {
				t.Errorf("labels = %v, want %v", labels, tt.wantLabels)
			}
			if isSet != tt.wantIsSet {
				t.Errorf("isSet = %v, want %v", isSet, tt.wantIsSet)
			}
		})
	}
}

func orderEnumSetMeta() *TableMeta {
	return &TableMeta{
		Schema: "myapp",
		Table:  "orders",
		Columns: []ColumnMeta{
			{Name: "id", DataType: "int", ColumnType: "int unsigned", IsPK: true},
			{Name: "status", DataType: "enum", ColumnType: "enum('pending','processing','shipped')"},
			{Name: "tags", DataType: "set", ColumnType: "set('red','blue')"},
		},
		PKColumns: []string{"id"},
	}
}

func TestEnumLabelMapperMapImage(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]any
		want map[string]any
	}{
		{
			name: "enum ordinal maps to label (JSON float64)",
			in:   map[string]any{"id": float64(1), "status": float64(3)},
			want: map[string]any{"id": float64(1), "status": "shipped"},
		},
		{
			name: "enum ordinal zero is MySQL's empty sentinel",
			in:   map[string]any{"status": float64(0)},
			want: map[string]any{"status": ""},
		},
		{
			name: "out-of-range ordinal passes through (enum shrank post-event)",
			in:   map[string]any{"status": float64(7)},
			want: map[string]any{"status": float64(7)},
		},
		{
			name: "non-integral value passes through",
			in:   map[string]any{"status": 1.5},
			want: map[string]any{"status": 1.5},
		},
		{
			name: "string value passes through (baseline already carries labels)",
			in:   map[string]any{"status": "shipped"},
			want: map[string]any{"status": "shipped"},
		},
		{
			name: "NULL passes through",
			in:   map[string]any{"status": nil},
			want: map[string]any{"status": nil},
		},
		{
			name: "non-enum columns untouched",
			in:   map[string]any{"id": float64(3)},
			want: map[string]any{"id": float64(3)},
		},
		{
			name: "defensive int64 ordinal maps",
			in:   map[string]any{"status": int64(2)},
			want: map[string]any{"status": "processing"},
		},
		{
			name: "set bitmask joins members in definition order",
			in:   map[string]any{"tags": float64(3)},
			want: map[string]any{"tags": "red,blue"},
		},
		{
			name: "set single bit",
			in:   map[string]any{"tags": float64(2)},
			want: map[string]any{"tags": "blue"},
		},
		{
			name: "set zero is the empty set",
			in:   map[string]any{"tags": float64(0)},
			want: map[string]any{"tags": ""},
		},
		{
			name: "set mask with unknown bits passes through (set shrank post-event)",
			in:   map[string]any{"tags": float64(5)},
			want: map[string]any{"tags": float64(5)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewEnumLabelMapper(orderEnumSetMeta())
			if m == nil {
				t.Fatal("expected a mapper for a table with enum/set columns")
			}
			m.MapImage(tt.in)
			if !reflect.DeepEqual(tt.in, tt.want) {
				t.Errorf("image = %v, want %v", tt.in, tt.want)
			}
		})
	}
}

func TestEnumLabelMapperNilSafety(t *testing.T) {
	if m := NewEnumLabelMapper(nil); m != nil {
		t.Error("nil TableMeta must yield a nil mapper")
	}
	noEnums := &TableMeta{Columns: []ColumnMeta{
		{Name: "id", ColumnType: "int unsigned"},
		{Name: "name", ColumnType: "varchar(20)"},
		{Name: "legacy", ColumnType: ""}, // pre-#212 snapshot
	}}
	if m := NewEnumLabelMapper(noEnums); m != nil {
		t.Error("table without enum/set columns must yield a nil mapper")
	}

	// The nil receiver and nil image are both valid no-ops — every call
	// site relies on this instead of guarding.
	var m *EnumLabelMapper
	m.MapImage(map[string]any{"status": float64(1)})
	m.MapImage(nil)
	real := NewEnumLabelMapper(orderEnumSetMeta())
	real.MapImage(nil)
}

// ─── Epochs (#475) ──────────────────────────────────────────────────────────

func TestEpochAt(t *testing.T) {
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	epochs := []SnapshotEpoch{
		{ID: 1, At: t0},
		{ID: 2, At: t0.Add(time.Hour)},
		{ID: 5, At: t0.Add(2 * time.Hour)},
	}
	tests := []struct {
		name   string
		at     time.Time
		wantID int
		wantOK bool
	}{
		{"before first snapshot clamps to first", t0.Add(-time.Hour), 1, true},
		{"exactly at an epoch", t0.Add(time.Hour), 2, true},
		{"between epochs", t0.Add(90 * time.Minute), 2, true},
		{"after last epoch", t0.Add(3 * time.Hour), 5, true},
		{"empty epochs", t0, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eps := epochs
			if tt.name == "empty epochs" {
				eps = nil
			}
			id, ok := EpochAt(eps, tt.at)
			if id != tt.wantID || ok != tt.wantOK {
				t.Errorf("EpochAt = (%d, %v), want (%d, %v)", id, ok, tt.wantID, tt.wantOK)
			}
		})
	}
}

// TestEnumMapperSource pins the selection + degradation ladder: epoch
// resolver when available, Fallback when the per-id load fails or no
// epochs exist, nil (pass-through) when nothing resolves — and per-key
// memoization so ResolverFor runs once per (epoch, table).
func TestEnumMapperSource(t *testing.T) {
	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	// Epoch 1: enum('pending','processing','shipped'); epoch 2: REORDERED.
	metaV1 := orderEnumSetMeta()
	metaV2 := &TableMeta{Schema: "myapp", Table: "orders", Columns: []ColumnMeta{
		{Name: "id", ColumnType: "int", IsPK: true},
		{Name: "status", ColumnType: "enum('shipped','processing','pending')"},
	}}
	resolvers := map[int]*Resolver{
		1: NewResolverFromTables(1, map[string]*TableMeta{"myapp.orders": metaV1}),
		2: NewResolverFromTables(2, map[string]*TableMeta{"myapp.orders": metaV2}),
	}
	loads := 0
	src := &EnumMapperSource{
		Epochs: []SnapshotEpoch{{ID: 1, At: t0}, {ID: 2, At: t0.Add(time.Hour)}},
		ResolverFor: func(id int) (*Resolver, error) {
			loads++
			r, ok := resolvers[id]
			if !ok {
				return nil, errors.New("no such snapshot")
			}
			return r, nil
		},
		Fallback: resolvers[2],
	}

	// Ordinal 3 decodes differently per epoch: 'shipped' under v1,
	// 'pending' under v2 — the exact #475 failure the source prevents.
	img := map[string]any{"status": float64(3)}
	src.MapperAt("myapp", "orders", t0.Add(time.Minute)).MapImage(img)
	if img["status"] != "shipped" {
		t.Errorf("epoch-1 decode = %v, want \"shipped\"", img["status"])
	}
	img = map[string]any{"status": float64(3)}
	src.MapperAt("myapp", "orders", t0.Add(2*time.Hour)).MapImage(img)
	if img["status"] != "pending" {
		t.Errorf("epoch-2 decode = %v, want \"pending\"", img["status"])
	}

	// Memoization: repeated lookups in the same epochs add no loads.
	before := loads
	src.MapperAt("myapp", "orders", t0.Add(2*time.Minute))
	src.MapperAt("myapp", "orders", t0.Add(3*time.Hour))
	if loads != before {
		t.Errorf("memo miss: ResolverFor ran %d extra times", loads-before)
	}

	// Per-id load failure → Fallback (latest definition), not no-mapping.
	failing := &EnumMapperSource{
		Epochs:      []SnapshotEpoch{{ID: 9, At: t0}},
		ResolverFor: func(int) (*Resolver, error) { return nil, errors.New("boom") },
		Fallback:    resolvers[1],
	}
	img = map[string]any{"status": float64(3)}
	failing.MapperAt("myapp", "orders", t0).MapImage(img)
	if img["status"] != "shipped" {
		t.Errorf("fallback decode = %v, want \"shipped\" (epoch-1 fallback)", img["status"])
	}

	// No epochs + no fallback → nil mapper → honest pass-through.
	empty := &EnumMapperSource{}
	img = map[string]any{"status": float64(3)}
	empty.MapperAt("myapp", "orders", t0).MapImage(img)
	if img["status"] != float64(3) {
		t.Errorf("empty source must pass through, got %v", img["status"])
	}

	// Table absent from the epoch's snapshot → nil mapper, pass-through.
	img = map[string]any{"status": float64(3)}
	src.MapperAt("myapp", "missing", t0).MapImage(img)
	if img["status"] != float64(3) {
		t.Errorf("unknown table must pass through, got %v", img["status"])
	}
}
