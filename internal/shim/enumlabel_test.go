package shim

import (
	"log/slog"
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

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

func orderEnumSetMeta() *metadata.TableMeta {
	return &metadata.TableMeta{
		Schema: "myapp",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
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
			m := newEnumLabelMapper(orderEnumSetMeta())
			if m == nil {
				t.Fatal("expected a mapper for a table with enum/set columns")
			}
			m.mapImage(tt.in)
			if !reflect.DeepEqual(tt.in, tt.want) {
				t.Errorf("image = %v, want %v", tt.in, tt.want)
			}
		})
	}
}

func TestEnumLabelMapperNilSafety(t *testing.T) {
	if m := newEnumLabelMapper(nil); m != nil {
		t.Error("nil TableMeta must yield a nil mapper")
	}
	noEnums := &metadata.TableMeta{Columns: []metadata.ColumnMeta{
		{Name: "id", ColumnType: "int unsigned"},
		{Name: "name", ColumnType: "varchar(20)"},
		{Name: "legacy", ColumnType: ""}, // pre-#212 snapshot
	}}
	if m := newEnumLabelMapper(noEnums); m != nil {
		t.Error("table without enum/set columns must yield a nil mapper")
	}

	// The nil receiver and nil image are both valid no-ops — every call
	// site relies on this instead of guarding.
	var m *enumLabelMapper
	m.mapImage(map[string]any{"status": float64(1)})
	m.mapImage(nil)
	real := newEnumLabelMapper(orderEnumSetMeta())
	real.mapImage(nil)
}

// TestEnumMapperFor covers the Handler-side composition (tableMetaFor →
// newEnumLabelMapper) that every render path calls — the only other
// protection it has is one integration-tagged test.
func TestEnumMapperFor(t *testing.T) {
	h := &Handler{
		logger: slog.Default(),
		resolverFn: func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
				"myapp.orders": orderEnumSetMeta(),
				"myapp.plain": {Schema: "myapp", Table: "plain", Columns: []metadata.ColumnMeta{
					{Name: "id", ColumnType: "int unsigned"},
				}},
			}), nil
		},
	}

	m := h.enumMapperFor("myapp", "orders")
	if m == nil {
		t.Fatal("expected a mapper for a snapshot-resolved enum table")
	}
	img := map[string]any{"status": float64(2)}
	m.mapImage(img)
	if img["status"] != "processing" {
		t.Errorf("status = %v, want \"processing\"", img["status"])
	}

	if m := h.enumMapperFor("myapp", "plain"); m != nil {
		t.Error("table without enum/set columns must yield a nil mapper")
	}
	if m := h.enumMapperFor("myapp", "missing"); m != nil {
		t.Error("table absent from the snapshot must yield a nil mapper")
	}
	if m := (&Handler{logger: slog.Default()}).enumMapperFor("a", "b"); m != nil {
		t.Error("handler without resolverFn must yield a nil mapper")
	}
}
