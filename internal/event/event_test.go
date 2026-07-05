package event_test

import (
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

func TestBuildPKValues(t *testing.T) {
	tests := []struct {
		name string
		cols []metadata.ColumnMeta
		row  map[string]any
		want string
	}{
		{"single", []metadata.ColumnMeta{{Name: "id"}}, map[string]any{"id": 42}, "42"},
		{"composite", []metadata.ColumnMeta{{Name: "a"}, {Name: "b"}}, map[string]any{"a": 1, "b": 2}, "1|2"},
		{"escape pipe and backslash", []metadata.ColumnMeta{{Name: "c"}},
			map[string]any{"c": `x|y\z`}, `x\|y\\z`},
		// #756: a BINARY(16) PK (e.g. a binary UUID) now arrives from
		// metadata.MapRow as []byte rather than a raw Go string. "%v" on a
		// []byte prints Go's bracketed decimal representation
		// (e.g. "[222 173]"), not the raw bytes — BuildPKValues must special-
		// case it so pk_values/pk_hash stay exactly what they were when the
		// same bytes arrived as a string.
		{"binary PK bytes preserved raw", []metadata.ColumnMeta{{Name: "id"}},
			map[string]any{"id": []byte{0xDE, 0xAD}}, string([]byte{0xDE, 0xAD})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := event.BuildPKValues(tt.cols, tt.row); got != tt.want {
				t.Errorf("BuildPKValues = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestChangedColumns(t *testing.T) {
	if got := event.ChangedColumns(nil, map[string]any{"a": 1}); got != nil {
		t.Errorf("INSERT (nil before) = %v, want nil", got)
	}
	if got := event.ChangedColumns(map[string]any{"a": 1}, nil); got != nil {
		t.Errorf("DELETE (nil after) = %v, want nil", got)
	}
	got := event.ChangedColumns(
		map[string]any{"a": 1, "b": 2, "c": 3},
		map[string]any{"a": 1, "b": 99, "c": 100},
	)
	want := []string{"b", "c"} // sorted
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ChangedColumns = %v, want %v", got, want)
	}
}

func TestFiltersMatches(t *testing.T) {
	var allow event.Filters // nil maps = accept all
	if !allow.Matches("any", "thing") {
		t.Error("nil Filters should accept all")
	}
	f := event.Filters{
		Schemas: map[string]bool{"app": true},
		Tables:  map[string]bool{"app.users": true},
	}
	if !f.Matches("app", "users") {
		t.Error("app.users should match")
	}
	if f.Matches("other", "users") {
		t.Error("other schema should be filtered out")
	}
	if f.Matches("app", "orders") {
		t.Error("app.orders should be filtered out")
	}
}
