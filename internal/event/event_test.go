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
		// same bytes arrived as a string. UNCHANGED by #1132: {0xDE,0xAD} is a
		// well-formed 2-byte UTF-8 sequence (U+07AD), so utf8mb4 accepts it and
		// the verbatim spelling — and therefore the pk_hash — must stay exactly
		// as it was. This is the regression guard for "hex-encoding only ever
		// touches values that could not be stored at all".
		{"binary PK bytes preserved raw", []metadata.ColumnMeta{{Name: "id"}},
			map[string]any{"id": []byte{0xDE, 0xAD}}, string([]byte{0xDE, 0xAD})},
		// #1132: 0xB2 is a UTF-8 continuation byte, so it can never lead a
		// sequence — these bytes are unstorable in the utf8mb4 pk_values
		// column. Written verbatim, MySQL rejected the whole batch INSERT with
		// error 1366 and, batch failures being fail-loud by contract, stopped
		// capture for EVERY table in that source. Hex-encoded, the row is
		// storable.
		{"binary PK bytes hex-encoded when not valid UTF-8", []metadata.ColumnMeta{{Name: "id"}},
			map[string]any{"id": []byte{0xB2, 0x81}}, "0xB281"},
		// A full BINARY(16) PK — the shape reported in #1132 (MD5/binary UUID).
		// 0x5C is a literal backslash and 0x7C would be a pipe: hex-encoding
		// runs BEFORE EscapePKValue, so the delimiter escaping is a no-op on a
		// hex component. (The issue's error message shows the "\\" escape
		// working on the raw form — the destination charset was the problem,
		// not the escaping.)
		{"binary(16) PK hex-encoded", []metadata.ColumnMeta{{Name: "k"}},
			map[string]any{"k": []byte{
				0xB2, 0x81, 0x5C, 0xC3, 0xC2, 0x00, 0xFF, 0x7C,
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x80}},
			"0xB2815CC3C200FF7C0102030405060780"},
		// A composite PK mixing an int with unstorable bytes still joins on "|".
		{"composite with binary component", []metadata.ColumnMeta{{Name: "a"}, {Name: "b"}},
			map[string]any{"a": 7, "b": []byte{0xFF, 0xFE}}, "7|0xFFFE"},
		// A []byte that IS valid UTF-8 takes the verbatim path and therefore
		// still goes through EscapePKValue — the delimiter escaping must not
		// be skipped just because the value arrived as bytes. (Only the hex
		// path is escape-free, and only because hex digits contain neither
		// character.)
		{"valid-UTF-8 bytes still get delimiter-escaped", []metadata.ColumnMeta{{Name: "k"}},
			map[string]any{"k": []byte(`x|y\z`)}, `x\|y\\z`},
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
