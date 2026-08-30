package mcptools

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

// jsonschemaDescription reflects the jsonschema tag (the text the inferred
// input schema advertises) of one field of a tool args struct.
func jsonschemaDescription(t *testing.T, args any, field string) string {
	t.Helper()
	f, ok := reflect.TypeOf(args).FieldByName(field)
	if !ok {
		t.Fatalf("%T has no field %s", args, field)
	}
	return f.Tag.Get("jsonschema")
}

// ─── #1440: pk_min/pk_max on the query and recover tools ────────────────────

func TestBuildQueryOptions_pkRangeRules(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    FilterParams
		want string
	}{
		{"needs schema", FilterParams{Table: "t", PKMin: "1"}, "pk_min/pk_max require both schema and table"},
		{"needs table", FilterParams{Schema: "s", PKMax: "1"}, "pk_min/pk_max require both schema and table"},
		{"exclusive with pk", FilterParams{Schema: "s", Table: "t", PK: "5", PKMin: "1"}, "cannot be combined with pk or pks"},
		{"exclusive with pks", FilterParams{Schema: "s", Table: "t", PKs: []string{"5"}, PKMax: "1"}, "cannot be combined with pk or pks"},
		{"min not integer", FilterParams{Schema: "s", Table: "t", PKMin: "abc"}, "pk_min: \"abc\" is not an integer"},
		{"max not integer", FilterParams{Schema: "s", Table: "t", PKMax: "1e3"}, "pk_max: \"1e3\" is not an integer"},
		{"inverted", FilterParams{Schema: "s", Table: "t", PKMin: "10", PKMax: "9"}, "pk_min/pk_max: lower bound 10 is above upper bound 9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildQueryOptions(tc.p, 100)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want %q", err, tc.want)
			}
		})
	}

	opts, err := BuildQueryOptions(FilterParams{Schema: "s", Table: "t", PKMin: "10", PKMax: "18446744073709551615"}, 100)
	if err != nil {
		t.Fatalf("valid range refused: %v", err)
	}
	if opts.PKRange == nil || opts.PKRange.Min.String() != "10" || opts.PKRange.Max.String() != "18446744073709551615" {
		t.Fatalf("range not plumbed: %+v", opts.PKRange)
	}
	// The builder has no snapshot: the range must leave here UNRESOLVED so
	// an engine refuses it if a handler ever skips Target.resolvePKRange.
	if opts.PKRange.Cast != query.PKCastUnset {
		t.Errorf("BuildQueryOptions guessed a cast (%d); the schema snapshot decides it", opts.PKRange.Cast)
	}
	if err := opts.ValidatePKRange(); err == nil {
		t.Error("an unresolved range passed the engine check; the belt is gone")
	}
	opts, err = BuildQueryOptions(FilterParams{Schema: "s", Table: "t"}, 100)
	if err != nil || opts.PKRange != nil {
		t.Errorf("no bounds must mean no range: %+v, %v", opts.PKRange, err)
	}
}

// TestQueryToolSchema_pkRangeStatesCost: the tool schema text is what an
// agent reads before choosing the filter; it must say the range scans and
// point at since/until, on both tools.
func TestQueryToolSchema_pkRangeStatesCost(t *testing.T) {
	for _, args := range []any{QueryArgs{}, RecoverArgs{}} {
		desc := jsonschemaDescription(t, args, "PKMin")
		for _, want := range []string{"since", "until", "scan", "pk or pks", "schema and table"} {
			if !strings.Contains(desc, want) {
				t.Errorf("%T pk_min schema text lacks %q: %s", args, want, desc)
			}
		}
		if strings.Contains(desc, "—") || strings.Contains(jsonschemaDescription(t, args, "PKMax"), "—") {
			t.Errorf("%T pk range schema text carries an em dash", args)
		}
	}
}
