package event

// #1137: tests for splitPKValues (the exact inverse of EscapePKValue + the
// "|" join) and CanonicalPKValues (the BYOS compat read path that re-spells
// pre-#1132 raw binary-PK components as 0x + uppercase hex).

import (
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// rawBinaryPK is the pre-#1132 raw spelling source for a BINARY(8) PK whose
// bytes are not valid UTF-8. Deliberately includes 0x5C ('\') and 0x7C ('|')
// so the pipe/backslash escaping is exercised, not bypassed.
var rawBinaryPK = string([]byte{0xB2, 0x81, 0x5C, 0xC3, 0xC2, 0x00, 0xFF, 0x7C})

// hexBinaryPK is what formatPKValue produces for rawBinaryPK's bytes since
// #1132 — and what SELECT CONCAT('0x', HEX(k)) prints on the source.
const hexBinaryPK = "0xB2815CC3C200FF7C"

func joinEscaped(parts []string) string {
	escaped := make([]string, len(parts))
	for i, p := range parts {
		escaped[i] = EscapePKValue(p)
	}
	return strings.Join(escaped, "|")
}

func TestSplitPKValues_invertsEscapeJoin(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
	}{
		{"single", []string{"42"}},
		{"composite", []string{"1", "2"}},
		{"pipe and backslash", []string{`a|b`, `c\d`, `\|`}},
		{"trailing backslash component", []string{`x\`, "y"}},
		{"empty components", []string{"", ""}},
		{"raw binary single", []string{rawBinaryPK}},
		{"raw binary composite", []string{"x", rawBinaryPK, "y|z"}},
		{"hex spelling", []string{hexBinaryPK, "7"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			joined := joinEscaped(tt.parts)
			got := splitPKValues(joined)
			if !reflect.DeepEqual(got, tt.parts) {
				t.Errorf("splitPKValues(%q) = %q, want %q", joined, got, tt.parts)
			}
		})
	}
}

func TestSplitPKValues_invertsBuildPKValues(t *testing.T) {
	cols := []metadata.ColumnMeta{{Name: "a"}, {Name: "b"}}
	row := map[string]any{"a": `x|y\`, "b": 42}
	got := splitPKValues(BuildPKValues(cols, row))
	want := []string{`x|y\`, "42"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("splitPKValues(BuildPKValues(...)) = %q, want %q", got, want)
	}
}

func TestCanonicalPKValues_passthroughSameAllocation(t *testing.T) {
	for _, s := range []string{"", "42", `a\|b|c`, hexBinaryPK, "0xDEAD|7"} {
		got := CanonicalPKValues(s)
		if got != s {
			t.Errorf("CanonicalPKValues(%q) = %q, want input unchanged", s, got)
			continue
		}
		if len(s) > 0 && unsafe.StringData(got) != unsafe.StringData(s) {
			t.Errorf("CanonicalPKValues(%q) reallocated an already-canonical value", s)
		}
	}
}

func TestCanonicalPKValues_respellsRawBinary(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"single raw", EscapePKValue(rawBinaryPK), hexBinaryPK},
		{"truncated multibyte", string([]byte{0xC3}), "0xC3"},
		{"composite mixed", joinEscaped([]string{rawBinaryPK, `x|y\`}),
			hexBinaryPK + `|x\|y\\`},
		{"raw last", joinEscaped([]string{"7", rawBinaryPK}), "7|" + hexBinaryPK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanonicalPKValues(tt.in)
			if got != tt.want {
				t.Errorf("CanonicalPKValues(%q) = %q, want %q", tt.in, got, tt.want)
			}
			// Idempotent: the canonical form must pass through untouched.
			if again := CanonicalPKValues(got); again != got {
				t.Errorf("not idempotent: CanonicalPKValues(%q) = %q", got, again)
			}
		})
	}
}

// TestCanonicalPKValues_matchesBuildPKValues pins the load-bearing
// equivalence: canonicalizing the pre-#1132 stored spelling of a key yields
// byte-for-byte what BuildPKValues produces for that same key today — so a
// hash over the canonical form equals a post-fix producer's hash.
func TestCanonicalPKValues_matchesBuildPKValues(t *testing.T) {
	rawBytes := []byte(rawBinaryPK)

	single := []metadata.ColumnMeta{{Name: "k"}}
	post := BuildPKValues(single, map[string]any{"k": rawBytes})
	preFix := EscapePKValue(rawBinaryPK) // what a pre-#1132 producer persisted
	if got := CanonicalPKValues(preFix); got != post {
		t.Errorf("single: CanonicalPKValues(%q) = %q, want BuildPKValues result %q", preFix, got, post)
	}

	composite := []metadata.ColumnMeta{{Name: "k"}, {Name: "n"}}
	postC := BuildPKValues(composite, map[string]any{"k": rawBytes, "n": `p|q\`})
	preFixC := joinEscaped([]string{rawBinaryPK, `p|q\`})
	if got := CanonicalPKValues(preFixC); got != postC {
		t.Errorf("composite: CanonicalPKValues(%q) = %q, want BuildPKValues result %q", preFixC, got, postC)
	}
}
