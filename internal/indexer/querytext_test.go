package indexer

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// ─── sanitizeQueryText (#699) ─────────────────────────────────────────────────

func TestSanitizeQueryText_passthrough(t *testing.T) {
	if got := sanitizeQueryText(""); got != "" {
		t.Errorf("empty in, got %q", got)
	}
	s := "UPDATE users SET email='a@x' WHERE id=42"
	if got := sanitizeQueryText(s); got != s {
		t.Errorf("short valid statement must pass through unchanged, got %q", got)
	}
}

// A _binary'...' literal embeds raw bytes; under strict mode those would 1366
// the whole batch INSERT. sanitizeQueryText must yield valid UTF-8.
func TestSanitizeQueryText_invalidUTF8Replaced(t *testing.T) {
	s := "INSERT INTO t VALUES (_binary'\xff\xfe\x00')"
	got := sanitizeQueryText(s)
	if !utf8.ValidString(got) {
		t.Fatalf("output is not valid UTF-8: %q", got)
	}
	if !strings.Contains(got, "INSERT INTO t VALUES") {
		t.Errorf("statement head must survive sanitization, got %q", got)
	}
}

func TestSanitizeQueryText_truncatesAtCap(t *testing.T) {
	// Build a statement well over the cap with a multi-byte rune straddling
	// the cut point so the rune-boundary backoff is exercised.
	long := "INSERT INTO t VALUES ('" + strings.Repeat("é", maxQueryTextBytes) + "')"
	got := sanitizeQueryText(long)

	if !strings.HasSuffix(got, queryTextTruncationMarker) {
		t.Fatalf("truncated statement must end with the truncation marker, got tail %q", got[len(got)-40:])
	}
	if !utf8.ValidString(got) {
		t.Fatal("truncation must not cut a rune in half")
	}
	if len(got) > maxQueryTextBytes+len(queryTextTruncationMarker) {
		t.Errorf("output length %d exceeds cap %d + marker", len(got), maxQueryTextBytes)
	}
	if !strings.HasPrefix(got, "INSERT INTO t VALUES ('") {
		t.Error("truncation must preserve the statement head")
	}
}

func TestSanitizeQueryText_exactCapUntouched(t *testing.T) {
	s := strings.Repeat("a", maxQueryTextBytes)
	if got := sanitizeQueryText(s); got != s {
		t.Errorf("statement exactly at the cap must pass through unchanged (len %d)", len(got))
	}
}
