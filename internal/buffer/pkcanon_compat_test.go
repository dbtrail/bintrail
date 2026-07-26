package buffer

// #1137 compat: ResolvePK must also match the hash of the canonical
// (post-#1132 hex) spelling when the stored entry carries the pre-fix RAW
// spelling of a binary PK.

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// TestResolvePK_preFixRawSpelling: a buffer entry carrying the pre-#1132 RAW
// spelling of a binary PK (customer Parquet / buffer never rejected it) must
// still resolve when the caller's hash was computed over the post-#1132 hex
// spelling of the same key.
func TestResolvePK_preFixRawSpelling(t *testing.T) {
	raw := string([]byte{0xB2, 0x81, 0x5C, 0xC3, 0xC2, 0x00, 0xFF, 0x7C})
	stored := event.EscapePKValue(raw) // what a pre-#1132 producer persisted (0x5C='\', 0x7C='|')

	buf := New(Config{MaxAge: 6 * time.Hour})
	buf.Insert([]parser.Event{makeUpdate("db", "t", stored, time.Now().UTC())})

	// A post-#1132 producer spells the same key as 0x + uppercase hex; the
	// control plane hashes THAT spelling.
	const canonical = "0xB2815CC3C200FF7C"
	val, ok := buf.ResolvePK(pkHash(canonical), "db", "t")
	if !ok {
		t.Fatal("pre-fix raw-spelling entry not found via canonical-spelling hash")
	}
	if val != stored {
		t.Errorf("pk_values = %q, want the stored raw spelling %q", val, stored)
	}

	// Exact-spelling lookup still works too.
	if _, ok := buf.ResolvePK(pkHash(stored), "db", "t"); !ok {
		t.Error("raw-spelling hash no longer resolves the raw-spelling entry")
	}
}

// TestResolvePK_preFixRawSpelling_differentKeyMisses: the compat path must
// only widen matching to the SAME key's other spelling, never to other keys.
func TestResolvePK_preFixRawSpelling_differentKeyMisses(t *testing.T) {
	raw := string([]byte{0xB2, 0x81, 0x5C, 0xC3, 0xC2, 0x00, 0xFF, 0x7C})
	buf := New(Config{MaxAge: 6 * time.Hour})
	buf.Insert([]parser.Event{makeUpdate("db", "t", event.EscapePKValue(raw), time.Now().UTC())})

	if _, ok := buf.ResolvePK(pkHash("0xDEADBEEF"), "db", "t"); ok {
		t.Error("hash of a different key must still miss")
	}
}
