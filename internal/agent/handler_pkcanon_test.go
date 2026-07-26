package agent

// #1137 compat: the cross-boundary pk_hash lookups must also match the hash
// of the canonical (post-#1132 hex) spelling when the stored row carries the
// pre-fix RAW spelling of a binary PK.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/buffer"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestHandleResolvePK_archivePreFixRawSpelling: an archive row persisted
// before the #1132 hex fix stores the RAW spelling of a binary PK; a
// control-plane PKHash computed over the post-fix hex spelling of the same
// key must still resolve it (to the stored spelling), and a different key
// must still miss.
func TestHandleResolvePK_archivePreFixRawSpelling(t *testing.T) {
	raw := string([]byte{0xB2, 0x81, 0x5C, 0xC3, 0xC2, 0x00, 0xFF, 0x7C})
	stored := event.EscapePKValue(raw)
	const canonical = "0xB2815CC3C200FF7C" // the post-#1132 spelling of the same key

	h := &DefaultHandler{
		ArchiveSources: []string{"srcA"},
		ArchiveFetcher: func(ctx context.Context, opts query.Options, source string) ([]query.ResultRow, error) {
			return []query.ResultRow{{PKValues: stored}}, nil
		},
	}
	req := ResolvePKRequest{Items: []PKItem{
		{PKHash: byosPKHash(canonical), Schema: "shop", Table: "orders"},
		{PKHash: byosPKHash("0xDEADBEEF"), Schema: "shop", Table: "orders"}, // different key
	}}
	results, err := h.HandleResolvePK(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleResolvePK: %v", err)
	}
	if !results[0].Found || results[0].PKValues != stored {
		t.Errorf("results[0] = %+v, want Found with stored raw spelling %q", results[0], stored)
	}
	if results[1].Found {
		t.Errorf("results[1] = %+v, want a miss for a different key", results[1])
	}
}

// TestHandleResolvePK_archiveExactBeatsCanonicalAlias: collision precedence
// must not depend on scan order. Row A stores the pre-#1132 RAW spelling of
// bytes {0xFF,0xFE} (canonicalizes to "0xFFFE"); row B is a VARBINARY PK
// literally holding the ASCII text "0xFFFE" (valid UTF-8, stored verbatim).
// A hash over "0xFFFE" is B's EXACT stored-spelling hash and only A's alias —
// B must win in both insertion orders, and A must stay resolvable via its own
// exact hash.
func TestHandleResolvePK_archiveExactBeatsCanonicalAlias(t *testing.T) {
	rawA := string([]byte{0xFF, 0xFE})
	const literalB = "0xFFFE"

	orders := map[string][]query.ResultRow{
		"raw first":     {{PKValues: rawA}, {PKValues: literalB}},
		"literal first": {{PKValues: literalB}, {PKValues: rawA}},
	}
	for name, rows := range orders {
		t.Run(name, func(t *testing.T) {
			h := &DefaultHandler{
				ArchiveSources: []string{"srcA"},
				ArchiveFetcher: func(ctx context.Context, opts query.Options, source string) ([]query.ResultRow, error) {
					return rows, nil
				},
			}
			req := ResolvePKRequest{Items: []PKItem{
				{PKHash: byosPKHash(literalB), Schema: "shop", Table: "orders"},
				{PKHash: byosPKHash(rawA), Schema: "shop", Table: "orders"},
			}}
			results, err := h.HandleResolvePK(context.Background(), req)
			if err != nil {
				t.Fatalf("HandleResolvePK: %v", err)
			}
			if !results[0].Found || results[0].PKValues != literalB {
				t.Errorf("hash of %q resolved to %+v, want the EXACT-spelling row %q (alias must not shadow it)",
					literalB, results[0], literalB)
			}
			if !results[1].Found || results[1].PKValues != rawA {
				t.Errorf("hash of the raw spelling resolved to %+v, want %q", results[1], rawA)
			}
		})
	}
}

// TestHandleRecover_pkHashesPreFixRawSpelling: the recover pk_hash filter —
// a pre-#1132 raw-spelling row must be selected by a hash computed over the
// post-fix hex spelling, and a hash for a different key must select nothing.
func TestHandleRecover_pkHashesPreFixRawSpelling(t *testing.T) {
	now := time.Now().UTC()
	raw := string([]byte{0xB2, 0x81, 0x5C, 0xC3, 0xC2, 0x00, 0xFF, 0x7C})
	stored := event.EscapePKValue(raw)
	const canonical = "0xB2815CC3C200FF7C"

	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	buf.Insert([]parser.Event{
		makeRecoverEvent(now, stored, ""),
		makeRecoverEvent(now, "other", ""),
	})
	h := &DefaultHandler{Buffer: buf}

	sqlOut, err := h.HandleRecover(context.Background(), RecoverRequest{
		TimeStart: now.Add(-time.Minute),
		TimeEnd:   now.Add(time.Minute),
		PKHashes:  []string{byosPKHash(canonical)},
	})
	if err != nil {
		t.Fatalf("HandleRecover: %v", err)
	}
	if got := strings.Count(sqlOut, "DELETE FROM"); got != 1 {
		t.Errorf("reversal script has %d DELETEs, want exactly 1 (the raw-spelling row):\n%s", got, sqlOut)
	}
	if strings.Contains(sqlOut, "'other'") {
		t.Errorf("unrelated pk selected by the compat path:\n%s", sqlOut)
	}

	// Different key: nothing selected, empty reversal.
	sqlOut, err = h.HandleRecover(context.Background(), RecoverRequest{
		TimeStart: now.Add(-time.Minute),
		TimeEnd:   now.Add(time.Minute),
		PKHashes:  []string{byosPKHash("0xDEADBEEF")},
	})
	if err != nil {
		t.Fatalf("HandleRecover (miss): %v", err)
	}
	if strings.Contains(sqlOut, "DELETE FROM") {
		t.Errorf("hash of a different key selected events:\n%s", sqlOut)
	}
}
