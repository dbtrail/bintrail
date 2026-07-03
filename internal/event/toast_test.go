package event_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestUnchangedToastKeyStable pins the on-disk contract: the marker is
// persisted as JSON in binlog_events row images, so the key must never change —
// a marker written by any past version must remain detectable by any future
// read side.
func TestUnchangedToastKeyStable(t *testing.T) {
	if event.UnchangedToastKey != "__bintrail_unchanged_toast__" {
		t.Fatalf("UnchangedToastKey changed to %q — this breaks detection of markers already persisted in existing indexes", event.UnchangedToastKey)
	}
}

func TestIsUnchangedToastMarker(t *testing.T) {
	marker := map[string]any{event.UnchangedToastKey: true}

	// The marker as the decoder emits it, and as it comes back out of a stored
	// row image via query.UnmarshalRowImage (UseNumber) — the REAL read-path
	// decoder every #592 guard consumes images from, not a plain json.Unmarshal.
	image := query.UnmarshalRowImage([]byte(`{"body":{"` + event.UnchangedToastKey + `":true}}`))
	if image == nil {
		t.Fatal("UnmarshalRowImage returned nil for a valid image")
	}
	for name, v := range map[string]any{"decoder shape": marker, "UnmarshalRowImage round-trip": image["body"]} {
		if !event.IsUnchangedToastMarker(v) {
			t.Errorf("%s: expected marker to be detected: %#v", name, v)
		}
	}

	// Strict non-matches: anything that is not exactly the one-key true map.
	nonMarkers := map[string]any{
		"nil":              nil,
		"string literal":   event.UnchangedToastKey,
		"bool":             true,
		"empty map":        map[string]any{},
		"extra key":        map[string]any{event.UnchangedToastKey: true, "other": 1},
		"false value":      map[string]any{event.UnchangedToastKey: false},
		"non-bool value":   map[string]any{event.UnchangedToastKey: "true"},
		"different key":    map[string]any{"unchanged_toast": true},
		"nested one level": map[string]any{"payload": map[string]any{event.UnchangedToastKey: true}},
	}
	for name, v := range nonMarkers {
		if event.IsUnchangedToastMarker(v) {
			t.Errorf("%s: %#v must NOT match the marker (strict shape)", name, v)
		}
	}
}

func TestUnresolvedToastColumns(t *testing.T) {
	if got := event.UnresolvedToastColumns(nil); got != nil {
		t.Errorf("nil image → %v, want nil", got)
	}
	image := map[string]any{
		"id":    "1",
		"zbody": map[string]any{event.UnchangedToastKey: true},
		"attrs": map[string]any{event.UnchangedToastKey: true},
		"note":  "fine",
	}
	want := []string{"attrs", "zbody"} // sorted
	if got := event.UnresolvedToastColumns(image); !reflect.DeepEqual(got, want) {
		t.Errorf("UnresolvedToastColumns = %v, want %v", got, want)
	}
}

func TestCheckUnresolvedToast(t *testing.T) {
	clean := map[string]any{"id": "1", "body": "real value"}
	dirty := map[string]any{"id": "1", "body": map[string]any{event.UnchangedToastKey: true}}

	if err := event.CheckUnresolvedToast("public", "docs", "1", clean, nil, clean); err != nil {
		t.Fatalf("clean images must pass: %v", err)
	}

	err := event.CheckUnresolvedToast("public", "docs", "1", clean, dirty)
	if err == nil {
		t.Fatal("expected a loud error for a marker-carrying image")
	}
	for _, want := range []string{
		"unresolved unchanged-TOAST marker",
		"capture invariant violated",
		"public.docs",
		"pk=1",
		"body",
		"REPLICA IDENTITY FULL",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
}

// TestUnresolvedToastError_elidesEmptyContext covers callers without row
// context (the shim's pure resultset builders): the message must stay clean,
// with no dangling "in . (pk=)".
func TestUnresolvedToastError_elidesEmptyContext(t *testing.T) {
	err := event.UnresolvedToastError("", "", "", []string{"body"})
	msg := err.Error()
	if strings.Contains(msg, " in .") || strings.Contains(msg, "pk=") {
		t.Errorf("empty locator context must be elided:\n%s", msg)
	}
	if !strings.HasPrefix(msg, "unresolved unchanged-TOAST marker, column(s) body — capture invariant violated") {
		t.Errorf("unexpected message shape:\n%s", msg)
	}
}
