package reconstruct

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
)

func TestDecodeStoredBase64_stringUnaffected(t *testing.T) {
	// Normal base64-stored TEXT/BLOB values must round-trip exactly as before.
	encoded := base64.StdEncoding.EncodeToString([]byte("hello"))
	if got := decodeStoredBase64(encoded, false); got != "hello" {
		t.Errorf("text decode: got %v, want %q", got, "hello")
	}
	if got := decodeStoredBase64(encoded, true); string(got.([]byte)) != "hello" {
		t.Errorf("binary decode: got %v, want []byte(hello)", got)
	}
	if got := decodeStoredBase64("not-base64!!", false); got != "not-base64!!" {
		t.Errorf("undecodable string must pass through unchanged, got %v", got)
	}
}

func TestDecodeStoredBase64_boolRepaired(t *testing.T) {
	// #736: a TEXT/BLOB value mis-promoted to a JSON bool by the pre-fix
	// marshalRow (e.g. the literal string "false") must be restored to its
	// original textual literal, not left as a stray Go bool.
	if got := decodeStoredBase64(false, false); got != "false" {
		t.Errorf("text: got %v (%T), want %q", got, got, "false")
	}
	if got := decodeStoredBase64(true, false); got != "true" {
		t.Errorf("text: got %v (%T), want %q", got, got, "true")
	}
	if got := decodeStoredBase64(false, true); string(got.([]byte)) != "false" {
		t.Errorf("binary: got %v, want []byte(false)", got)
	}
}

func TestDecodeStoredBase64_jsonNumberRepaired(t *testing.T) {
	// #736: a TEXT/BLOB value mis-promoted to a JSON number (e.g. the literal
	// string "123") must be restored to its original textual literal.
	if got := decodeStoredBase64(json.Number("123"), false); got != "123" {
		t.Errorf("text: got %v (%T), want %q", got, got, "123")
	}
	if got := decodeStoredBase64(json.Number("0"), true); string(got.([]byte)) != "0" {
		t.Errorf("binary: got %v, want []byte(0)", got)
	}
}

func TestDecodeStoredBase64_nilNotGuessed(t *testing.T) {
	// A value that decoded to Go nil (originally the string "null", per the
	// pre-fix bug, OR a genuine SQL NULL — indistinguishable after the fact)
	// must be left as nil rather than guessed at.
	if got := decodeStoredBase64(nil, false); got != nil {
		t.Errorf("expected nil to pass through unchanged, got %v", got)
	}
}

func TestDecodeStoredBase64_jsonContainerUnaffected(t *testing.T) {
	// #736 added "json" to base64StoredKind, so decodeStoredBase64 is now
	// invoked for JSON-typed columns too — a genuine JSON object/array value
	// (the overwhelmingly common case) must pass through completely
	// unchanged, not just fall into some accidental no-op.
	obj := map[string]any{"a": json.Number("1")}
	if got := decodeStoredBase64(obj, false); fmt.Sprintf("%v", got) != fmt.Sprintf("%v", obj) {
		t.Errorf("expected map[string]any to pass through unchanged, got %v (%T)", got, got)
	}
	arr := []any{json.Number("1"), json.Number("2")}
	if got := decodeStoredBase64(arr, false); fmt.Sprintf("%v", got) != fmt.Sprintf("%v", arr) {
		t.Errorf("expected []any to pass through unchanged, got %v (%T)", got, got)
	}
}

func TestBase64StoredKind_json(t *testing.T) {
	binary, ok := base64StoredKind("json")
	if !ok || binary {
		t.Errorf("expected json => (binary=false, ok=true), got (%v, %v)", binary, ok)
	}
}

// TestBase64StoredKind_binaryVarbinary pins #756: metadata.MapRow now
// reinterprets BINARY/VARBINARY as []byte, so they take the same base64
// storage path as BLOB and must be reported as decodable+binary here too.
func TestBase64StoredKind_binaryVarbinary(t *testing.T) {
	for _, dt := range []string{"binary", "varbinary", "BINARY", "VarBinary"} {
		binary, ok := base64StoredKind(dt)
		if !ok || !binary {
			t.Errorf("base64StoredKind(%q) = (%v,%v), want (true,true)", dt, binary, ok)
		}
	}
}

// TestBase64StoredKind_spatialAndVector pins the #1136 entries: the spatial
// family and VECTOR are delivered by go-mysql as []byte (SRID+WKB / packed
// floats) and stored base64 like BLOB, so they must decode as binary here.
// This guards against drift between this copy and the deliberately-untouched
// sibling copies (internal/recovery, internal/shim) without needing Docker.
func TestBase64StoredKind_spatialAndVector(t *testing.T) {
	for _, dt := range []string{
		"geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection",
		"vector",
		"GEOMETRY", "Point", // case-insensitive
	} {
		binary, ok := base64StoredKind(dt)
		if !ok || !binary {
			t.Errorf("base64StoredKind(%q) = (%v,%v), want (true,true)", dt, binary, ok)
		}
	}
}
