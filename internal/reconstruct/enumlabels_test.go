package reconstruct

import (
	"encoding/base64"
	"testing"
)

// TestDecodeImageBinaries pins the per-image base64 decode primitive used by
// DecodeEventBinaries (#666): BLOB → []byte, TEXT → string, NULL stays nil, a
// non-blob/text column and an absent column are untouched.
func TestDecodeImageBinaries(t *testing.T) {
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	image := map[string]any{
		"id": float64(1),
		"bl": b64("BIN\x00BLOB"), // base64 binary blob with a NUL byte
		"tx": b64("hello text"),  // base64 text
		"bn": nil,                // NULL blob stays nil
		"vc": "plain varchar",    // not in the decode set → untouched
	}
	decodeImageBinaries(image, map[string]bool{"bl": true, "tx": false, "bn": true, "gone": true})

	if got, ok := image["bl"].([]byte); !ok || string(got) != "BIN\x00BLOB" {
		t.Errorf("bl: want decoded []byte \"BIN\\0BLOB\", got %T %v", image["bl"], image["bl"])
	}
	if got, ok := image["tx"].(string); !ok || got != "hello text" {
		t.Errorf("tx: want decoded string \"hello text\", got %T %v", image["tx"], image["tx"])
	}
	if image["bn"] != nil {
		t.Errorf("bn: NULL must stay nil, got %v", image["bn"])
	}
	if image["vc"] != "plain varchar" {
		t.Errorf("vc: column not in the decode set must be untouched, got %v", image["vc"])
	}
	if image["id"] != float64(1) {
		t.Errorf("id: non-blob/text column must be untouched, got %v", image["id"])
	}
	if _, present := image["gone"]; present {
		t.Errorf("a decode-set column absent from the image must not be materialized, got %v", image["gone"])
	}
}

// TestDecodeImageBinaries_noopGuards confirms the cheap no-op paths: an empty
// decode set or a nil image must not panic and must change nothing.
func TestDecodeImageBinaries_noopGuards(t *testing.T) {
	image := map[string]any{"bl": "QUJD"}
	decodeImageBinaries(image, nil)
	if image["bl"] != "QUJD" {
		t.Errorf("empty binCols must leave the image untouched, got %v", image["bl"])
	}
	decodeImageBinaries(nil, map[string]bool{"bl": true}) // must not panic
}

// TestFormatCell pins the table/CSV cell formatter (#666): a decoded BLOB column
// is a []byte and must render as base64 (matching the JSON path of the same
// command), not the raw "%v" decimal byte array; everything else uses "%v".
func TestFormatCell(t *testing.T) {
	raw := "BIN\x00BLOB"
	if got, want := formatCell([]byte(raw)), base64.StdEncoding.EncodeToString([]byte(raw)); got != want {
		t.Errorf("[]byte: got %q, want base64 %q (not a decimal array)", got, want)
	}
	if got := formatCell("hello text"); got != "hello text" {
		t.Errorf("string: got %q, want %q", got, "hello text")
	}
	if got := formatCell(int64(42)); got != "42" {
		t.Errorf("int64: got %q, want %q", got, "42")
	}
	if got := formatCell(nil); got != "<nil>" {
		t.Errorf("nil: got %q, want %q", got, "<nil>")
	}
}
