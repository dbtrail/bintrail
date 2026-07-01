// Package verify reconstructs each table's state from baseline + binlog and
// compares its content fingerprint against the live source, proving that a
// recovery would reproduce the source. Capstone of the data-consistency epic
// (#631): the on-demand "does my recovery actually work?" check.
package verify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// renderCell renders one reconstructed value into the exact text bytes that
// internal/consistency.ConsistentTableChecksum reads from the live source —
// MySQL's text-protocol form with the session pinned to UTC and DATE/DATETIME/
// TIMESTAMP read via CAST(... AS CHAR). A nil return is SQL NULL.
//
// Reconstructed values arrive in two shapes that must render identically: from
// the baseline they are DuckDB-native (int64/uint64/float64/time.Time/[]byte/
// string); from binlog events they are JSON-decoded (json.Number/string/[]byte/
// nil). The column metadata is needed only for temporal columns — to pick
// DATE's date-only form and DATETIME/TIMESTAMP's declared fractional precision,
// which the value's Go type alone cannot convey.
//
// renderCell never errors: it always produces bytes so the digest completes,
// and the caller decides conclusiveness. A value class whose rendering can't be
// guaranteed to match the source (FLOAT/DOUBLE text; JSON containers, which
// MySQL normalizes differently than Go) renders best-effort and, for columns the
// caller marks as deferred-representation, a resulting mismatch is reported
// inconclusive rather than as a failure (see VerifyTable). FLOAT/DOUBLE are not
// in the deferred set, so a float-only divergence surfaces as a (safe) mismatch,
// never as a false match — there is intentionally no float-inconclusive downgrade
// (that would create a masking path).
func renderCell(v any, col metadata.ColumnMeta) []byte {
	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case json.Number:
		// Binlog event integers/decimals are JSON-decoded as json.Number, whose
		// String() is the original literal — so an integer >2^53 stays exact
		// (#496) instead of rounding through float64.
		return []byte(x.String())
	case []byte:
		return x
	case string:
		return []byte(x)
	case time.Time:
		return renderTemporal(x, col)
	case int64:
		return []byte(strconv.FormatInt(x, 10))
	case int32:
		return []byte(strconv.FormatInt(int64(x), 10))
	case uint64:
		return []byte(strconv.FormatUint(x, 10))
	case uint32:
		return []byte(strconv.FormatUint(uint64(x), 10))
	case float64:
		return []byte(strconv.FormatFloat(x, 'g', -1, 64))
	case float32:
		return []byte(strconv.FormatFloat(float64(x), 'g', -1, 32))
	case bool:
		if x {
			return []byte("1")
		}
		return []byte("0")
	default:
		// JSON columns touched by an event decode to map[string]any / []any.
		// Marshal deterministically (Go sorts map keys) so the digest completes;
		// it won't match MySQL's canonical JSON text, but such columns are in the
		// deferred-representation set so the mismatch is reported inconclusive.
		if b, err := json.Marshal(v); err == nil {
			return b
		}
		return []byte(fmt.Sprintf("%v", v))
	}
}

// renderTemporal formats a time.Time the way MySQL's CAST(col AS CHAR) does for
// the column's declared type: DATE → "2006-01-02"; DATETIME/TIMESTAMP →
// "2006-01-02 15:04:05" plus the column's fractional precision (0–6 digits).
//
// Precision comes from col.ColumnType, which is empty on pre-#212 schema
// snapshots; with it empty a DATETIME(n>0) renders without its fraction and
// would spuriously mismatch the source. Modern snapshots carry ColumnType, so
// this only affects baselines/snapshots taken before #212 — re-run bintrail
// snapshot to refresh.
func renderTemporal(t time.Time, col metadata.ColumnMeta) []byte {
	t = t.UTC()
	if strings.EqualFold(strings.TrimSpace(col.DataType), "date") {
		return []byte(t.Format("2006-01-02"))
	}
	base := t.Format("2006-01-02 15:04:05")
	prec := temporalPrecision(col.ColumnType)
	if prec == 0 {
		return []byte(base)
	}
	frac := t.Nanosecond() / 1e3 // microseconds
	tail := fmt.Sprintf("%06d", frac)
	if prec > 6 {
		prec = 6
	}
	return []byte(base + "." + tail[:prec])
}

// temporalPrecision extracts the fractional-seconds precision from a column type
// like "datetime(6)" or "timestamp(3)". Returns 0 when none is declared.
func temporalPrecision(columnType string) int {
	open := strings.IndexByte(columnType, '(')
	if open < 0 {
		return 0
	}
	close := strings.IndexByte(columnType[open:], ')')
	if close < 0 {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(columnType[open+1 : open+close]))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// canonicalizeJSONContainer re-renders a JSON object/array value from its
// decoded form, so two byte-different-but-semantically-equal serializations
// of the same JSON content compare equal. Scoped to CONTAINERS ({...}/[...])
// only — a scalar that happens to also be valid JSON (a bare number, "true",
// or a quoted string) is left untouched, since key-order drift can only
// happen inside an object; touching scalars would only add risk (whitespace/
// escaping differences) for no benefit.
//
// This operates on the RENDERED bytes, not the column's declared SQL type, so
// it applies to any value that renders JSON-shaped — a native MySQL JSON
// column, and equally a MySQL TEXT/LONGTEXT column (not the native JSON type)
// whose stored value happens to be JSON text, a common pattern for plugins
// that json_encode() into a text field. That TEXT case is the gap this
// primarily exists to close: once an event touches such a row, its
// event-image round-trips through Go's map[string]any
// (query.UnmarshalRowImage), which loses the original key order; renderCell's
// default case then re-marshals it with Go's own (alphabetically sorted) key
// order, while a baseline dump's text preserves the source's original order
// verbatim. Two renderings of identical data disagree on bytes alone. A
// column typed as MySQL's native JSON already had a narrower version of this
// same gap, covered by isDeferredType's inconclusive downgrade — this closes
// it more precisely there too, resolving to a genuine match rather than
// merely downgrading to "can't tell": see renderCellCanonicalJSON.
//
// UseNumber preserves large-integer/decimal literals exactly, the same
// precision requirement renderCell's json.Number case already protects
// (#496). SetEscapeHTML(false) avoids gratuitously mangling '<'/'>'/'&' that
// a human reading a mismatch cell would otherwise have to puzzle over.
//
// Returns the input unchanged with ok=false when b is not a JSON
// object/array, fails to parse, is not valid UTF-8, decodes to a value
// containing U+FFFD (see the surrogate-escape note below), or contains a
// duplicate object key at any depth.
func canonicalizeJSONContainer(b []byte) ([]byte, bool) {
	t := bytes.TrimSpace(b)
	if len(t) == 0 || (t[0] != '{' && t[0] != '[') {
		return b, false
	}
	if !json.Valid(t) {
		return b, false
	}
	// Not eligible to canonicalize: raw content this transform would corrupt
	// or hide a divergence in, rather than merely reorder.
	//   - invalid UTF-8 bytes: both json.Decode/Encode replace them with
	//     U+FFFD, so two DIFFERENT invalid byte sequences could canonicalize
	//     to the SAME output — silently erasing a real difference.
	//   - a repeated key within one object: decoding into map[string]any keeps
	//     only the last occurrence (Go's stdlib behavior), which would make
	//     `{"a":1,"a":2}` and `{"a":2}` compare equal — but those are NOT the
	//     same source bytes, and if that duplicate-keyed value came from
	//     event-image reconstruction, this is exactly the kind of divergence
	//     verify exists to catch, not paper over.
	// All leave b returned unchanged, so a genuinely malformed/duplicated
	// value still falls back to the pre-fix byte comparison (conservative:
	// worst case reports a mismatch verify's caller downgrades to
	// inconclusive or a human reviews, never a silently masked one).
	if !utf8.Valid(t) {
		return b, false
	}
	if hasDuplicateObjectKeys(t) {
		return b, false
	}
	dec := json.NewDecoder(bytes.NewReader(t))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return b, false
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return b, false
	}
	out := bytes.TrimRight(buf.Bytes(), "\n")
	// An unpaired \uD800-\uDFFF surrogate escape is valid JSON syntax AND
	// valid raw UTF-8 (it's just six ASCII characters before unescaping), so
	// utf8.Valid(t) above does not catch it — the invalidity only appears
	// once json.Decode unescapes the string content, where Go silently
	// substitutes U+FFFD, same as it does for invalid raw bytes. Two
	// DIFFERENT unpaired surrogates would decode to the identical U+FFFD and
	// canonicalize to the identical output. Refuse whenever the canonical
	// form contains U+FFFD: a real, intentional U+FFFD in source data is
	// vanishingly rare, and the cost of a false refusal here is only a
	// fallback to raw-byte comparison, not a wrong answer.
	if bytes.ContainsRune(out, utf8.RuneError) {
		return b, false
	}
	return out, true
}

// hasDuplicateObjectKeys reports whether any JSON object within data (already
// confirmed json.Valid) repeats a key WITHIN THAT SAME OBJECT — a case Go's
// map[string]any decode would silently collapse to last-key-wins, exactly the
// kind of information loss canonicalizeJSONContainer must not introduce. Two
// sibling objects (or an object and its parent) reusing the same key name is
// NOT a duplicate — key uniqueness is scoped per object, not per nesting
// depth. Walks the raw token stream (not a map) so duplicates are visible
// before any collapsing decode runs.
func hasDuplicateObjectKeys(data []byte) bool {
	dup, _ := walkForDuplicateKeys(json.NewDecoder(bytes.NewReader(data)))
	return dup
}

// walkForDuplicateKeys consumes exactly one JSON value (scalar, object, or
// array) from dec and reports whether a duplicate object key was found
// anywhere within it, recursing into nested objects/arrays. err is non-nil
// only on a stream read failure, which cannot happen against
// already-json.Valid input; callers treat that case as "no duplicate found"
// (the same conservative, fall-back-to-raw-bytes behavior every other
// canonicalizeJSONContainer failure path takes).
func walkForDuplicateKeys(dec *json.Decoder) (bool, error) {
	tok, err := dec.Token()
	if err != nil {
		return false, err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return false, nil // scalar value: string/number/bool/null, no keys to check
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return false, err
			}
			key, _ := keyTok.(string) // object keys are always strings per the JSON grammar
			if seen[key] {
				return true, nil
			}
			seen[key] = true
			if dup, err := walkForDuplicateKeys(dec); dup || err != nil {
				return dup, err
			}
		}
		_, err := dec.Token() // consume the closing '}'
		return false, err
	case '[':
		for dec.More() {
			if dup, err := walkForDuplicateKeys(dec); dup || err != nil {
				return dup, err
			}
		}
		_, err := dec.Token() // consume the closing ']'
		return false, err
	default:
		return false, nil // stray '}'/']' — unreachable against valid JSON
	}
}

// renderCellCanonicalJSON renders a cell like renderCell, additionally
// canonicalizing a JSON object/array value (see canonicalizeJSONContainer)
// so a pure representation difference doesn't register as a content
// difference.
//
// Used ONLY by the baseline-anchored comparison (VerifyBaselinePair,
// ExplainBaselinePairMismatch): both operands there are produced by this
// same package, so canonicalizing them symmetrically cannot introduce a new
// disagreement. The live-source comparison (VerifyTable) keeps using
// renderCell unwrapped — its OTHER operand is MySQL's own raw text via
// internal/consistency.ConsistentTableChecksum, which this package does not
// control and does not canonicalize; wrapping only one side there would
// create a new mismatch instead of fixing one.
func renderCellCanonicalJSON(v any, col metadata.ColumnMeta) []byte {
	b := renderCell(v, col)
	if b == nil {
		return nil
	}
	if canon, ok := canonicalizeJSONContainer(b); ok {
		return canon
	}
	return b
}
