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
// This exists for one specific gap: a MySQL TEXT/LONGTEXT column (not the
// native JSON type) whose stored value happens to be JSON text — a common
// pattern for plugins that json_encode() into a text field. Once an event
// touches such a row, its event-image round-trips through Go's
// map[string]any (query.UnmarshalRowImage), which loses the original key
// order; renderCell's default case then re-marshals it with Go's own
// (alphabetically sorted) key order, while a baseline dump's text preserves
// the source's original order verbatim. Two renderings of identical data
// disagree on bytes alone. A column typed as MySQL's native JSON already has
// a narrower version of this same gap, covered by isDeferredType's
// inconclusive downgrade — this closes it more precisely, for the case
// canonicalization can actually resolve to a genuine match rather than
// merely downgrading to "can't tell": see renderCellCanonicalJSON.
//
// UseNumber preserves large-integer/decimal literals exactly, the same
// precision requirement renderCell's json.Number case already protects
// (#496). SetEscapeHTML(false) avoids gratuitously mangling '<'/'>'/'&' that
// a human reading a mismatch cell would otherwise have to puzzle over.
//
// Returns the input unchanged with ok=false when b is not a JSON
// object/array, or fails to parse.
func canonicalizeJSONContainer(b []byte) ([]byte, bool) {
	t := bytes.TrimSpace(b)
	if len(t) == 0 || (t[0] != '{' && t[0] != '[') {
		return b, false
	}
	if !json.Valid(t) {
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
	return bytes.TrimRight(buf.Bytes(), "\n"), true
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
