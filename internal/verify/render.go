// Package verify reconstructs each table's state from baseline + binlog and
// compares its content fingerprint against the live source, proving that a
// recovery would reproduce the source. Capstone of the data-consistency epic
// (#631): the on-demand "does my recovery actually work?" check.
package verify

import (
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
