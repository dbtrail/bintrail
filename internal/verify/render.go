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
// Known divergence: FLOAT/DOUBLE text rendering between Go and MySQL is not
// guaranteed byte-identical (baseline FLOAT is read as float32 and widened);
// callers treat a float-only mismatch as inconclusive rather than a failure.
func renderCell(v any, col metadata.ColumnMeta) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	switch x := v.(type) {
	case json.Number:
		// Binlog event integers/decimals are JSON-decoded as json.Number, whose
		// String() is the original literal — so an integer >2^53 stays exact
		// (#496) instead of rounding through float64.
		return []byte(x.String()), nil
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	case time.Time:
		return renderTemporal(x, col), nil
	case int64:
		return []byte(strconv.FormatInt(x, 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(x), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(x, 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(x), 10)), nil
	case float64:
		return []byte(strconv.FormatFloat(x, 'g', -1, 64)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(x), 'g', -1, 32)), nil
	case bool:
		if x {
			return []byte("1"), nil
		}
		return []byte("0"), nil
	default:
		return nil, fmt.Errorf("renderCell: unsupported value type %T for column %q", v, col.Name)
	}
}

// renderTemporal formats a time.Time the way MySQL's CAST(col AS CHAR) does for
// the column's declared type: DATE → "2006-01-02"; DATETIME/TIMESTAMP →
// "2006-01-02 15:04:05" plus the column's fractional precision (0–6 digits).
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
