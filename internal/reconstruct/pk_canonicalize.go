package reconstruct

import (
	"fmt"
	"strings"
	"time"

	"github.com/dbtrail/bintrail/internal/metadata"
)

// canonicalizePKValue normalizes a raw Go value scanned from a baseline
// Parquet column to match the string representation that the bintrail
// indexer stored in binlog_events.pk_values for the same column.
//
// This is the linchpin of the merge-on-read PK lookup: the indexer calls
// parser.BuildPKValues with whatever types go-mysql delivered at parse time
// (INT → int32/int64, VARCHAR → string, DATETIME → pre-formatted string with
// parseTime=false as the bintrail default), while DuckDB parquet_scan returns
// its own type set (INT → int32/int64, VARCHAR → string, TIMESTAMP →
// time.Time). Without normalisation the PK strings diverge and every event
// silently misses the baseline row it was supposed to update.
//
// dataType is the lowercase MySQL type token from schema_snapshots
// (column_key / data_type), e.g. "int", "bigint", "varchar", "datetime",
// "timestamp".
//
// Supported canonicalisations (v1):
//
//   - int, bigint, smallint, tinyint, mediumint, int unsigned, etc. → %v
//     (already matches — go-mysql and DuckDB both deliver intN/uintN)
//   - char, varchar, text, enum, set → %v (string on both sides)
//   - datetime, timestamp → format the time.Time via "2006-01-02 15:04:05"
//     base pattern, appending ".000000" fraction only if the subsecond part
//     is non-zero, matching go-mysql's formatDatetime output for
//     DATETIME/TIMESTAMP columns when parseTime=false.
//   - date → "2006-01-02" when the input is a time.Time; pass-through for
//     strings.
//
// Unsupported types (fall through to %v and may not round-trip):
//
//   - decimal / numeric: go-mysql returns a pre-formatted string; DuckDB may
//     return string or float64 depending on column definition. Not handled.
//   - binary / varbinary / blob / bit: go-mysql returns []byte; DuckDB
//     returns []byte; %v formatting diverges because string([]byte) varies
//     with content. Not handled.
//   - year: go-mysql returns int; DuckDB returns int32. Works.
//   - json: not used as a PK column. Not handled.
//
// If the input is already a string (e.g. the indexer stored a pre-formatted
// representation and DuckDB returned the same), pass it through unchanged —
// this covers the common path where both sides already agree.
func canonicalizePKValue(raw any, dataType string) any {
	if raw == nil {
		return nil
	}
	dt := strings.ToLower(strings.TrimSpace(dataType))

	switch dt {
	case "datetime", "timestamp":
		return canonicalizeDatetime(raw)
	case "date":
		return canonicalizeDate(raw)
	default:
		// For the remaining types, fall through to the raw value. The
		// indexer-side BuildPKValues will fmt.Sprintf("%v", ...) it, and so
		// will the baseline side, so they match as long as the Go type is
		// the same on both sides (true for int/string types).
		return raw
	}
}

// canonicalizeDatetime converts a time.Time (typical DuckDB scan output for
// TIMESTAMP columns) into the string format that go-mysql's formatDatetime
// produces for DATETIME/TIMESTAMP row events when parseTime is false (the
// bintrail default): "2006-01-02 15:04:05" with an optional ".123456" tail
// when the subsecond part is non-zero.
//
// Strings are passed through unchanged: the indexer may have already stored
// a formatted string, and some DuckDB drivers return TIMESTAMP as string.
func canonicalizeDatetime(raw any) any {
	switch v := raw.(type) {
	case time.Time:
		// UTC normalisation matches the indexer convention for
		// event_timestamp and avoids tripping on local-time skew between
		// the original binlog and the Parquet reader.
		t := v.UTC()
		if t.Nanosecond() == 0 {
			return t.Format("2006-01-02 15:04:05")
		}
		// Microsecond precision — go-mysql's formatDatetime with dec=6.
		// Higher precision (nanoseconds) is truncated because MySQL tops
		// out at microsecond.
		return t.Format("2006-01-02 15:04:05.000000")
	case string:
		return v
	default:
		// Unknown concrete type — fall back to %v, same as the indexer.
		return fmt.Sprintf("%v", v)
	}
}

// canonicalizeDate converts a time.Time scanned from a DATE column into the
// "2006-01-02" string format the indexer stores. Strings pass through.
func canonicalizeDate(raw any) any {
	switch v := raw.(type) {
	case time.Time:
		return v.UTC().Format("2006-01-02")
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

// canonicalizePKMap takes a full row map and a PK column descriptor, and
// returns a new map with only the PK columns' values canonicalised according
// to their DataType. Non-PK columns are not touched (they are not used for
// key construction) — only the entries BuildPKValues will look up are
// rewritten.
//
// The source map is not mutated so callers can continue to use it for other
// purposes (e.g. passing to a MydumperWriter.WriteRow).
func canonicalizePKMap(row map[string]any, pkCols []metadata.ColumnMeta) map[string]any {
	out := make(map[string]any, len(row))
	for k, v := range row {
		out[k] = v
	}
	for _, col := range pkCols {
		if v, ok := out[col.Name]; ok {
			out[col.Name] = canonicalizePKValue(v, col.DataType)
		}
	}
	return out
}

// supportedPKType returns true if dataType is in the set of PK column types
// that canonicalizePKValue handles correctly. Callers use this at the start
// of a reconstruct run to warn operators about edge cases.
func supportedPKType(dataType string) bool {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "int", "integer", "smallint", "tinyint", "mediumint", "bigint",
		"int unsigned", "bigint unsigned", "smallint unsigned", "tinyint unsigned", "mediumint unsigned",
		"char", "varchar", "text", "tinytext", "mediumtext", "longtext",
		"enum", "set",
		"datetime", "timestamp", "date",
		"year":
		return true
	default:
		return false
	}
}
