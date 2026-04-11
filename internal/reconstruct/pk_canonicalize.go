package reconstruct

import (
	"errors"
	"fmt"
	"strconv"
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
// (INT → int32/int64, VARCHAR → string, DATETIME → pre-formatted string
// with parseTime=false as the bintrail default), while DuckDB parquet_scan
// returns its own type set (INT → int32/int64, VARCHAR → string, TIMESTAMP
// → time.Time). Without normalisation the PK strings diverge and every
// event silently misses the baseline row it was supposed to update.
//
// col carries the column's full metadata including ColumnType, which for
// DATETIME/TIMESTAMP encodes the declared fractional precision (e.g.
// "datetime(6)"). Without precision the canonicalizer cannot distinguish
// DATETIME(0) from DATETIME(6) with a whole-second value — the indexer
// stores "14:30:45" and "14:30:45.000000" for those two cases respectively,
// and both scan back to a time.Time with Nanosecond()==0.
//
// Returns an error on any condition the canonicalizer cannot translate
// losslessly: nil PK value (MySQL forbids NULL in PKs, so nil means a
// bug), DATETIME scan not a time.Time/string, or any type not in the
// supported set. Error → the caller must abort the table reconstruction
// rather than silently produce wrong output.
//
// Supported data types (v1 + #212 + #214):
//
//   - int, smallint, tinyint, mediumint, bigint (+ unsigned): pass-through
//     (indexer and DuckDB both deliver intN/uintN — fmt.Sprintf("%v", ...)
//     produces identical decimal strings)
//   - char, varchar, text, tinytext, mediumtext, longtext, enum, set:
//     pass-through (both deliver string)
//   - datetime, timestamp: time.Time → fractional-precision-aware format
//     matching go-mysql's formatDatetime output
//   - date: time.Time → "2006-01-02"
//   - year: pass-through (go-mysql int, DuckDB int32 — both stringify the
//     same via %v)
//   - decimal, numeric: pass-through string (#214). go-mysql v1.13.0's
//     decodeDecimal returns a pre-formatted string when useDecimal is
//     false — and bintrail never sets useDecimal, so every DECIMAL PK
//     lands in the indexer as a Go string like "0.00" or "-99.99". The
//     baseline writer stores DECIMAL as parquet.String() (see
//     internal/baseline/schema.go), fed from mydumper SQL output via
//     parseSQLValue's default branch — which also returns the unquoted
//     numeric literal verbatim. Both sides end up with byte-identical
//     strings, so this branch is a pure type-check + pass-through. Zero
//     values are safe: decodeDecimal writes "0" (not "") for zero-leading
//     integer parts at row_event.go:1565-1567.
//
// Unsupported types fall through to an error. The caller runs
// supportedPKType upstream to catch these at reconstruct-start before any
// real work happens, but this path is the defense-in-depth check.
//
// Why BINARY/VARBINARY/BLOB/BIT/JSON are NOT supported here: the indexer
// and baseline-writer representations diverge on the disk level, not just
// at the Go-type level, so no canonicalizer can reconcile them without
// touching one of those upstream code paths. BINARY/VARBINARY go through
// go-mysql's decodeString → Go string of raw bytes, while mydumper emits
// them as 0x... hex literals that baseline.parseSQLValue returns as literal
// ASCII (reader_sql.go:134-140 does not decode hex). BLOB variants and BIT
// have similar upstream mismatches. Fixing those requires modifying
// parser.BuildPKValues or internal/baseline/reader_sql.go, both of which
// are non-additive changes to data already on disk. Tracked as follow-up
// issues.
func canonicalizePKValue(raw any, col metadata.ColumnMeta) (any, error) {
	if raw == nil {
		return nil, fmt.Errorf("canonicalizePKValue: nil PK value for column %q (MySQL forbids NULL in PK columns; baseline row may be missing the column after schema drift)", col.Name)
	}
	dt := strings.ToLower(strings.TrimSpace(col.DataType))

	switch dt {
	case "int", "integer", "smallint", "tinyint", "mediumint", "bigint", "year":
		// Both indexer and DuckDB deliver Go int/uint types; %v
		// produces identical strings. Pass-through without inspection so
		// int32/int64/uint32/uint64 differences don't matter.
		return raw, nil

	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set":
		// Both sides deliver Go string. Reject non-strings because that
		// would indicate a type mismatch we can't reason about.
		if _, ok := raw.(string); !ok {
			return nil, fmt.Errorf("canonicalizePKValue: %s column %q: expected string, got %T", dt, col.Name, raw)
		}
		return raw, nil

	case "decimal", "numeric":
		// go-mysql returns a pre-formatted string (useDecimal=false, the
		// bintrail default — see cmd/bintrail/stream.go and agent.go which
		// never set it). DuckDB returns the Parquet string column as a Go
		// string too (baseline stores DECIMAL as parquet.String to avoid
		// precision loss). Both sides agree byte-for-byte, so pass-through
		// is correct. Reject non-strings for the same reason as varchar
		// above — a type mismatch here means the caller passed us raw data
		// from a non-string Parquet column and we can't reason about it.
		if _, ok := raw.(string); !ok {
			return nil, fmt.Errorf("canonicalizePKValue: %s column %q: expected string, got %T", dt, col.Name, raw)
		}
		return raw, nil

	case "datetime", "timestamp":
		return canonicalizeDatetime(raw, col)
	case "date":
		return canonicalizeDate(raw, col)

	default:
		return nil, fmt.Errorf("canonicalizePKValue: column %q has unsupported PK type %q (BINARY/VARBINARY/BLOB/BIT/JSON/spatial PK types are not supported because the indexer and baseline-writer representations diverge on disk; file a follow-up issue if you need one)", col.Name, col.DataType)
	}
}

// canonicalizeDatetime converts a time.Time (typical DuckDB scan output for
// TIMESTAMP columns) into the string format that go-mysql's formatDatetime
// produces for DATETIME/TIMESTAMP row events when parseTime is false (the
// bintrail default): "2006-01-02 15:04:05" with a trailing "%0Nd" fraction
// where N is the column's declared precision (0-6).
//
// Precision comes from parsing col.ColumnType, e.g. "datetime(3)" → 3.
// A bare "datetime" with no precision means DATETIME(0). When ColumnType
// is empty (pre-#212 snapshot), we fall back to a Nanosecond()==0 heuristic:
// no fraction if nanoseconds are zero, full microsecond tail otherwise.
// This best-effort mode handles the common DATETIME(0) case correctly but
// is unreliable for DATETIME(N>0) PKs — users hit by that mode should
// re-run `bintrail snapshot` to refresh schema_snapshots.
func canonicalizeDatetime(raw any, col metadata.ColumnMeta) (any, error) {
	switch v := raw.(type) {
	case time.Time:
		t := v.UTC() // indexer stores UTC; guard against non-UTC DuckDB output
		dec, known := parseDatetimePrecision(col.ColumnType)
		if !known {
			// Pre-#212 snapshot fallback: best-effort formatting based on
			// whether the scanned value has sub-second content. Reliable
			// for DATETIME(0); unreliable for DATETIME(N>0) whole-second
			// values.
			if t.Nanosecond() == 0 {
				return t.Format("2006-01-02 15:04:05"), nil
			}
			return t.Format("2006-01-02 15:04:05.000000"), nil
		}
		if dec == 0 {
			return t.Format("2006-01-02 15:04:05"), nil
		}
		// Format with full microsecond tail, then slice off (6-dec) digits
		// to match go-mysql's formatDatetime output at the declared precision.
		full := t.Format("2006-01-02 15:04:05.000000")
		return full[:len(full)-(6-dec)], nil
	case string:
		return v, nil
	default:
		return nil, fmt.Errorf("canonicalizeDatetime: column %q: expected time.Time or string, got %T", col.Name, raw)
	}
}

// canonicalizeDate converts a time.Time scanned from a DATE column into the
// "2006-01-02" string format the indexer stores. Strings pass through.
func canonicalizeDate(raw any, col metadata.ColumnMeta) (any, error) {
	switch v := raw.(type) {
	case time.Time:
		return v.UTC().Format("2006-01-02"), nil
	case string:
		return v, nil
	default:
		return nil, fmt.Errorf("canonicalizeDate: column %q: expected time.Time or string, got %T", col.Name, raw)
	}
}

// parseDatetimePrecision extracts the declared fractional second precision
// from a COLUMN_TYPE string like "datetime(6)". Returns (precision, true)
// on a successful parse, (0, true) for a bare "datetime" (DATETIME(0)),
// and (0, false) when ColumnType is empty (pre-#212 snapshot without
// column_type populated).
func parseDatetimePrecision(columnType string) (int, bool) {
	s := strings.ToLower(strings.TrimSpace(columnType))
	if s == "" {
		return 0, false
	}
	// Strip off known prefixes; the precision lives in parentheses.
	for _, prefix := range []string{"datetime", "timestamp"} {
		if !strings.HasPrefix(s, prefix) {
			continue
		}
		rest := strings.TrimPrefix(s, prefix)
		if rest == "" {
			// Bare "datetime" → DATETIME(0).
			return 0, true
		}
		if !strings.HasPrefix(rest, "(") || !strings.HasSuffix(rest, ")") {
			// Malformed — fall through to "unknown".
			return 0, false
		}
		digits := rest[1 : len(rest)-1]
		n, err := strconv.Atoi(digits)
		if err != nil || n < 0 || n > 6 {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// ErrPKColumnMissing is a sentinel matchable via errors.Is for the case
// where a PK column declared in the resolver was not found in the row map
// passed to canonicalizePKMap. Returned errors are always typed as
// *MissingPKColumnError so callers that need the offending column name
// can recover it via errors.As.
var ErrPKColumnMissing = errors.New("PK column missing from row")

// MissingPKColumnError carries the specific column name that was absent
// from the baseline row, letting callers produce actionable error messages
// without string-parsing. Returned wrapped as `errors.Is(err,
// ErrPKColumnMissing)` via the Is method below.
type MissingPKColumnError struct {
	Column string
}

func (e *MissingPKColumnError) Error() string {
	return fmt.Sprintf("%s: column %q not in baseline row (run `bintrail snapshot` to refresh the schema snapshot if the table has been altered)",
		ErrPKColumnMissing.Error(), e.Column)
}

// Is returns true when target is the ErrPKColumnMissing sentinel, so
// `errors.Is(err, ErrPKColumnMissing)` keeps working even when the
// concrete type is *MissingPKColumnError.
func (e *MissingPKColumnError) Is(target error) bool {
	return target == ErrPKColumnMissing
}

// canonicalizePKMap takes a full row map and a PK column descriptor, and
// returns a new map with only the PK columns' values canonicalised
// according to their metadata. Non-PK columns flow through untouched.
//
// The source map is not mutated. Errors propagate from canonicalizePKValue
// and block the caller from attempting a downstream lookup that would
// return a garbage key.
func canonicalizePKMap(row map[string]any, pkCols []metadata.ColumnMeta) (map[string]any, error) {
	out := make(map[string]any, len(row))
	for k, v := range row {
		out[k] = v
	}
	for _, col := range pkCols {
		raw, ok := out[col.Name]
		if !ok {
			return nil, &MissingPKColumnError{Column: col.Name}
		}
		val, err := canonicalizePKValue(raw, col)
		if err != nil {
			return nil, err
		}
		out[col.Name] = val
	}
	return out, nil
}

// supportedPKType returns true if dataType is in the set of PK column types
// that canonicalizePKValue handles correctly. Callers use this at the start
// of a reconstruct run to warn operators about edge cases.
//
// Only DATA_TYPE values are expected here (lowercase base type from
// information_schema.COLUMNS.DATA_TYPE, e.g. "int", "datetime"), not the
// full COLUMN_TYPE. MySQL's DATA_TYPE never contains the "unsigned"
// qualifier — that lives in COLUMN_TYPE.
func supportedPKType(dataType string) bool {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "int", "integer", "smallint", "tinyint", "mediumint", "bigint",
		"char", "varchar", "text", "tinytext", "mediumtext", "longtext",
		"enum", "set",
		"datetime", "timestamp", "date",
		"year",
		"decimal", "numeric":
		return true
	default:
		return false
	}
}
