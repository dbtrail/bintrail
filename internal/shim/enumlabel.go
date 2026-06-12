package shim

import (
	"math"
	"strings"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// enumLabelMapper rewrites ENUM and SET ordinals stored in binlog row
// images back to their string labels (#472).
//
// Binlog ROW images store an ENUM value as its 1-based ordinal and a SET
// value as a bitmask, so every time-travel path that materializes rows
// from row_before/row_after would otherwise answer `'3'` where a live
// `SELECT` answers `'shipped'` — different representations of the same
// column on the same ProxySQL connection. The labels come from the schema
// snapshot's COLUMN_TYPE (e.g. `enum('pending','shipped')`), captured
// since #212.
//
// The mapping is deliberately conservative: anything that doesn't match a
// known ordinal exactly — out-of-range values (the enum shrank after the
// event), non-integral numbers, values already strings (the _snapshot
// baseline path seeds labels from mydumper dumps) — passes through
// unchanged rather than guessing.
//
// KNOWN LIMITATION: ordinals are decoded with the LATEST snapshot's
// definition, not the one in effect at the event's timestamp. Appending
// members (the common evolution) is safe, and end-shrink is caught by the
// out-of-range guard — but a REORDER or middle-member removal between the
// event and the latest snapshot maps an old ordinal to the wrong label
// with no signal. Documented in docs/time-travel-sql.md's Limitations;
// the fix (resolve the snapshot in effect at the event time via
// metadata.NewResolver(db, N)) is tracked as a follow-up issue.
type enumLabelMapper struct {
	enums map[string][]string // column → labels in 1-based ordinal order
	sets  map[string][]string // column → members in bit order (bit i ↔ member i)
}

// newEnumLabelMapper builds a mapper for the table's ENUM/SET columns.
// Returns nil — a valid no-op receiver for mapImage — when tm is nil
// (resolver unavailable; same degradation contract as columnOrderFor) or
// the table has no ENUM/SET columns, so the per-row mapping cost on the
// common path is a single nil check. Pre-#212 snapshots have empty
// ColumnType and naturally fall out as "no ENUM/SET columns".
func newEnumLabelMapper(tm *metadata.TableMeta) *enumLabelMapper {
	if tm == nil {
		return nil
	}
	var m *enumLabelMapper
	for _, c := range tm.Columns {
		labels, isSet, ok := parseEnumSetLabels(c.ColumnType)
		if !ok {
			continue
		}
		if m == nil {
			m = &enumLabelMapper{
				enums: make(map[string][]string),
				sets:  make(map[string][]string),
			}
		}
		if isSet {
			m.sets[c.Name] = labels
		} else {
			m.enums[c.Name] = labels
		}
	}
	return m
}

// mapImage rewrites the ENUM/SET ordinals in a single row image, in
// place. Images are request-scoped maps decoded from the index's JSON
// columns (or merged by the _snapshot paths), so in-place mutation is
// safe. Safe on a nil receiver and a nil image.
func (m *enumLabelMapper) mapImage(image map[string]any) {
	if m == nil || image == nil {
		return
	}
	for col, labels := range m.enums {
		v, present := image[col]
		if !present {
			continue
		}
		n, ok := ordinalValue(v)
		if !ok {
			continue
		}
		switch {
		case n == 0:
			// MySQL's sentinel for an invalid/empty ENUM entry.
			image[col] = ""
		case n <= uint64(len(labels)):
			image[col] = labels[n-1]
		}
	}
	for col, members := range m.sets {
		v, present := image[col]
		if !present {
			continue
		}
		n, ok := ordinalValue(v)
		if !ok {
			continue
		}
		if s, ok := setString(n, members); ok {
			image[col] = s
		}
	}
}

// ordinalValue extracts a non-negative integral ordinal from the value
// types a row image can carry. JSON-decoded images yield float64; the
// defensive integer cases cover merged values that skipped the JSON
// round-trip. Textual values (string or []byte — already labels),
// NULLs, negatives, non-integral floats, and floats beyond exact
// integer precision (2^53) all report !ok — the caller leaves those
// values untouched. The 2^53 bound also means a SET mask using members
// beyond bit 53 degrades to pass-through: such a mask is already
// precision-lossy after the JSON round-trip, so refusing it is the
// honest call.
func ordinalValue(v any) (uint64, bool) {
	switch n := v.(type) {
	case float64:
		if n < 0 || n != math.Trunc(n) || n > 1<<53 {
			return 0, false
		}
		return uint64(n), true
	case int:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int32:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int64:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case uint64:
		return n, true
	}
	return 0, false
}

// setString renders a SET bitmask as MySQL's comma-joined member list
// (bit i set → members[i] included, in definition order; 0 → "").
// Reports !ok when the mask carries bits beyond the known members —
// the definition changed since the event, so the number is more honest
// than a partial label list.
func setString(mask uint64, members []string) (string, bool) {
	if mask == 0 {
		return "", true
	}
	if len(members) < 64 && mask >= 1<<uint(len(members)) {
		return "", false
	}
	var picked []string
	for i, member := range members {
		if mask&(1<<uint(i)) != 0 {
			picked = append(picked, member)
		}
	}
	return strings.Join(picked, ","), true
}

// parseEnumSetLabels extracts the member labels from an
// information_schema COLUMN_TYPE declaration like
// `enum('pending','shipped')` or `set('a','b')`. Members are
// single-quoted with embedded quotes doubled (`'it''s'`), and may
// legitimately be empty (`enum('','a')`) or contain commas. MySQL
// additionally renders backslashes and control characters inside
// members with C-style escapes (verified on 8.0: `\\`, `\n`, `\r`,
// `\0`); those are decoded so the label's bytes match the live
// value. Reports !ok for any other type declaration ("int unsigned",
// "varchar(20)", the empty pre-#212 string), for malformed input,
// and for an escape sequence we don't recognize — !ok degrades to
// the honest raw ordinal, never a guessed label.
func parseEnumSetLabels(columnType string) (labels []string, isSet, ok bool) {
	ct := strings.TrimSpace(columnType)
	lower := strings.ToLower(ct)
	var inner string
	switch {
	case strings.HasPrefix(lower, "enum(") && strings.HasSuffix(lower, ")"):
		inner = ct[len("enum(") : len(ct)-1]
	case strings.HasPrefix(lower, "set(") && strings.HasSuffix(lower, ")"):
		inner = ct[len("set(") : len(ct)-1]
		isSet = true
	default:
		return nil, false, false
	}

	var cur strings.Builder
	inString := false  // inside a 'quoted' member
	pending := false   // a member has been opened since the last comma
	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if inString {
			switch ch {
			case '\'':
				if i+1 < len(inner) && inner[i+1] == '\'' {
					cur.WriteByte('\'') // doubled quote → literal quote
					i++
					continue
				}
				inString = false
			case '\\':
				// Quotes are only ever doubled (never \'), so a backslash
				// always starts one of MySQL's C-style escapes.
				if i+1 >= len(inner) {
					return nil, false, false
				}
				i++
				switch inner[i] {
				case '\\':
					cur.WriteByte('\\')
				case 'n':
					cur.WriteByte('\n')
				case 'r':
					cur.WriteByte('\r')
				case '0':
					cur.WriteByte(0)
				default:
					return nil, false, false
				}
			default:
				cur.WriteByte(ch)
			}
			continue
		}
		switch ch {
		case '\'':
			inString = true
			pending = true
		case ',':
			if !pending {
				return nil, false, false
			}
			labels = append(labels, cur.String())
			cur.Reset()
			pending = false
		case ' ', '\t':
			// whitespace between members
		default:
			return nil, false, false
		}
	}
	if inString || !pending {
		// unterminated quote, trailing comma, or empty list
		return nil, false, false
	}
	labels = append(labels, cur.String())
	return labels, isSet, true
}
