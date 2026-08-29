package query

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// PKCast names the 64-bit integer type both engines cast pk_values to before
// comparing it against a PKRange (#1440). pk_values is VARCHAR(512), so a bare
// comparison is lexicographic ("9" > "10"); the cast is what makes the range
// numeric, and it must be chosen by the column's declared signedness: a
// signed column can hold negatives, which CAST AS UNSIGNED / UBIGINT mangles,
// and an unsigned BIGINT can hold values above 2^63-1, which the signed cast
// overflows. The zero value is deliberately "unset": a range that reaches an
// engine without a resolved cast is refused, never guessed.
type PKCast uint8

const (
	// PKCastUnset is the zero value. ResolveCast replaces it; every engine
	// refuses a range still carrying it.
	PKCastUnset PKCast = iota
	// PKCastSigned casts with CAST(pk_values AS SIGNED) on MySQL and
	// TRY_CAST(pk_values AS BIGINT) on DuckDB.
	PKCastSigned
	// PKCastUnsigned casts with CAST(pk_values AS UNSIGNED) on MySQL and
	// TRY_CAST(pk_values AS UBIGINT) on DuckDB.
	PKCastUnsigned
)

// PKRange restricts results to events whose single-column integer primary key
// lies within the inclusive [Min, Max] window (#1440). Either bound may be nil
// (open on that side); at least one is set.
//
// Scope: single-column integer primary keys only (TINYINT through BIGINT,
// signed or unsigned). The shape is checked against the schema snapshot by
// ResolveCast BEFORE any query runs; a composite, DECIMAL, FLOAT or string key
// is refused with a message naming the table's actual key, never answered
// lexicographically.
//
// Cost: the cast predicate cannot use the pk_hash index. Within the partitions
// the time filters keep it is a scan, which is acceptable for an incident
// window and the reason every surface's help text says to pair a range with
// since/until.
//
// Both engines exclude rows whose pk_values is not an integer of the chosen
// width: MySQL through an explicit `pk_values <> ”` guard (CAST(” AS
// UNSIGNED) is 0, which a range starting at 0 would otherwise match), DuckDB
// through TRY_CAST (NULL compares false). On an integer-keyed table the only
// such rows are the #318 drift rows with an empty key.
type PKRange struct {
	// Cast is the integer type both engines compare through. Set by
	// ResolveCast from the resolved PK column; PKCastUnset is refused.
	Cast PKCast
	// Min and Max are the inclusive bounds. nil leaves that side open.
	Min, Max *big.Int
}

var (
	pkBoundRE   = regexp.MustCompile(`^-?[0-9]+$`)
	bigMaxInt64 = big.NewInt(math.MaxInt64)
	bigMinInt64 = big.NewInt(math.MinInt64)
	bigMaxUint  = new(big.Int).SetUint64(math.MaxUint64)
)

// ParsePKBound parses one bound of a primary key range: an optionally negative
// run of decimal digits. Width is not checked here; the column's signedness
// decides it in ResolveCast.
func ParsePKBound(s string) (*big.Int, error) {
	s = strings.TrimSpace(s)
	if !pkBoundRE.MatchString(s) {
		return nil, fmt.Errorf("%q is not an integer", s)
	}
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("%q is not an integer", s)
	}
	return v, nil
}

// NewPKRange builds an UNRESOLVED range from parsed bounds. It returns nil
// when both are nil, and refuses an inverted window. The result must go
// through ResolveCast before it reaches an engine.
func NewPKRange(lo, hi *big.Int) (*PKRange, error) {
	if lo == nil && hi == nil {
		return nil, nil
	}
	if lo != nil && hi != nil && lo.Cmp(hi) > 0 {
		return nil, fmt.Errorf("lower bound %s is above upper bound %s", lo, hi)
	}
	return &PKRange{Min: lo, Max: hi}, nil
}

// integerDataTypes are the MySQL/MariaDB information_schema DATA_TYPE values
// of the integer family. INTEGER is normalised to "int" by the server.
var integerDataTypes = map[string]bool{
	"tinyint": true, "smallint": true, "mediumint": true, "int": true, "bigint": true,
}

// ResolveCast checks the table's primary key shape against the range's scope
// and, when it fits, picks the cast from the column's declared signedness and
// checks that the bounds fit that width. It must run BEFORE any query: the
// refusals here are the difference between "no" and a silently wrong answer.
func (r *PKRange) ResolveCast(tm *metadata.TableMeta) error {
	if tm == nil {
		return errors.New("range filters need a single integer primary key; the table is not in the schema snapshot")
	}
	pks := tm.PKColumnMetas()
	switch len(pks) {
	case 0:
		return errors.New("range filters need a single integer primary key; this table has no primary key in the schema snapshot")
	case 1:
	default:
		return fmt.Errorf("range filters need a single integer primary key; this table's is (%s)", strings.Join(tm.PKColumns, ", "))
	}
	col := pks[0]
	dataType := strings.ToLower(col.DataType)
	if dataType == "" {
		// PostgreSQL snapshots never record data_type (the type lives in
		// pg_type_oid, which the resolver does not load), and no MySQL
		// snapshot omits it. Say which case this is not.
		return fmt.Errorf("range filters need a single integer primary key; the schema snapshot does not record the type of this table's key column (%s), which a MySQL or MariaDB snapshot always does", col.Name)
	}
	if !integerDataTypes[dataType] {
		return fmt.Errorf("range filters need a single integer primary key; this table's is (%s %s)", col.Name, describeColumnType(col))
	}
	if col.ColumnType == "" {
		// Pre-#212 snapshots carry data_type but not column_type, so the
		// signedness (the whole reason for the cast choice) is unknown.
		return fmt.Errorf("range filters need to know whether the primary key is signed; the schema snapshot does not record it for (%s %s), run `bintrail snapshot` again to refresh it", col.Name, dataType)
	}
	cast := PKCastSigned
	if strings.Contains(strings.ToLower(col.ColumnType), "unsigned") {
		cast = PKCastUnsigned
	}
	if err := checkPKBoundsFit(cast, r.Min, r.Max, col.Name+" "+describeColumnType(col)); err != nil {
		return err
	}
	r.Cast = cast
	return nil
}

// describeColumnType renders the column's declared type for a refusal:
// the full COLUMN_TYPE when the snapshot has it, else DATA_TYPE.
func describeColumnType(col metadata.ColumnMeta) string {
	if col.ColumnType != "" {
		return strings.ToLower(col.ColumnType)
	}
	return strings.ToLower(col.DataType)
}

// checkPKBoundsFit refuses a bound the chosen cast cannot represent. keyDesc
// names the column in the message ("id bigint unsigned").
func checkPKBoundsFit(cast PKCast, lo, hi *big.Int, keyDesc string) error {
	check := func(label string, v *big.Int) error {
		if v == nil {
			return nil
		}
		switch cast {
		case PKCastUnsigned:
			if v.Sign() < 0 {
				return fmt.Errorf("%s %s is negative, but the primary key column is unsigned (%s)", label, v, keyDesc)
			}
			if v.Cmp(bigMaxUint) > 0 {
				return fmt.Errorf("%s %s is above the largest unsigned 64-bit value %s (%s)", label, v, bigMaxUint, keyDesc)
			}
		case PKCastSigned:
			if v.Cmp(bigMaxInt64) > 0 {
				return fmt.Errorf("%s %s is above the largest signed 64-bit value %s, and the primary key column is signed (%s)", label, v, bigMaxInt64, keyDesc)
			}
			if v.Cmp(bigMinInt64) < 0 {
				return fmt.Errorf("%s %s is below the smallest signed 64-bit value %s (%s)", label, v, bigMinInt64, keyDesc)
			}
		default:
			return errors.New("primary key range cast is not resolved; check the table's key against the schema snapshot first")
		}
		return nil
	}
	if err := check("lower bound", lo); err != nil {
		return err
	}
	return check("upper bound", hi)
}

// Validate is the engine-side belt: it refuses an unresolved cast, an empty
// window, an inverted one, and a bound outside the cast's width. Every engine
// that emits the predicate calls it, so a hand-built Options cannot reach the
// SQL with a guessed cast.
func (r *PKRange) Validate() error {
	if r == nil {
		return nil
	}
	if r.Cast != PKCastSigned && r.Cast != PKCastUnsigned {
		return errors.New("query: primary key range cast is not resolved; check the table's key against the schema snapshot first")
	}
	if r.Min == nil && r.Max == nil {
		return errors.New("query: primary key range has no bounds")
	}
	if r.Min != nil && r.Max != nil && r.Min.Cmp(r.Max) > 0 {
		return fmt.Errorf("query: primary key range lower bound %s is above upper bound %s", r.Min, r.Max)
	}
	keyDesc := "signed"
	if r.Cast == PKCastUnsigned {
		keyDesc = "unsigned"
	}
	if err := checkPKBoundsFit(r.Cast, r.Min, r.Max, keyDesc); err != nil {
		return fmt.Errorf("query: %w", err)
	}
	return nil
}

// Contains reports whether a stored pk_values string falls inside the range,
// with the same semantics as the SQL predicates: the text is parsed as an
// integer of the cast's width and anything that does not parse is out
// (DuckDB's TRY_CAST returns NULL there). An unresolved cast matches nothing.
// Used by the in-memory buffer, which has no SQL engine to cast for it.
func (r *PKRange) Contains(pkValues string) bool {
	if r == nil {
		return true
	}
	var v *big.Int
	switch r.Cast {
	case PKCastUnsigned:
		u, err := strconv.ParseUint(pkValues, 10, 64)
		if err != nil {
			return false
		}
		v = new(big.Int).SetUint64(u)
	case PKCastSigned:
		i, err := strconv.ParseInt(pkValues, 10, 64)
		if err != nil {
			return false
		}
		v = big.NewInt(i)
	default:
		return false
	}
	if r.Min != nil && v.Cmp(r.Min) < 0 {
		return false
	}
	if r.Max != nil && v.Cmp(r.Max) > 0 {
		return false
	}
	return true
}

// mysqlPredicates renders the live-index WHERE fragments. The bounds are
// inlined as integer literals rather than bound, for the same reason the
// partition hint above inlines TO_SECONDS: the value is a validated digit run
// (ParsePKBound), so inlining is safe, and the archive side has to inline
// anyway (see DuckDBPredicates), so one rendering keeps the two predicates
// comparable. MySQL types a literal above 2^63-1 as BIGINT UNSIGNED, so the
// comparison against CAST(... AS UNSIGNED) stays exact.
func (r *PKRange) mysqlPredicates() []string {
	cast := "SIGNED"
	if r.Cast == PKCastUnsigned {
		cast = "UNSIGNED"
	}
	// CAST('' AS UNSIGNED) is 0 with a warning, so a #318 drift row (empty
	// pk_values) would match any range that includes 0. Exclude it up front,
	// which also keeps this side in step with DuckDB's TRY_CAST returning
	// NULL for the same row.
	preds := []string{"pk_values <> ''"}
	if r.Min != nil {
		preds = append(preds, fmt.Sprintf("CAST(pk_values AS %s) >= %s", cast, r.Min))
	}
	if r.Max != nil {
		preds = append(preds, fmt.Sprintf("CAST(pk_values AS %s) <= %s", cast, r.Max))
	}
	return preds
}

// DuckDBPredicates renders the archive-side WHERE fragments, the mirror of the
// live predicate over the Parquet pk_values column. TRY_CAST, not CAST: an
// hour's archive file holds every table's events, and a plain CAST would
// abort the whole scan on the first string key of some OTHER table, while
// TRY_CAST yields NULL there and the comparison excludes the row. Bounds are
// inlined because database/sql's default value converter, which the duckdb
// driver relies on, refuses a uint64 with the high bit set as a bind value,
// and an unsigned BIGINT key can sit exactly there; DuckDB types a literal
// above 2^63-1 wide enough to compare exactly against UBIGINT.
func (r *PKRange) DuckDBPredicates() []string {
	typ := "BIGINT"
	if r.Cast == PKCastUnsigned {
		typ = "UBIGINT"
	}
	var preds []string
	if r.Min != nil {
		preds = append(preds, fmt.Sprintf("TRY_CAST(pk_values AS %s) >= %s", typ, r.Min))
	}
	if r.Max != nil {
		preds = append(preds, fmt.Sprintf("TRY_CAST(pk_values AS %s) <= %s", typ, r.Max))
	}
	return preds
}

// ValidatePKRange is the cross-field check every engine runs before emitting
// a range predicate: the range needs schema and table (the shape check and
// the scan bound both depend on them), cannot be combined with an exact key
// lookup, and must itself be valid and resolved. Surfaces validate the same
// things earlier with their own flag names; this is the belt for a
// hand-built Options.
func (o Options) ValidatePKRange() error {
	if o.PKRange == nil {
		return nil
	}
	if o.Schema == "" || o.Table == "" {
		return errors.New("query: a primary key range needs both schema and table")
	}
	if o.PKValues != "" || len(o.PKValuesIn) > 0 {
		return errors.New("query: a primary key range cannot be combined with an exact primary key lookup")
	}
	return o.PKRange.Validate()
}
