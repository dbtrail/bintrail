package query

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// ─── #1440: pk_min/pk_max over single-column integer primary keys ───────────

func bigStr(t *testing.T, s string) *big.Int {
	t.Helper()
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("bad test literal %q", s)
	}
	return v
}

func TestParsePKBound(t *testing.T) {
	for _, s := range []string{"0", "9", "-5", "18446744073709551615", "99999999999999999999999", " 42 "} {
		if _, err := ParsePKBound(s); err != nil {
			t.Errorf("ParsePKBound(%q) = %v, want ok", s, err)
		}
	}
	for _, s := range []string{"", "abc", "1.5", "1e3", "0x10", "+5", "--1", "1|2", "9 9"} {
		if _, err := ParsePKBound(s); err == nil {
			t.Errorf("ParsePKBound(%q) accepted, want refusal", s)
		}
	}
}

func TestNewPKRange(t *testing.T) {
	if r, err := NewPKRange(nil, nil); err != nil || r != nil {
		t.Errorf("both nil: got (%v, %v), want (nil, nil)", r, err)
	}
	if _, err := NewPKRange(big.NewInt(10), big.NewInt(9)); err == nil || !strings.Contains(err.Error(), "above upper bound") {
		t.Errorf("inverted window accepted: %v", err)
	}
	r, err := NewPKRange(big.NewInt(10), big.NewInt(10))
	if err != nil || r == nil || r.Cast != PKCastUnset {
		t.Errorf("equal bounds: got (%+v, %v), want an unresolved range", r, err)
	}
}

func tableWith(cols ...metadata.ColumnMeta) *metadata.TableMeta {
	tm := &metadata.TableMeta{Schema: "app", Table: "t", Columns: cols}
	for _, c := range cols {
		if c.IsPK {
			tm.PKColumns = append(tm.PKColumns, c.Name)
		}
	}
	return tm
}

func pkCol(name, dataType, columnType string) metadata.ColumnMeta {
	return metadata.ColumnMeta{Name: name, IsPK: true, DataType: dataType, ColumnType: columnType}
}

func TestPKRange_ResolveCast_shapeMatrix(t *testing.T) {
	cases := []struct {
		name     string
		tm       *metadata.TableMeta
		lo, hi   string
		wantCast PKCast
		wantErr  string // substring; "" = success
	}{
		{"nil table", nil, "1", "", 0, "not in the schema snapshot"},
		{"no pk", tableWith(metadata.ColumnMeta{Name: "x", DataType: "int", ColumnType: "int"}), "1", "", 0, "has no primary key"},
		{"composite", tableWith(pkCol("a", "int", "int"), pkCol("b", "int", "int")), "1", "", 0, "this table's is (a, b)"},
		{"varchar", tableWith(pkCol("sku", "varchar", "varchar(32)")), "1", "", 0, "this table's is (sku varchar(32))"},
		{"decimal", tableWith(pkCol("id", "decimal", "decimal(20,0)")), "1", "", 0, "this table's is (id decimal(20,0))"},
		{"float", tableWith(pkCol("id", "float", "float")), "1", "", 0, "(id float)"},
		{"binary", tableWith(pkCol("id", "binary", "binary(16)")), "1", "", 0, "(id binary(16))"},
		{"pg snapshot (no data_type)", tableWith(pkCol("id", "", "")), "1", "", 0, "A PostgreSQL snapshot never records it, so there is nothing to re-run"},
		{"pre-#212 snapshot (no column_type)", tableWith(pkCol("id", "bigint", "")), "1", "", 0, "does not record it for (id bigint)"},
		{"tinyint signed", tableWith(pkCol("id", "tinyint", "tinyint(4)")), "-1", "5", PKCastSigned, ""},
		{"smallint unsigned", tableWith(pkCol("id", "smallint", "smallint(5) unsigned")), "0", "5", PKCastUnsigned, ""},
		{"mediumint", tableWith(pkCol("id", "mediumint", "mediumint(9)")), "7", "", PKCastSigned, ""},
		{"int unsigned, upper case", tableWith(pkCol("id", "INT", "INT(10) UNSIGNED")), "", "9", PKCastUnsigned, ""},
		{"bigint signed full width", tableWith(pkCol("id", "bigint", "bigint(20)")), "-9223372036854775808", "9223372036854775807", PKCastSigned, ""},
		{"bigint unsigned full width", tableWith(pkCol("id", "bigint", "bigint(20) unsigned")), "0", "18446744073709551615", PKCastUnsigned, ""},
		{"negative on unsigned", tableWith(pkCol("id", "bigint", "bigint unsigned")), "-1", "", 0, "lower bound -1 is negative, but the primary key column is unsigned (id bigint unsigned)"},
		{"negative max on unsigned", tableWith(pkCol("id", "int", "int unsigned")), "", "-1", 0, "upper bound -1 is negative"},
		{"above int64 on signed", tableWith(pkCol("id", "bigint", "bigint")), "", "9223372036854775808", 0, "upper bound 9223372036854775808 is above the largest signed 64-bit value 9223372036854775807, and the primary key column is signed (id bigint)"},
		{"below int64 on signed", tableWith(pkCol("id", "bigint", "bigint")), "-9223372036854775809", "", 0, "below the smallest signed 64-bit value"},
		{"above uint64 on unsigned", tableWith(pkCol("id", "bigint", "bigint unsigned")), "18446744073709551616", "", 0, "above the largest unsigned 64-bit value 18446744073709551615"},
		// A bound wider than the column's own width but inside the cast is
		// fine: the comparison is well defined and simply matches nothing.
		{"tinyint with a bigint-sized bound", tableWith(pkCol("id", "tinyint", "tinyint")), "1000", "", PKCastSigned, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var lo, hi *big.Int
			if tc.lo != "" {
				lo = bigStr(t, tc.lo)
			}
			if tc.hi != "" {
				hi = bigStr(t, tc.hi)
			}
			r := &PKRange{Min: lo, Max: hi}
			err := r.ResolveCast(tc.tm)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ResolveCast accepted, want refusal containing %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ResolveCast error %q does not contain %q", err, tc.wantErr)
				}
				if r.Cast != PKCastUnset {
					t.Errorf("a refused range must stay unresolved, got cast %d", r.Cast)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveCast: %v", err)
			}
			if r.Cast != tc.wantCast {
				t.Errorf("cast = %d, want %d", r.Cast, tc.wantCast)
			}
		})
	}
}

func TestPKRange_Validate(t *testing.T) {
	one := big.NewInt(1)
	if err := (&PKRange{Min: one}).Validate(); err == nil || !strings.Contains(err.Error(), "not resolved") {
		t.Errorf("unresolved cast accepted: %v", err)
	}
	if err := (&PKRange{Cast: PKCastSigned}).Validate(); err == nil || !strings.Contains(err.Error(), "no bounds") {
		t.Errorf("empty window accepted: %v", err)
	}
	if err := (&PKRange{Cast: PKCastSigned, Min: big.NewInt(5), Max: one}).Validate(); err == nil || !strings.Contains(err.Error(), "above upper bound") {
		t.Errorf("inverted window accepted: %v", err)
	}
	if err := (&PKRange{Cast: PKCastUnsigned, Min: big.NewInt(-1)}).Validate(); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Errorf("negative bound on unsigned cast accepted: %v", err)
	}
	if err := (&PKRange{Cast: PKCastSigned, Max: bigStr(t, "9223372036854775808")}).Validate(); err == nil {
		t.Error("bound above int64 on signed cast accepted")
	}
	if err := (&PKRange{Cast: PKCastUnsigned, Max: bigStr(t, "18446744073709551615")}).Validate(); err != nil {
		t.Errorf("full-width unsigned bound refused: %v", err)
	}
	var nilRange *PKRange
	if err := nilRange.Validate(); err != nil {
		t.Errorf("nil range must validate: %v", err)
	}
}

func TestOptions_ValidatePKRange(t *testing.T) {
	r := &PKRange{Cast: PKCastSigned, Min: big.NewInt(1)}
	if err := (Options{PKRange: r}).ValidatePKRange(); err == nil || !strings.Contains(err.Error(), "schema and table") {
		t.Errorf("range without schema/table accepted: %v", err)
	}
	if err := (Options{Schema: "s", Table: "t", PKRange: r, PKValues: "1"}).ValidatePKRange(); err == nil || !strings.Contains(err.Error(), "exact primary key lookup") {
		t.Errorf("range plus pk accepted: %v", err)
	}
	if err := (Options{Schema: "s", Table: "t", PKRange: r, PKValuesIn: []string{"1"}}).ValidatePKRange(); err == nil {
		t.Error("range plus pks accepted")
	}
	if err := (Options{Schema: "s", Table: "t", PKRange: &PKRange{Min: big.NewInt(1)}}).ValidatePKRange(); err == nil {
		t.Error("unresolved range accepted by the options check")
	}
	if err := (Options{Schema: "s", Table: "t", PKRange: r}).ValidatePKRange(); err != nil {
		t.Errorf("valid range refused: %v", err)
	}
	if err := (Options{PKValues: "1"}).ValidatePKRange(); err != nil {
		t.Errorf("no range must validate: %v", err)
	}
}

func TestPKRange_Contains(t *testing.T) {
	signed := &PKRange{Cast: PKCastSigned, Min: big.NewInt(-5), Max: big.NewInt(10)}
	for pk, want := range map[string]bool{
		"-5": true, "-6": false, "0": true, "9": true, "10": true, "11": false,
		"100": false, // lexicographically "100" < "9"; numerically it is out
		"":    false, "abc": false, "1|2": false, "9223372036854775808": false,
		// strconv accepts these; the SQL predicates do not, so neither may this.
		"+5": false, "007": false, " 9": false, "2.00": false, "1e1": false,
	} {
		if got := signed.Contains(pk); got != want {
			t.Errorf("signed[-5,10].Contains(%q) = %v, want %v", pk, got, want)
		}
	}
	unsigned := &PKRange{Cast: PKCastUnsigned, Min: big.NewInt(10)}
	for pk, want := range map[string]bool{
		"9": false, "10": true, "100": true, "18446744073709551615": true,
		"-5": false, "": false, "18446744073709551616": false, "+10": false, "010": false,
	} {
		if got := unsigned.Contains(pk); got != want {
			t.Errorf("unsigned[10,).Contains(%q) = %v, want %v", pk, got, want)
		}
	}
	if (&PKRange{Min: big.NewInt(0)}).Contains("5") {
		t.Error("an unresolved range must match nothing, not everything")
	}
	var nilRange *PKRange
	if !nilRange.Contains("anything") {
		t.Error("a nil range must not filter")
	}
}

// TestBuildQuery_pkRange pins the live-index predicate: the cast follows the
// resolved signedness, the bounds are inlined as integer literals (no bind
// args added), and the round-trip guard is present so a drifted key MySQL's
// CAST would coerce (an empty key to 0, '1|2' to 1) cannot match.
func TestBuildQuery_pkRange(t *testing.T) {
	q, args := buildQuery(Options{Schema: "s", Table: "t", Limit: 10,
		PKRange: &PKRange{Cast: PKCastUnsigned, Min: bigStr(t, "10"), Max: bigStr(t, "18446744073709551610")}})
	for _, want := range []string{
		"CAST(CAST(pk_values AS UNSIGNED) AS CHAR) = CAST(pk_values AS BINARY)",
		"CAST(pk_values AS UNSIGNED) >= 10",
		"CAST(pk_values AS UNSIGNED) <= 18446744073709551610",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("query missing %q:\n%s", want, q)
		}
	}
	// schema, table, limit: the bounds must not have become bind args.
	if len(args) != 3 {
		t.Errorf("expected 3 bind args (schema, table, limit), got %d: %v", len(args), args)
	}

	q, _ = buildQuery(Options{Schema: "s", Table: "t", Limit: 10,
		PKRange: &PKRange{Cast: PKCastSigned, Min: big.NewInt(-5)}})
	if !strings.Contains(q, "CAST(pk_values AS SIGNED) >= -5") {
		t.Errorf("signed cast missing:\n%s", q)
	}
	if !strings.Contains(q, "CAST(CAST(pk_values AS SIGNED) AS CHAR) = CAST(pk_values AS BINARY)") {
		t.Errorf("round-trip guard must use the same cast as the range:\n%s", q)
	}
	if strings.Contains(q, "<=") {
		t.Errorf("an open upper bound must emit no upper predicate:\n%s", q)
	}
	if strings.Contains(q, "UNSIGNED") {
		t.Errorf("a signed key must never be cast UNSIGNED (negatives would wrap):\n%s", q)
	}
}

// TestEngineFetch_refusesUnresolvedPKRange is the live-index mirror of the
// archive engine's belt: Engine.Fetch must refuse an unresolved range before
// building SQL. The engine has no database here on purpose: validation runs
// first, so a nil db proves nothing was queried.
func TestEngineFetch_refusesUnresolvedPKRange(t *testing.T) {
	_, err := New(nil).Fetch(context.Background(), Options{Schema: "s", Table: "t",
		PKRange: &PKRange{Min: big.NewInt(1)}})
	if err == nil || !strings.Contains(err.Error(), "not resolved") {
		t.Fatalf("unresolved range reached the live engine: %v", err)
	}
	_, err = New(nil).Fetch(context.Background(), Options{PKRange: &PKRange{Cast: PKCastSigned, Min: big.NewInt(1)}})
	if err == nil || !strings.Contains(err.Error(), "schema and table") {
		t.Fatalf("range without schema/table reached the live engine: %v", err)
	}
}

// TestPredicates_unresolvedCastNeverDefaults pins the structural belt in the
// builders themselves: with the engine check gone, an unresolved cast must
// still not become SIGNED (or BIGINT) by default. It emits a no-match clause.
func TestPredicates_unresolvedCastNeverDefaults(t *testing.T) {
	q, _ := buildQuery(Options{Schema: "s", Table: "t", Limit: 10, PKRange: &PKRange{Min: big.NewInt(1)}})
	if strings.Contains(q, "CAST(") {
		t.Errorf("an unresolved range was cast by default:\n%s", q)
	}
	if !strings.Contains(q, "1=0") {
		t.Errorf("an unresolved range must emit a no-match clause:\n%s", q)
	}
	got := (&PKRange{Min: big.NewInt(1)}).DuckDBPredicates()
	if len(got) != 1 || got[0] != "FALSE" {
		t.Errorf("DuckDB unresolved range = %v, want [FALSE]", got)
	}
}

// TestSnapshotFilters_refusesPKRange pins the --include-snapshot belt: the
// baseline reader has no pk_values to range over and must say so.
func TestSnapshotFilters_refusesPKRange(t *testing.T) {
	_, _, err := snapshotFilters(Options{Schema: "s", Table: "t", PKRange: &PKRange{Cast: PKCastSigned, Min: big.NewInt(1)}})
	if err == nil || !strings.Contains(err.Error(), "cannot be applied to snapshot rows") {
		t.Fatalf("snapshotFilters accepted a range: %v", err)
	}
	if _, _, err := snapshotFilters(Options{Schema: "s", Table: "t"}); err != nil {
		t.Fatalf("no range must pass: %v", err)
	}
}

func TestDuckDBPredicates_pkRange(t *testing.T) {
	got := (&PKRange{Cast: PKCastSigned, Min: big.NewInt(-5), Max: big.NewInt(9)}).DuckDBPredicates()
	want := []string{"CAST(TRY_CAST(pk_values AS BIGINT) AS VARCHAR) = pk_values", "TRY_CAST(pk_values AS BIGINT) >= -5", "TRY_CAST(pk_values AS BIGINT) <= 9"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("signed predicates = %v, want %v", got, want)
	}
	got = (&PKRange{Cast: PKCastUnsigned, Max: bigStr(t, "18446744073709551615")}).DuckDBPredicates()
	want = []string{"CAST(TRY_CAST(pk_values AS UBIGINT) AS VARCHAR) = pk_values", "TRY_CAST(pk_values AS UBIGINT) <= 18446744073709551615"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("unsigned predicates = %v, want %v", got, want)
	}
}
