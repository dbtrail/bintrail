//go:build integration

package parquetquery

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// ─── #1440: the live and archive engines must agree on a pk range ───────────
//
// pk_values is VARCHAR(512): a bare comparison says "9" > "10". The seeded
// keys are chosen to expose that ordering (9, 10, 100), signedness (-5 on a
// signed key), and 64-bit width (a signed key near 2^63-1, an unsigned key
// above 2^63, where a signed cast overflows). Each case is fetched from the
// live index (query.Engine) and from a Parquet archive of the SAME rows
// (archive.ArchivePartition, the rotate writer) and both must equal an
// oracle computed in Go by numeric comparison.

// Besides the integer keys, each table carries DRIFTED keys: rows captured
// before the key changed shape (composite "1|2", text "abc"/"12abc", an
// overlong digit run, a negative under a now-unsigned column, and the
// spellings go-mysql stores for a DECIMAL or FLOAT key, "2.00"/"1.50", plus
// "1e1", "007", "+5") and the empty key of a #318 drift row. MySQL's CAST
// coerces every one of them to some integer and DuckDB's TRY_CAST rounds
// "2.00" to 2 and accepts "007"/"+5"; both engines must exclude them, or
// MergeResults keeps whichever side admitted the row and recover reverses
// a row nobody named.
var (
	pkRangeSignedKeys   = []string{"-9223372036854775808", "-5", "9", "10", "100", "9223372036854775800", "9223372036854775807", "", "abc", "1|2", "12abc", "99999999999999999999", "2.00", "1.50", "1e1", "007", "+5"}
	pkRangeUnsignedKeys = []string{"9", "10", "100", "18446744073709551610", "18446744073709551615", "", "-5", "abc", "1|2", "2.00", "1e1", "007", "+5"}
	// A string-keyed row of another table in the SAME hour file. Neither
	// engine may trip on it. Note what this row does and does not prove: a
	// TRY_CAST -> CAST mutation is killed by the EMPTY drift key of the
	// queried table, not by this row (DuckDB applied the table filter before
	// the cast when tried), so this row pins that the predicate stays total
	// over a foreign string key, not that CAST would have failed on it.
	pkRangeTextKeys = []string{"sku-abc"}
)

// seedPKRangeIndex creates two integer-keyed source tables in the test
// database, snapshots them the way `bintrail snapshot` does (so the resolver
// sees real information_schema types, "bigint" / "bigint unsigned"), inserts
// one event per key into binlog_events, and archives the partition to a
// Parquet file under the Hive layout. It returns the index and the archive
// base directory (the parquetquery source).
func seedPKRangeIndex(t *testing.T) (db *sql.DB, dbName, archiveBase string) {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	db, dbName = testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	testutil.MustExec(t, db, `CREATE TABLE t_signed (id BIGINT NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB`)
	testutil.MustExec(t, db, `CREATE TABLE t_unsigned (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB`)
	testutil.MustExec(t, db, `CREATE TABLE t_composite (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (a, b)) ENGINE=InnoDB`)
	testutil.MustExec(t, db, `CREATE TABLE t_text (sku VARCHAR(32) NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB`)
	if _, err := metadata.TakeSnapshot(db, db, []string{dbName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	pos := uint64(100)
	insert := func(table string, keys []string) {
		for i, k := range keys {
			// The empty key stands in for a #318 drift row: present in both
			// stores, and excluded by both engines from every range.
			testutil.InsertEvent(t, db, "bin.000001", pos, pos+50,
				fmt.Sprintf("2026-06-01 12:00:%02d", i), nil,
				dbName, table, 1, k, nil, nil, []byte(`{"id":0}`))
			pos += 100
		}
	}
	insert("t_signed", pkRangeSignedKeys)
	insert("t_unsigned", pkRangeUnsignedKeys)
	insert("t_text", pkRangeTextKeys)

	archiveBase = filepath.Join(t.TempDir(), "bintrail_id=pkrange-1440")
	hourDir := filepath.Join(archiveBase, "event_date=2026-06-01", "event_hour=12")
	if err := os.MkdirAll(hourDir, 0o755); err != nil {
		t.Fatal(err)
	}
	stats, err := archive.ArchivePartition(context.Background(), db, dbName, "p_future",
		filepath.Join(hourDir, "events.parquet"), "none")
	if err != nil {
		t.Fatalf("ArchivePartition: %v", err)
	}
	if want := int64(len(pkRangeSignedKeys) + len(pkRangeUnsignedKeys) + len(pkRangeTextKeys)); stats.Rows != want {
		t.Fatalf("archived %d rows, want %d", stats.Rows, want)
	}
	return db, dbName, archiveBase
}

// resolvedRange builds a range the way every surface does: parsed bounds,
// then ResolveCast against the table's snapshot entry.
func resolvedRange(t *testing.T, db *sql.DB, dbName, table, lo, hi string) *query.PKRange {
	t.Helper()
	var loB, hiB *big.Int
	var err error
	if lo != "" {
		if loB, err = query.ParsePKBound(lo); err != nil {
			t.Fatal(err)
		}
	}
	if hi != "" {
		if hiB, err = query.ParsePKBound(hi); err != nil {
			t.Fatal(err)
		}
	}
	r, err := query.NewPKRange(loB, hiB)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	tm, err := resolver.Resolve(dbName, table)
	if err != nil {
		t.Fatalf("Resolve %s: %v", table, err)
	}
	if err := r.ResolveCast(tm); err != nil {
		t.Fatalf("ResolveCast %s [%s,%s]: %v", table, lo, hi, err)
	}
	return r
}

// numericOracle is the independent expectation: the seeded keys that parse
// as an integer of the cast's width AND render back to the same text
// (strconv, not the product's code), then fall in [lo, hi] by big-integer
// comparison. The render-back is what keeps "+5" and "007" out: strconv
// accepts them, no engine may.
func numericOracle(keys []string, cast query.PKCast, lo, hi string) []string {
	var out []string
	for _, k := range keys {
		v := new(big.Int)
		if cast == query.PKCastUnsigned {
			u, err := strconv.ParseUint(k, 10, 64)
			if err != nil || strconv.FormatUint(u, 10) != k {
				continue
			}
			v.SetUint64(u)
		} else {
			i, err := strconv.ParseInt(k, 10, 64)
			if err != nil || strconv.FormatInt(i, 10) != k {
				continue
			}
			v.SetInt64(i)
		}
		if lo != "" {
			l, _ := new(big.Int).SetString(lo, 10)
			if v.Cmp(l) < 0 {
				continue
			}
		}
		if hi != "" {
			h, _ := new(big.Int).SetString(hi, 10)
			if v.Cmp(h) > 0 {
				continue
			}
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func keysOf(rows []query.ResultRow) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.PKValues)
	}
	sort.Strings(out)
	return out
}

func TestIntegrationPKRange_liveAndArchiveAgree(t *testing.T) {
	db, dbName, archiveBase := seedPKRangeIndex(t)
	ctx := context.Background()
	engine := query.New(db)

	cases := []struct {
		table  string
		keys   []string
		lo, hi string
		cast   query.PKCast
	}{
		// min alone: lexicographically "9" >= "10" is TRUE, so a string
		// comparison wrongly keeps 9 and this case goes red without the cast.
		{"t_signed", pkRangeSignedKeys, "10", "", query.PKCastSigned},
		{"t_unsigned", pkRangeUnsignedKeys, "10", "", query.PKCastUnsigned},
		// max alone: "100" must be out, the 64-bit key must be out.
		{"t_signed", pkRangeSignedKeys, "", "10", query.PKCastSigned},
		{"t_unsigned", pkRangeUnsignedKeys, "", "10", query.PKCastUnsigned},
		// both bounds.
		{"t_signed", pkRangeSignedKeys, "9", "100", query.PKCastSigned},
		{"t_unsigned", pkRangeUnsignedKeys, "9", "100", query.PKCastUnsigned},
		// negatives on a signed key: an UNSIGNED cast would wrap -5 to the
		// top of the range and exclude it.
		{"t_signed", pkRangeSignedKeys, "-5", "9", query.PKCastSigned},
		{"t_signed", pkRangeSignedKeys, "", "-1", query.PKCastSigned},
		// 64-bit width: the top signed keys, and unsigned keys above 2^63
		// that a signed cast cannot hold, up to the exact limits.
		{"t_signed", pkRangeSignedKeys, "9223372036854775000", "", query.PKCastSigned},
		{"t_signed", pkRangeSignedKeys, "9223372036854775807", "9223372036854775807", query.PKCastSigned},
		{"t_signed", pkRangeSignedKeys, "-9223372036854775808", "-9223372036854775808", query.PKCastSigned},
		{"t_unsigned", pkRangeUnsignedKeys, "9223372036854775808", "", query.PKCastUnsigned},
		{"t_unsigned", pkRangeUnsignedKeys, "18446744073709551610", "18446744073709551615", query.PKCastUnsigned},
		{"t_unsigned", pkRangeUnsignedKeys, "18446744073709551615", "18446744073709551615", query.PKCastUnsigned},
		// Ranges that include the values the casts turn drifted keys into:
		// 0 ('', 'abc'), 1 ('1|2'), 2 ('2.00', '1.50' rounded by DuckDB), 5
		// ('+5'), 7 ('007'), 10 ('1e1' in DuckDB), 12 ('12abc'), -1 (MySQL's
		// CAST AS SIGNED of '99999999999999999999') and the wrapped top ('-5'
		// AS UNSIGNED). None may leak in.
		{"t_signed", pkRangeSignedKeys, "0", "0", query.PKCastSigned},
		{"t_signed", pkRangeSignedKeys, "0", "12", query.PKCastSigned},
		{"t_signed", pkRangeSignedKeys, "2", "2", query.PKCastSigned},
		{"t_signed", pkRangeSignedKeys, "-1", "-1", query.PKCastSigned},
		{"t_unsigned", pkRangeUnsignedKeys, "0", "9", query.PKCastUnsigned},
		{"t_unsigned", pkRangeUnsignedKeys, "1", "9", query.PKCastUnsigned},
		{"t_unsigned", pkRangeUnsignedKeys, "2", "9", query.PKCastUnsigned},
		{"t_unsigned", pkRangeUnsignedKeys, "18446744073709551611", "18446744073709551615", query.PKCastUnsigned},
	}
	for _, tc := range cases {
		name := fmt.Sprintf("%s[%s,%s]", tc.table, tc.lo, tc.hi)
		t.Run(name, func(t *testing.T) {
			r := resolvedRange(t, db, dbName, tc.table, tc.lo, tc.hi)
			if r.Cast != tc.cast {
				t.Fatalf("resolved cast %d, want %d (the snapshot's column_type decides it)", r.Cast, tc.cast)
			}
			opts := query.Options{Schema: dbName, Table: tc.table, PKRange: r, Limit: 100}

			live, err := engine.Fetch(ctx, opts)
			if err != nil {
				t.Fatalf("live fetch: %v", err)
			}
			arch, err := Fetch(ctx, opts, archiveBase)
			if err != nil {
				t.Fatalf("archive fetch: %v", err)
			}
			// slices.Equal, not a joined string: Join([""]) == Join(nil),
			// which would let a leaked empty drift key pass as no row.
			want := numericOracle(tc.keys, tc.cast, tc.lo, tc.hi)
			// Only a point range may have an empty oracle (those exist to
			// catch a leaked drift key); every other case must expect rows.
			if len(want) == 0 && tc.lo != tc.hi {
				t.Fatalf("oracle is empty for %s; the case proves nothing", name)
			}
			if got := keysOf(live); !slices.Equal(got, want) {
				t.Errorf("live index returned %d row(s) %q, want %d %q", len(got), got, len(want), want)
			}
			if got := keysOf(arch); !slices.Equal(got, want) {
				t.Errorf("archive returned %d row(s) %q, want %d %q", len(got), got, len(want), want)
			}
			if !slices.Equal(keysOf(live), keysOf(arch)) {
				t.Errorf("engines disagree: live %q vs archive %q", keysOf(live), keysOf(arch))
			}
		})
	}
}

// TestIntegrationPKRange_shapeRefusalsFromRealSnapshot drives ResolveCast
// with entries a real `bintrail snapshot` wrote, so the refusal text is the
// one an operator sees, and the signedness comes from the server's own
// COLUMN_TYPE rather than a hand-written fixture.
func TestIntegrationPKRange_shapeRefusalsFromRealSnapshot(t *testing.T) {
	db, dbName, _ := seedPKRangeIndex(t)
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	resolve := func(table string, lo string) error {
		tm, err := resolver.Resolve(dbName, table)
		if err != nil {
			t.Fatalf("Resolve %s: %v", table, err)
		}
		v, _ := new(big.Int).SetString(lo, 10)
		return (&query.PKRange{Min: v}).ResolveCast(tm)
	}
	if err := resolve("t_composite", "1"); err == nil || !strings.Contains(err.Error(), "this table's is (a, b)") {
		t.Errorf("composite key: %v", err)
	}
	if err := resolve("t_text", "1"); err == nil || !strings.Contains(err.Error(), "this table's is (sku varchar(32))") {
		t.Errorf("varchar key: %v", err)
	}
	if err := resolve("t_unsigned", "-1"); err == nil || !strings.Contains(err.Error(), "is negative, but the primary key column is unsigned (id bigint unsigned)") {
		t.Errorf("negative bound on unsigned key: %v", err)
	}
	if err := resolve("t_signed", "9223372036854775808"); err == nil || !strings.Contains(err.Error(), "above the largest signed 64-bit value") {
		t.Errorf("oversized bound on signed key: %v", err)
	}
	if err := resolve("t_signed", "-9223372036854775808"); err != nil {
		t.Errorf("smallest signed bound refused: %v", err)
	}
}
