//go:build integration

package parquetquery

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
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

var (
	pkRangeSignedKeys   = []string{"-5", "9", "10", "100", "9223372036854775800", ""}
	pkRangeUnsignedKeys = []string{"9", "10", "100", "18446744073709551610", ""}
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
	if want := int64(len(pkRangeSignedKeys) + len(pkRangeUnsignedKeys)); stats.Rows != want {
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

// numericOracle is the independent expectation: the seeded keys that fall in
// [lo, hi] by big-integer comparison. The empty key is never in.
func numericOracle(keys []string, lo, hi string) []string {
	var out []string
	for _, k := range keys {
		if k == "" {
			continue
		}
		v, _ := new(big.Int).SetString(k, 10)
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
		// 64-bit width: the top signed key, and an unsigned key above 2^63
		// that a signed cast cannot hold.
		{"t_signed", pkRangeSignedKeys, "9223372036854775000", "", query.PKCastSigned},
		{"t_unsigned", pkRangeUnsignedKeys, "9223372036854775808", "", query.PKCastUnsigned},
		{"t_unsigned", pkRangeUnsignedKeys, "18446744073709551610", "18446744073709551615", query.PKCastUnsigned},
		// A range that includes 0: the empty drift key must not leak in as 0.
		{"t_signed", pkRangeSignedKeys, "0", "0", query.PKCastSigned},
		{"t_unsigned", pkRangeUnsignedKeys, "0", "9", query.PKCastUnsigned},
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
			want := numericOracle(tc.keys, tc.lo, tc.hi)
			if got := keysOf(live); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("live index returned %v, want %v", got, want)
			}
			if got := keysOf(arch); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("archive returned %v, want %v", got, want)
			}
			if strings.Join(keysOf(live), ",") != strings.Join(keysOf(arch), ",") {
				t.Errorf("engines disagree: live %v vs archive %v", keysOf(live), keysOf(arch))
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
