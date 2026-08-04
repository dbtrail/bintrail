package query

import (
	"context"
	"errors"
	"strings"
	"testing"
)

const testDigest = "3f2a1b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708"

// TestBuildQuery_queryHashPredicate pins the live-MySQL half of the
// statement-digest filter: the predicate is emitted, bound (never
// interpolated), and lowercased so it agrees with the case-SENSITIVE archive
// engine on the same input.
func TestBuildQuery_queryHashPredicate(t *testing.T) {
	q, args := buildQuery(Options{QueryHash: strings.ToUpper(testDigest)})

	if !strings.Contains(q, "query_hash = ?") {
		t.Fatalf("predicate missing from SQL:\n%s", q)
	}
	if len(args) != 1 {
		t.Fatalf("args = %v, want exactly the digest", args)
	}
	if args[0] != testDigest {
		t.Errorf("bound arg = %v, want the lowercased digest %q", args[0], testDigest)
	}
}

// TestBuildQuery_queryHashInsideKeysSubquery is the one placement that is not
// obvious from reading the predicate: buildQuery selects narrow keys in an
// inner subquery and JOINs binlog_events back for the wide columns (#1038). A
// filter that landed only on the outer JOIN would still return the right rows
// for a plain query but would let the per-PK window see — and cap against —
// events the filter should have removed.
func TestBuildQuery_queryHashInsideKeysSubquery(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"plain", Options{QueryHash: testDigest}},
		{"limit-per-pk", Options{QueryHash: testDigest, PKValues: "1", LimitPerPK: 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := buildQuery(tc.opts)
			hashAt := strings.Index(q, "query_hash = ?")
			joinAt := strings.Index(q, "JOIN (")
			if hashAt < 0 || joinAt < 0 {
				t.Fatalf("unexpected query shape:\n%s", q)
			}
			if hashAt < joinAt {
				t.Errorf("filter sits on the outer SELECT, not in the keys subquery:\n%s", q)
			}
		})
	}
}

// TestValidateStatementFilter_refusesUnderEveryRedactionShape walks all three
// ways a policy becomes active. They are enumerated deliberately: applyRedaction
// blanks the digest under ANY of them, so a validator that only checked
// ProfileActive would leave the deny/redact callers filtering on a column their
// rows come back without.
func TestValidateStatementFilter_refusesUnderEveryRedactionShape(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    Options
		wantErr bool
	}{
		{"no policy", Options{QueryHash: testDigest}, false},
		{"named profile", Options{QueryHash: testDigest, ProfileActive: true}, true},
		{"redact rules only", Options{QueryHash: testDigest, RedactColumns: []SchemaTableColumn{{Schema: "d", Table: "t", Column: "c"}}}, true},
		{"deny rules only", Options{QueryHash: testDigest, DenyTables: []SchemaTable{{Schema: "d", Table: "t"}}}, true},
		{"policy without the filter", Options{ProfileActive: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.ValidateStatementFilter()
			if tc.wantErr && !errors.Is(err, ErrQueryHashUnderProfile) {
				t.Fatalf("err = %v, want ErrQueryHashUnderProfile", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
		})
	}
}

// TestFetch_refusesQueryHashUnderProfileBeforeTouchingTheDB proves the refusal
// is on the engine path and not merely available to callers who remember it.
// The nil *sql.DB is the assertion: reaching the query would panic, so a pass
// means the check fired first.
func TestFetch_refusesQueryHashUnderProfileBeforeTouchingTheDB(t *testing.T) {
	e := New(nil)
	_, err := e.Fetch(context.Background(), Options{QueryHash: testDigest, ProfileActive: true})
	if !errors.Is(err, ErrQueryHashUnderProfile) {
		t.Fatalf("err = %v, want ErrQueryHashUnderProfile", err)
	}
}

// TestFetchMergedOptions_validateRefusesQueryHashUnderProfile pins the check at
// the ENTRY point rather than only inside Engine.Fetch. A window fully covered
// by archives skips the MySQL fetch (QueryPlan.SkipMySQL), and the archive
// engine builds its own predicate with no policy check of its own — so
// "Engine.Fetch validates" is not by itself a guarantee about which tiers ran.
//
// validate() is called directly, not through FetchMerged: with a nil engine
// FetchMerged reaches Engine.Fetch and returns the same error anyway, so the
// public path cannot tell the two checks apart.
func TestFetchMergedOptions_validateRefusesQueryHashUnderProfile(t *testing.T) {
	o := FetchMergedOptions{
		NoArchive: true,
		AllowGaps: true,
		Opts:      Options{QueryHash: testDigest, ProfileActive: true},
	}
	if err := o.validate(); !errors.Is(err, ErrQueryHashUnderProfile) {
		t.Fatalf("err = %v, want ErrQueryHashUnderProfile", err)
	}
	o.Opts.ProfileActive = false
	if err := o.validate(); err != nil {
		t.Fatalf("err = %v, want the same options without a policy to validate cleanly", err)
	}
}

// TestNormalizeQueryHash pins the shape check. Its value is entirely in the
// failure cases: a digest that is silently wrong matches no row on any engine,
// which is indistinguishable from a correct filter over a statement that
// touched nothing — a false negative on a forensic question.
func TestNormalizeQueryHash(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"empty is no filter", "", "", false},
		{"canonical", testDigest, testDigest, false},
		{"uppercase is canonicalised", strings.ToUpper(testDigest), testDigest, false},
		{"surrounding space", "  " + testDigest + "\n", testDigest, false},
		{"truncated", testDigest[:63], "", true},
		{"the statement text instead of its digest", "UPDATE mydb.orders SET status = 'shipped' WHERE id = 1", "", true},
		{"non-hex of the right length", strings.Repeat("z", 64), "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeQueryHash(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("got %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestShouldSkipSnapshot_queryHash: a baseline row is a materialised row image,
// not a statement's effect. Letting one survive a digest-scoped query would
// claim provenance it does not have.
func TestShouldSkipSnapshot_queryHash(t *testing.T) {
	reason, skip := shouldSkipSnapshot(Options{QueryHash: testDigest})
	if !skip {
		t.Fatal("snapshot source not skipped under a statement-digest filter")
	}
	if !strings.Contains(reason, "query-hash") {
		t.Errorf("reason = %q, want it to name the filter", reason)
	}
}
