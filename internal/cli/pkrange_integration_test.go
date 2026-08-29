//go:build integration

package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// ─── #1440: --pk-min/--pk-max through the real query and recover commands ───

// seedPKRangeCLI: two integer-keyed tables (one unsigned, one composite)
// snapshotted the way `bintrail snapshot` does, with keys that expose string
// ordering on the unsigned one.
func seedPKRangeCLI(t *testing.T) (dsn, dbName string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	testutil.MustExec(t, db, `CREATE TABLE orders (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB`)
	testutil.MustExec(t, db, `CREATE TABLE order_lines (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (a, b)) ENGINE=InnoDB`)
	if _, err := metadata.TakeSnapshot(db, db, []string{dbName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	pos := uint64(100)
	for i, k := range []string{"9", "10", "100", "18446744073709551610"} {
		testutil.InsertEvent(t, db, "bin.000001", pos, pos+50, "2026-06-01 12:00:0"+string(rune('0'+i)), nil,
			dbName, "orders", 2, k, []byte(`["v"]`), []byte(`{"id":`+k+`,"v":1}`), []byte(`{"id":`+k+`,"v":2}`))
		pos += 100
	}
	return testutil.IntegrationDSN(dbName), dbName
}

func TestRunQuery_pkRangeEndToEnd(t *testing.T) {
	dsn, dbName := seedPKRangeCLI(t)
	saved := struct{ dsn, format, s, tbl, min, max string }{qIndexDSN, qFormat, qSchema, qTable, qPKMin, qPKMax}
	t.Cleanup(func() {
		qIndexDSN, qFormat, qSchema, qTable, qPKMin, qPKMax = saved.dsn, saved.format, saved.s, saved.tbl, saved.min, saved.max
	})
	qIndexDSN, qFormat, qSchema, qTable = dsn, "json", dbName, "orders"
	queryCmd.SetContext(context.Background())

	run := func(min, max string) ([]string, error) {
		qPKMin, qPKMax = min, max
		var runErr error
		out := captureStdout(t, func() { runErr = runQuery(queryCmd, nil) })
		if runErr != nil {
			return nil, runErr
		}
		var rows []struct {
			PKValues string `json:"pk_values"`
		}
		if err := json.Unmarshal([]byte(out), &rows); err != nil {
			t.Fatalf("output is not a JSON array: %v\n%s", err, out)
		}
		var keys []string
		for _, r := range rows {
			keys = append(keys, r.PKValues)
		}
		return keys, nil
	}

	keys, err := run("10", "")
	if err != nil {
		t.Fatalf("--pk-min 10: %v", err)
	}
	if got := strings.Join(keys, ","); got != "10,100,18446744073709551610" {
		t.Errorf("--pk-min 10 returned %s; 9 must be out and 100 in (numeric order, not string order)", got)
	}
	keys, err = run("9223372036854775808", "")
	if err != nil {
		t.Fatalf("--pk-min above 2^63 on an unsigned key: %v", err)
	}
	if got := strings.Join(keys, ","); got != "18446744073709551610" {
		t.Errorf("--pk-min 2^63 returned %s, want only the top key", got)
	}
	keys, err = run("", "9")
	if err != nil {
		t.Fatalf("--pk-max 9: %v", err)
	}
	if got := strings.Join(keys, ","); got != "9" {
		t.Errorf("--pk-max 9 returned %s, want 9", got)
	}

	// Refusals from the real snapshot, before any query runs.
	if _, err := run("-1", ""); err == nil || !strings.Contains(err.Error(), "is negative, but the primary key column is unsigned (id bigint unsigned)") {
		t.Errorf("negative bound on an unsigned key: %v", err)
	}
	qTable = "order_lines"
	if _, err := run("1", ""); err == nil || !strings.Contains(err.Error(), "range filters need a single integer primary key; this table's is (a, b)") {
		t.Errorf("composite key: %v", err)
	}
	qTable = "missing"
	if _, err := run("1", ""); err == nil || !strings.Contains(err.Error(), "--pk-min/--pk-max: table "+dbName+".missing not found in snapshot") {
		t.Errorf("table absent from the snapshot: %v", err)
	}
}

func TestRunRecover_pkRangeEndToEnd(t *testing.T) {
	dsn, dbName := seedPKRangeCLI(t)
	saved := struct {
		dsn, s, tbl, min, max string
		dry                   bool
	}{rIndexDSN, rSchema, rTable, rPKMin, rPKMax, rDryRun}
	t.Cleanup(func() {
		rIndexDSN, rSchema, rTable, rPKMin, rPKMax, rDryRun = saved.dsn, saved.s, saved.tbl, saved.min, saved.max, saved.dry
	})
	rIndexDSN, rSchema, rTable, rDryRun = dsn, dbName, "orders", true
	recoverCmd.SetContext(context.Background())

	rPKMin, rPKMax = "10", "100"
	var runErr error
	out := captureStdout(t, func() { runErr = runRecover(recoverCmd, nil) })
	if runErr != nil {
		t.Fatalf("recover --pk-min 10 --pk-max 100: %v", runErr)
	}
	for _, want := range []string{"pk=10 ", "pk=100 "} {
		if !strings.Contains(out, want) {
			t.Errorf("reversal script lacks %q:\n%s", want, out)
		}
	}
	for _, leak := range []string{"pk=9 ", "pk=18446744073709551610 "} {
		if strings.Contains(out, leak) {
			t.Errorf("reversal script reverses %q, outside the range:\n%s", leak, out)
		}
	}

	rTable = "order_lines"
	rPKMin, rPKMax = "1", ""
	captureStdout(t, func() { runErr = runRecover(recoverCmd, nil) })
	if runErr == nil || !strings.Contains(runErr.Error(), "this table's is (a, b)") {
		t.Errorf("recover on a composite key must refuse before fetching: %v", runErr)
	}
}
