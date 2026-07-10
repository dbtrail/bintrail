//go:build integration

package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// setupPKEscapeSeamDB seeds a fresh index database with:
//   - "files": a single-column PK ("path"), holding one row whose PK contains
//     a literal pipe and one row whose PK contains a literal backslash — the
//     two escaping rules event.EscapePKValue applies.
//   - "items": a 2-column composite PK ("order_id", "line_no"), holding one
//     row with plain-digit PK components — proving the existing composite
//     --pk '5|2' behavior is unchanged by the #957 fix.
//
// Each row's stored pk_values is exactly what the real write path
// (event.BuildPKValues) would have produced: event.EscapePKValue applied to
// the raw test value for the single-column rows, and the plain pipe-joined
// string for the composite row (neither "5" nor "2" contains characters
// EscapePKValue touches).
func setupPKEscapeSeamDB(t *testing.T) (dbName, dsn string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})

	snapTS := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTS, dbName, "files", "path", 1, "PRI", "varchar", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, dbName, "files", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTS, dbName, "items", "order_id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, dbName, "items", "line_no", 2, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, dbName, "items", "qty", 3, "", "int", "YES")

	// Row 1: single-column PK containing a literal pipe.
	pipePK := `a|b`
	rowAfter1, err := json.Marshal(map[string]any{"path": pipePK, "name": "n1"})
	if err != nil {
		t.Fatal(err)
	}
	testutil.InsertEvent(t, db, "binlog.000001", 100, 150,
		h.Add(1*time.Minute).Format("2006-01-02 15:04:05"), nil,
		dbName, "files", 1 /* INSERT */, event.EscapePKValue(pipePK), nil, nil, rowAfter1)

	// Row 2: single-column PK containing a literal backslash.
	bsPK := `C:\temp\new`
	rowAfter2, err := json.Marshal(map[string]any{"path": bsPK, "name": "n2"})
	if err != nil {
		t.Fatal(err)
	}
	testutil.InsertEvent(t, db, "binlog.000001", 200, 250,
		h.Add(2*time.Minute).Format("2006-01-02 15:04:05"), nil,
		dbName, "files", 1 /* INSERT */, event.EscapePKValue(bsPK), nil, nil, rowAfter2)

	// Row 3: composite (2-column) PK, plain digits — no literal delimiters in
	// either component.
	rowAfter3, err := json.Marshal(map[string]any{"order_id": 5, "line_no": 2, "qty": 3})
	if err != nil {
		t.Fatal(err)
	}
	testutil.InsertEvent(t, db, "binlog.000001", 300, 350,
		h.Add(3*time.Minute).Format("2006-01-02 15:04:05"), nil,
		dbName, "items", 1 /* INSERT */, "5|2", nil, nil, rowAfter3)

	return dbName, testutil.IntegrationDSN(dbName)
}

// resetQueryGlobals snapshots every query command global, restores them on
// cleanup, and sets flag-default values for a clean programmatic run — the
// query-side equivalent of resetRecoverGlobals (recover_limit_integration_test.go).
func resetQueryGlobals(t *testing.T) {
	t.Helper()
	sIndexDSN, sSchema, sTable, sPK := qIndexDSN, qSchema, qTable, qPK
	sPKs, sLimitPerPK, sEventType, sGTID := qPKs, qLimitPerPK, qEventType, qGTID
	sSince, sUntil, sChangedCol, sColumnEq := qSince, qUntil, qChangedCol, qColumnEq
	sFlag, sFormat, sLimit, sOrder := qFlag, qFormat, qLimit, qOrder
	sArchiveDir, sArchiveS3, sBintrailID, sProfile := qArchiveDir, qArchiveS3, qBintrailID, qProfile
	sNoArchive, sIncludeSnapshot, sBaseline := qNoArchive, qIncludeSnapshot, qBaseline
	t.Cleanup(func() {
		qIndexDSN, qSchema, qTable, qPK = sIndexDSN, sSchema, sTable, sPK
		qPKs, qLimitPerPK, qEventType, qGTID = sPKs, sLimitPerPK, sEventType, sGTID
		qSince, qUntil, qChangedCol, qColumnEq = sSince, sUntil, sChangedCol, sColumnEq
		qFlag, qFormat, qLimit, qOrder = sFlag, sFormat, sLimit, sOrder
		qArchiveDir, qArchiveS3, qBintrailID, qProfile = sArchiveDir, sArchiveS3, sBintrailID, sProfile
		qNoArchive, qIncludeSnapshot, qBaseline = sNoArchive, sIncludeSnapshot, sBaseline
	})

	qIndexDSN, qSchema, qTable, qPK = "", "", "", ""
	qPKs, qLimitPerPK, qEventType, qGTID = nil, 0, "", ""
	qSince, qUntil, qChangedCol, qColumnEq = "", "", "", nil
	qFlag, qFormat, qLimit, qOrder = "", "table", 100, "ASC"
	qArchiveDir, qArchiveS3, qBintrailID, qProfile = "", "", "", ""
	qNoArchive, qIncludeSnapshot, qBaseline = true, false, ""
}

func newQueryTestCmd() *cobra.Command {
	c := &cobra.Command{}
	c.SetContext(context.Background())
	AddDuckDBTuningFlags(c)
	return c
}

// pkEscapeSeamCases isolates the two escape rules event.EscapePKValue applies
// (backslash doubling, pipe escaping) plus one composite-PK regression case.
var pkEscapeSeamCases = []struct {
	name  string
	table string
	pk    string
}{
	{"pipe in single-column PK", "files", `a|b`},
	{"backslash in single-column PK", "files", `C:\temp\new`},
	{"composite PK unaffected", "items", "5|2"},
}

// TestPKEscapeSeam_Query pins #957: --pk on a single-column PK containing a
// literal pipe or backslash must match the escaped at-rest pk_values a real
// capture would have stored (event.BuildPKValues), instead of silently
// returning zero rows. The composite-PK case proves the fix does not touch
// today's "--pk '5|2'" composite syntax, whose own pipe is the user-typed
// delimiter between PK components.
func TestPKEscapeSeam_Query(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	dbName, dsn := setupPKEscapeSeamDB(t)

	for _, tc := range pkEscapeSeamCases {
		t.Run(tc.name, func(t *testing.T) {
			resetQueryGlobals(t)
			qIndexDSN, qSchema, qTable, qPK = dsn, dbName, tc.table, tc.pk
			qFormat = "json"

			var runErr error
			out := captureStdout(t, func() {
				runErr = runQuery(newQueryTestCmd(), nil)
			})
			if runErr != nil {
				t.Fatalf("runQuery: %v", runErr)
			}

			var rows []struct {
				PKValues string `json:"pk_values"`
			}
			if err := json.Unmarshal([]byte(out), &rows); err != nil {
				t.Fatalf("unmarshal JSON output %q: %v", out, err)
			}
			if len(rows) != 1 {
				t.Fatalf("--pk %q: expected 1 row, got %d (output: %s)", tc.pk, len(rows), out)
			}
		})
	}
}

// TestPKEscapeSeam_Recover is the recover-side sibling of
// TestPKEscapeSeam_Query: the same --pk values must produce a non-empty
// reversal script instead of a silent empty one (the "indistinguishable from
// row never changed" failure mode #957 describes).
//
// Asserts on the `statements` count from --format json rather than raw
// output non-emptiness: GenerateSQLFromRows always writes a
// "-- No events matched ..." comment for a zero-row match (recovery.go), so a
// bare "output not empty" check would pass even with zero events reversed —
// silently defeating the regression test. --format json's `statements` field
// is the actual reversed-event count.
func TestPKEscapeSeam_Recover(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	dbName, dsn := setupPKEscapeSeamDB(t)

	for _, tc := range pkEscapeSeamCases {
		t.Run(tc.name, func(t *testing.T) {
			resetRecoverGlobals(t)
			rIndexDSN, rSchema, rTable, rPK = dsn, dbName, tc.table, tc.pk
			rDryRun, rFormat = true, "json"

			var runErr error
			out := captureStdout(t, func() {
				runErr = runRecover(newRecoverTestCmd(), nil)
			})
			if runErr != nil {
				t.Fatalf("runRecover: %v", runErr)
			}

			var payload struct {
				Statements int    `json:"statements"`
				SQL        string `json:"sql"`
			}
			if err := json.Unmarshal([]byte(out), &payload); err != nil {
				t.Fatalf("unmarshal JSON output %q: %v", out, err)
			}
			if payload.Statements != 1 {
				t.Fatalf("--pk %q: expected 1 reversal statement, got %d (sql: %q)", tc.pk, payload.Statements, payload.SQL)
			}
			if strings.TrimSpace(payload.SQL) == "" {
				t.Fatalf("--pk %q: expected non-empty reversal SQL", tc.pk)
			}
		})
	}
}
