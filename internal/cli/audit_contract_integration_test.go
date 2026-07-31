//go:build integration

package cli

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// resetReconstructGlobals does the same for the reconstruct command.
func resetReconstructGlobals(t *testing.T) {
	t.Helper()
	sIndexDSN, sSchema, sTable, sPK, sPKCols := recIndexDSN, recSchema, recTable, recPK, recPKColumns
	sAt, sBaselineDir, sBaselineS3, sBaselineOnly := recAt, recBaselineDir, recBaselineS3, recBaselineOnly
	sHistory, sSQL, sFormat, sNoArchive, sAllowGaps := recHistory, recSQL, recFormat, recNoArchive, recAllowGaps
	sOutFormat, sOutDir, sTables := recOutputFormat, recOutputDir, recTables
	t.Cleanup(func() {
		recIndexDSN, recSchema, recTable, recPK, recPKColumns = sIndexDSN, sSchema, sTable, sPK, sPKCols
		recAt, recBaselineDir, recBaselineS3, recBaselineOnly = sAt, sBaselineDir, sBaselineS3, sBaselineOnly
		recHistory, recSQL, recFormat, recNoArchive, recAllowGaps = sHistory, sSQL, sFormat, sNoArchive, sAllowGaps
		recOutputFormat, recOutputDir, recTables = sOutFormat, sOutDir, sTables
	})
	recIndexDSN, recSchema, recTable, recPK, recPKColumns = "", "", "", "", ""
	recAt, recBaselineDir, recBaselineS3, recBaselineOnly = "", "", "", false
	recHistory, recSQL, recFormat, recNoArchive, recAllowGaps = false, "", "json", true, true
	recOutputFormat, recOutputDir, recTables = "", "", ""
}

// auditOrdersBaseline seeds an index whose `orders` table has a baseline
// snapshot plus one indexed UPDATE after it — the minimum fixture the
// reconstruct and verify --explain contract cases need. It returns the index
// handle, the schema name, its DSN and the baseline directory.
func auditOrdersBaseline(t *testing.T, twoBaselines bool) (db *sql.DB, dbName, dsn, baseDir string, snapTime time.Time) {
	t.Helper()
	db, dbName = testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	for _, c := range []struct {
		name, key, dt string
		ord           int
	}{{"id", "PRI", "int", 1}, {"status", "", "varchar", 2}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, ?, ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			prevTS.Format("2006-01-02 15:04:05"), dbName, c.name, c.ord, c.key, c.dt, c.dt)
	}

	baseDir = t.TempDir()
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeCLIBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "b"}}, 200)
	if twoBaselines {
		// The second baseline disagrees with what the deltas replay to, so
		// verify reports a mismatch and --explain has something to drill into.
		writeCLIBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "shipped"}}, 300)
	}

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300,
		prevTS.Add(30*time.Minute).Format("2006-01-02 15:04:05"), nil,
		dbName, "orders", 2 /* UPDATE */, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"cancelled"}`))

	return db, dbName, testutil.IntegrationDSN(dbName), baseDir, prevTS
}

// TestIntegrationAuditContract_CLI is the CLI's #945 audit contract: every
// command that reads historical row images or emits a reversal script must
// record it on the ext.AuditSink seam.
//
// It is behavioural rather than source-level: each case runs the real command
// entry point against the integration MySQL with a recording sink installed,
// so an emission that is deleted — or quietly moved onto a branch the command
// never takes — fails here. CI runs the integration matrix on every pull
// request as a required check, so this gates merges.
//
// No t.Parallel(): the commands read package-level flag globals AND ext's
// sink is process-wide (audittest.Install).
func TestIntegrationAuditContract_CLI(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	rec := audittest.Install(t)

	cases := []struct {
		name   string
		action string
		table  string
		// wantMode, when set, pins Detail["mode"] — the per-mode contract the
		// pair-level CheckCoverage cannot see (#1123: --baseline-only was an
		// unaudited fifth mode behind an already-covered pair).
		wantMode string
		call     func(t *testing.T)
	}{
		{
			name:   "query",
			action: "query.run",
			table:  "orders",
			call: func(t *testing.T) {
				dbName, dsn := seedRecoverUpdates(t)
				resetQueryGlobals(t)
				qIndexDSN, qSchema, qTable = dsn, dbName, "orders"
				if err := runQuery(newQueryTestCmd(), nil); err != nil {
					t.Fatalf("runQuery: %v", err)
				}
			},
		},
		{
			name:   "recover",
			action: "recover.generate",
			table:  "orders",
			call: func(t *testing.T) {
				dbName, dsn := seedRecoverUpdates(t)
				resetRecoverGlobals(t)
				rIndexDSN, rSchema, rTable = dsn, dbName, "orders"
				rOutput = filepath.Join(t.TempDir(), "undo.sql")
				if err := runRecover(newRecoverTestCmd(), nil); err != nil {
					t.Fatalf("runRecover: %v", err)
				}
			},
		},
		{
			name:   "recover-cascade",
			action: "recover.cascade",
			table:  "parent",
			call: func(t *testing.T) {
				_, dbName, dsn := seedCascadeIndex(t)
				cleanCascadeFlags(dsn, dbName, filepath.Join(t.TempDir(), "cascade.sql"))
				if err := runCascadeCmd(t); err != nil {
					t.Fatalf("runRecoverCascade: %v", err)
				}
			},
		},
		{
			name:     "reconstruct",
			action:   "reconstruct.run",
			table:    "orders",
			wantMode: "row",
			call: func(t *testing.T) {
				_, dbName, dsn, baseDir, snapTime := auditOrdersBaseline(t, false)
				resetReconstructGlobals(t)
				recIndexDSN, recSchema, recTable, recPK = dsn, dbName, "orders", "2"
				// The fixture's schema_snapshots rows predate the event, so the
				// resolver cannot infer the PK; name it explicitly (the same
				// escape hatch the flag exists for).
				recPKColumns = "id"
				recBaselineDir = baseDir
				recAt = snapTime.Add(time.Hour).Format("2006-01-02 15:04:05")
				if err := runReconstruct(newQueryTestCmd(), nil); err != nil {
					t.Fatalf("runReconstruct: %v", err)
				}
			},
		},
		{
			// The fifth reconstruct mode: --baseline-only prints the raw
			// baseline row (no deltas, no index connection) and used to return
			// before the emission (#1123).
			name:     "reconstruct --baseline-only",
			action:   "reconstruct.run",
			table:    "orders",
			wantMode: "baseline-only",
			call: func(t *testing.T) {
				_, dbName, _, baseDir, _ := auditOrdersBaseline(t, false)
				resetReconstructGlobals(t)
				recSchema, recTable, recPK, recPKColumns = dbName, "orders", "1", "id"
				recBaselineDir, recBaselineOnly = baseDir, true
				if err := runReconstruct(newQueryTestCmd(), nil); err != nil {
					t.Fatalf("runReconstruct --baseline-only: %v", err)
				}
			},
		},
		{
			name:   "verify --explain",
			action: "verify.explain",
			table:  "orders",
			call: func(t *testing.T) {
				db, dbName, _, baseDir, _ := auditOrdersBaseline(t, true)
				resolver, err := metadata.NewResolver(db, 1)
				if err != nil {
					t.Fatalf("NewResolver: %v", err)
				}
				vfyNoArchive, vfyExplain = true, true
				t.Cleanup(func() { vfyNoArchive, vfyExplain = false, false })
				c := &cobra.Command{}
				c.SetContext(context.Background())
				c.SetOut(os.Stdout)
				// A mismatch is the point: --explain only drills down (and only
				// emits) when the fingerprints disagree, and the command then
				// exits non-zero by contract.
				if err := runVerifyBaselinePair(c, db, resolver, dbName, baseDir, duckdbutil.Tuning{}, ""); err == nil {
					t.Fatal("expected a mismatch so --explain drills down")
				}
			},
		},
	}

	var observed []audittest.Pair
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec.Reset()
			tc.call(t)
			evs := rec.Events()
			if len(evs) != 1 {
				t.Fatalf("recorded %d audit events, want exactly 1: %+v", len(evs), evs)
			}
			ev := evs[0]
			if ev.Surface != "cli" || ev.Action != tc.action {
				t.Errorf("event = %s/%s, want cli/%s", ev.Surface, ev.Action, tc.action)
			}
			if ev.Table != tc.table {
				t.Errorf("table = %q, want %q", ev.Table, tc.table)
			}
			if tc.wantMode != "" && ev.Detail["mode"] != tc.wantMode {
				t.Errorf("detail[mode] = %q, want %q", ev.Detail["mode"], tc.wantMode)
			}
			// A locally invoked command has no authenticated caller, so the
			// actor is the process identity — "os:<user>" per ext.ProcessActor.
			if ev.Actor == "" {
				t.Error("actor must not be empty")
			}
			if ev.Time.IsZero() {
				t.Error("event Time not stamped")
			}
			observed = append(observed, audittest.Pair{Surface: ev.Surface, Action: ev.Action})
		})
	}

	audittest.CheckCoverage(t, audittest.OwnerCLI, observed)
}
