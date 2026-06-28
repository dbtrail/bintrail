//go:build integration

package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func writeCLIBaseline(t *testing.T, baseDir string, ts time.Time, db, table, createSQL string,
	cols []baseline.Column, rows [][]string, anchorPos int64) {
	t.Helper()
	snapDir := filepath.Join(baseDir, strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-"))
	if err := os.MkdirAll(filepath.Join(snapDir, db), 0o755); err != nil {
		t.Fatal(err)
	}
	bw, err := baseline.NewWriter(filepath.Join(snapDir, db, table+".parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100, Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: createSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000001",
			baseline.MetaKeyBinlogPos:      strconv.FormatInt(anchorPos, 10),
		}})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := bw.WriteRow(r, make([]bool, len(r))); err != nil {
			t.Fatal(err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
}

// TestRunVerifyBaselinePair_EndToEnd drives the CLI baseline-anchored mode: two
// baselines + events between them → the report shows a match and the run exits 0.
func TestRunVerifyBaselinePair_EndToEnd(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt string
		ord           int
	}{{"id", "PRI", "int", 1}, {"status", "", "varchar", 2}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.dt)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeCLIBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "b"}}, 200)
	writeCLIBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "shipped"}}, 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, prevTS.Add(30*time.Minute).Format("2006-01-02 15:04:05"),
		nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"shipped"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	vfyNoArchive = true
	t.Cleanup(func() { vfyNoArchive = false })

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := runVerifyBaselinePair(cmd, db, resolver, dbName, baseDir, duckdbutil.Tuning{}); err != nil {
		t.Fatalf("runVerifyBaselinePair: %v\noutput:\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "1 match") {
		t.Errorf("expected '1 match' in report, got:\n%s", out.String())
	}
}

// TestRunVerifyBaselinePair_EndToEnd_Mismatch is the inversion that proves the
// command actually turns red on a real divergence — the whole reason it exists.
// Same setup as _EndToEnd, but the indexed UPDATE's after-image ("cancelled")
// disagrees with the new baseline's row ("shipped"), so reconstruct(prev→anchor)
// differs from the new baseline. The assertion checks the mismatch-specific
// signal ("1 mismatch"), NOT merely err != nil: printVerifyReport also errors on
// the all-inconclusive (match==0) path, so an err-only check would pass even if
// the comparison silently degraded to inconclusive instead of detecting the
// divergence. The divergence is built from a disagreeing after-image, not an
// omitted event (which would trip the coverage-gap inconclusive path — green for
// the wrong reason).
func TestRunVerifyBaselinePair_EndToEnd_Mismatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt string
		ord           int
	}{{"id", "PRI", "int", 1}, {"status", "", "varchar", 2}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.dt)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeCLIBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "b"}}, 200)
	writeCLIBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "shipped"}}, 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	// after-image "cancelled" disagrees with the new baseline's "shipped".
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, prevTS.Add(30*time.Minute).Format("2006-01-02 15:04:05"),
		nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"cancelled"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	vfyNoArchive = true
	t.Cleanup(func() { vfyNoArchive = false })

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)
	err = runVerifyBaselinePair(cmd, db, resolver, dbName, baseDir, duckdbutil.Tuning{})
	if err == nil {
		t.Fatalf("want a non-nil error (non-zero exit) on divergence, got nil\noutput:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "1 mismatch") {
		t.Errorf("want '1 mismatch' in report (divergence detected), got:\n%s", out.String())
	}
}

// TestRunVerifyBaselinePair_Explain pins the --explain CLI wiring: on a mismatch
// the drill-down prints BELOW the report, names the exact diff, AND the run still
// exits non-zero on the mismatch — a drill-down must never mask the verdict's exit
// status (this command is an automation gate).
func TestRunVerifyBaselinePair_Explain(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt string
		ord           int
	}{{"id", "PRI", "int", 1}, {"status", "", "varchar", 2}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.dt)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeCLIBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "b"}}, 200)
	writeCLIBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "shipped"}}, 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, prevTS.Add(30*time.Minute).Format("2006-01-02 15:04:05"),
		nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"cancelled"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	vfyNoArchive = true
	vfyExplain = true
	t.Cleanup(func() { vfyNoArchive = false; vfyExplain = false })

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)
	err = runVerifyBaselinePair(cmd, db, resolver, dbName, baseDir, duckdbutil.Tuning{})
	if err == nil {
		t.Fatalf("--explain must not swallow the mismatch exit status, got nil error\noutput:\n%s", out.String())
	}
	o := out.String()
	report := strings.Index(o, "1 mismatch")
	drill := strings.Index(o, "mismatch drill-down")
	if report < 0 || drill < 0 {
		t.Fatalf("want both the report ('1 mismatch') and the drill-down section, got:\n%s", o)
	}
	if drill < report {
		t.Errorf("drill-down must print BELOW the report (drill at %d, report at %d):\n%s", drill, report, o)
	}
	for _, want := range []string{"id=2", "recovery=cancelled", "baseline=shipped"} {
		if !strings.Contains(o, want) {
			t.Errorf("drill-down missing %q:\n%s", want, o)
		}
	}
}
