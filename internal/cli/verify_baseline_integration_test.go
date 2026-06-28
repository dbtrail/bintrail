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
