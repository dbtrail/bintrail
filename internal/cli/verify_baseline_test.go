package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// writeMinimalBaseline writes one complete baseline snapshot (one table, one
// row, plus the success marker) under baseDir. It needs no live database — the
// "nothing to verify" paths return before touching the index — so it lets the
// single-baseline case be a fast unit test rather than an integration one.
func writeMinimalBaseline(t *testing.T, baseDir, db, table string, ts time.Time) {
	t.Helper()
	snapDir := filepath.Join(baseDir, strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-"))
	if err := os.MkdirAll(filepath.Join(snapDir, db), 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	bw, err := baseline.NewWriter(filepath.Join(snapDir, db, table+".parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100, Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: "CREATE TABLE `" + table + "` (`id` INT PRIMARY KEY);",
			baseline.MetaKeyBinlogFile:     "binlog.000001",
			baseline.MetaKeyBinlogPos:      "200",
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := bw.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
}

// TestRunVerifyBaselinePair_NoBaselines locks the fail-loud contract: a source
// with NO baselines at all (an empty dir — almost always a misconfigured
// --baseline-dir or a broken baseline job) must error and exit non-zero, not
// silently exit 0. A green CI gate over zero baselines is the false assurance
// this command exists to prevent. It returns before touching the index, so nil
// DBs are safe.
func TestRunVerifyBaselinePair_NoBaselines(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)

	err := runVerifyBaselinePair(cmd, nil, nil, "", t.TempDir(), duckdbutil.Tuning{})
	if err == nil {
		t.Fatalf("want a non-nil error (non-zero exit) for zero baselines, got nil; output:\n%s", out.String())
	}
	if !strings.Contains(err.Error(), "no baselines found") {
		t.Errorf("want a 'no baselines found' error, got %v", err)
	}
}

// TestRunVerifyBaselinePair_SingleBaseline locks the other half of that
// distinction: a legitimate first run with exactly one baseline has no
// predecessor, so it prints a clear message and exits 0 (not an error). This
// also proves the NoBaselines fail-loud above did not over-reach into the
// legitimate not-yet case. Returns before touching the index, so nil DBs are
// safe.
func TestRunVerifyBaselinePair_SingleBaseline(t *testing.T) {
	baseDir := t.TempDir()
	writeMinimalBaseline(t, baseDir, "mydb", "orders", time.Now().UTC().Truncate(time.Hour))

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)

	err := runVerifyBaselinePair(cmd, nil, nil, "", baseDir, duckdbutil.Tuning{})
	if err != nil {
		t.Fatalf("want nil error (exit 0) for a single baseline, got %v\noutput:\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "only one baseline") {
		t.Errorf("want an 'only one baseline' message, got %q", out.String())
	}
}
