//go:build integration

package cli

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunReconstruct_singleRow_compositeIntBinaryPK closes the composite-key
// half of #1160 at the level of observable command behaviour: a single-row
// `bintrail reconstruct` over a mixed (INT, BINARY(16)) primary key — the
// tenant-id + uuid shape — must resolve under BOTH spellings an operator can
// legitimately produce for the binary component, with the integer component
// passing through untouched in the same pipe-joined value:
//
//   - the FULL-width spelling (`SELECT CONCAT('0x', HEX(k))` on the source):
//     the baseline lookup hits directly, and the event fetch must re-spell the
//     binary component down to pk_values' stripped form or it fetches ZERO
//     events and silently renders baseline-era state as the state at --at;
//   - the STRIPPED spelling (copied out of binlog_events.pk_values): the event
//     fetch matches directly, and the baseline lookup must retry the binary
//     component re-padded to the storage width or it errors "no row found in
//     baseline".
//
// Driving runReconstruct (not the helpers) keeps this pinned to behaviour, so
// it survives a relocation of the re-spelling machinery between packages.
//
// The fixture premise — a composite pk_values reads "7|0x<stripped uppercase
// hex>" with the integer first — is asserted against a real server's ROW
// binlog by reconstruct.TestCompositeIntBinaryPKBaselineJoin_endToEnd; the
// event here is hand-inserted in that pinned spelling, following this
// package's fixture convention.
func TestRunReconstruct_singleRow_compositeIntBinaryPK(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// Trailing 0x00 bytes make the two spellings differ; the stripped bytes
	// are invalid UTF-8 so pk_values carries the 0x-hex form.
	const (
		kHexPadded   = "11223344556677889900AABB00000000"
		kHexStripped = "11223344556677889900AABB"
	)
	strippedRaw, err := hex.DecodeString(kHexStripped)
	if err != nil {
		t.Fatalf("bad hex: %v", err)
	}
	kB64 := base64.StdEncoding.EncodeToString(strippedRaw)

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})

	// Composite PK: tenant (INT, ordinal 1) + k (BINARY(16), ordinal 2).
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"tenant", "PRI", "int", "int", 1},
		{"k", "PRI", "binary", "binary(16)", 2},
		{"val", "", "varchar", "varchar(32)", 3},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, ?, 'testcbp', 'items', ?, ?, ?, ?, ?, 'NO', 0)`,
			h1.Format("2006-01-02 15:04:05"), c.name, c.ord, c.key, c.dt, c.colType)
	}

	// Baseline: the SAME binary key under two tenants (padded, mydumper
	// --hex-blob form). Tenant 8's row has no events — it is the cross-tenant
	// bleed detector: a lookup that dropped the integer component could
	// resolve or fold the wrong tenant's row.
	baselineDir := t.TempDir()
	parquetDir := filepath.Join(baselineDir, strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-"), "testcbp")
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "tenant", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "k", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
		{Name: "val", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(filepath.Join(parquetDir, "items.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, row := range [][]string{
		{"7", "0x" + kHexPadded, "val-base"},
		{"8", "0x" + kHexPadded, "val-other"},
	} {
		if err := w.WriteRow(row, []bool{false, false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	// One UPDATE for tenant 7 only, keyed by the stored composite spelling.
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"testcbp", "items", 2 /*UPDATE*/, "7|0x"+kHexStripped, nil,
		[]byte(fmt.Sprintf(`{"tenant":7,"k":"%s","val":"val-base"}`, kB64)),
		[]byte(fmt.Sprintf(`{"tenant":7,"k":"%s","val":"val-live"}`, kB64)))

	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })
	recIndexDSN = testutil.SnapshotDSN(dbName)
	recSchema = "testcbp"
	recTable = "items"
	recPKColumns = "tenant,k"
	recBaselineDir = baselineDir
	recBaselineS3 = ""
	recBaselineOnly = false
	recHistory = false
	recSQL = ""
	recFormat = "json"
	recNoArchive = true
	recAllowGaps = true
	recAt = h1.Add(45 * time.Minute).Format(time.RFC3339)

	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	oldStdout := os.Stdout
	t.Cleanup(func() { os.Stdout = oldStdout })
	run := func(pk string) (string, error) {
		recPK = pk
		r, wPipe, err := os.Pipe()
		if err != nil {
			t.Fatalf("pipe: %v", err)
		}
		os.Stdout = wPipe
		runErr := runReconstruct(reconstructCmd, nil)
		wPipe.Close()
		os.Stdout = oldStdout
		out, _ := io.ReadAll(r)
		return string(out), runErr
	}

	// ── Full-width spelling: the baseline hits directly; the event fetch must
	// re-spell the binary component or it sees zero events and silently emits
	// the baseline-era "val-base" — the fail-loud-to-fail-silent regression.
	out, runErr := run("7|0x" + kHexPadded)
	if runErr != nil {
		t.Fatalf("runReconstruct (full-width composite pk): %v\noutput: %s", runErr, out)
	}
	if !strings.Contains(out, "val-live") || strings.Contains(out, "val-base") {
		t.Errorf("full-width composite pk: output = %s\nwant the post-event state val-live (baseline-era "+
			"val-base means the event fetch missed pk_values' stripped spelling)", out)
	}

	// ── Stripped (pk_values) spelling: the event fetch matches directly; the
	// baseline lookup must retry the binary component re-padded to the storage
	// width, leaving the integer component alone.
	out, runErr = run("7|0x" + kHexStripped)
	if runErr != nil {
		t.Fatalf("runReconstruct (stripped composite pk) must resolve via the padded retry, got: %v\noutput: %s", runErr, out)
	}
	if !strings.Contains(out, "val-live") || strings.Contains(out, "val-base") {
		t.Errorf("stripped composite pk: output = %s\nwant the post-event state val-live", out)
	}

	// ── Cross-tenant scope: tenant 8 shares the binary bytes but has no
	// events; both lookups must keep the integer component in play, so the
	// answer is tenant 8's untouched baseline row — never tenant 7's state.
	out, runErr = run("8|0x" + kHexPadded)
	if runErr != nil {
		t.Fatalf("runReconstruct (tenant 8): %v\noutput: %s", runErr, out)
	}
	if !strings.Contains(out, "val-other") || strings.Contains(out, "val-live") {
		t.Errorf("tenant 8 composite pk: output = %s\nwant val-other (tenant 7's update must not bleed across "+
			"the integer component)", out)
	}
}
