package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The type zoo below is the point of most of this file. A reconstructed
// snapshot is produced by reading a baseline back out of Parquet (typed Go
// values) and re-rendering it into the text form baseline.Writer parses. Every
// MySQL type family takes a different route through that render → convert pair,
// and a family that does not survive it silently corrupts the refreshed
// snapshot — the exact failure a chain of refreshes would compound.
const zooCreateTableSQL = "CREATE TABLE `orders` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `qty` bigint unsigned DEFAULT NULL,\n" +
	"  `name` varchar(64) DEFAULT NULL,\n" +
	"  `price` decimal(10,2) DEFAULT NULL,\n" +
	"  `created` datetime(6) DEFAULT NULL,\n" +
	"  `day` date DEFAULT NULL,\n" +
	"  `payload` blob,\n" +
	"  `ratio` double DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB;\n"

func zooColumns(t *testing.T) []baseline.Column {
	t.Helper()
	cols, err := baseline.ParseSchemaText(zooCreateTableSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	return cols
}

// writeZooBaseline writes a source snapshot holding rows in mydumper text form,
// through the REAL baseline writer — the same code path `bintrail baseline`
// runs — so what is read back is genuinely what a snapshot on disk contains.
func writeZooBaseline(t *testing.T, rows [][]string, nulls [][]bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "orders.parquet")
	w, err := baseline.NewWriter(path, zooColumns(t), baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 10,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: zooCreateTableSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000007",
			baseline.MetaKeyBinlogPos:      "4",
		},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for i, r := range rows {
		if err := w.WriteRow(r, nulls[i]); err != nil {
			t.Fatalf("WriteRow %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close source baseline: %v", err)
	}
	return path
}

// readSnapshotRows reads a baseline Parquet back into the same value shape the
// merge emits, using the production reader (mergeBaselineImages with an empty
// change map). Comparing two files through it compares what every downstream
// consumer — reconstruct, verify, the shim — would actually see.
func readSnapshotRows(t *testing.T, path string) []map[string]any {
	t.Helper()
	var out []map[string]any
	_, err := mergeBaselineImages(context.Background(), mergeCore{
		LocalBaselinePath: path,
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           map[string]*query.ResultRow{},
	}, func(row map[string]any) error {
		out = append(out, row)
		return nil
	})
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return out
}

func zooRows() ([][]string, [][]bool) {
	return [][]string{
			{"1", "18446744073709551615", "alpha", "12.34", "2026-01-02 03:04:05", "2026-01-02", "0x0001ff", "1.5"},
			{"2", "0", "bêta ünicode", "0.01", "2026-01-02 03:04:05.123456", "1999-12-31", "0xdeadbeef", "-2.25"},
			{"3", "", "", "", "", "", "", ""},
		}, [][]bool{
			{false, false, false, false, false, false, false, false},
			{false, false, false, false, false, false, false, false},
			// Row 3 is all-NULL except the PK: NULL is its own route through the
			// renderer and must not be confused with an empty string.
			{false, true, true, true, true, true, true, true},
		}
}

// emitSnapshot runs the Parquet emit path against a source baseline and returns
// the snapshot directory plus the emitted file's path.
func emitSnapshot(t *testing.T, sourcePath string, changes map[string]*query.ResultRow, cut *query.BinlogPos, at time.Time) (snapDir, filePath string, rep *TableReport) {
	t.Helper()
	snapDir = filepath.Join(t.TempDir(), snapshotDirName(at))
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir snapshot dir: %v", err)
	}
	rep = &TableReport{Schema: "mydb", Table: "orders"}
	srcMeta, err := baseline.ReadParquetMetadata(sourcePath)
	if err != nil {
		t.Fatalf("read source metadata: %v", err)
	}
	if changes == nil {
		changes = map[string]*query.ResultRow{}
	}
	err = mergeBaselineIntoParquet(context.Background(), mergeInput{
		LocalBaselinePath: sourcePath,
		CreateTableSQL:    zooCreateTableSQL,
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		SnapshotDir:       snapDir,
		SnapshotAt:        at,
		Cut:               cut,
		SourceBaseline: baselineMeta{
			Path:     sourcePath,
			Time:     at.Add(-time.Hour),
			Metadata: srcMeta,
		},
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoParquet: %v", err)
	}
	return snapDir, filepath.Join(snapDir, "mydb", "orders.parquet"), rep
}

// TestParquetSnapshot_passthroughIsValuePreserving is the load-bearing test for
// #1169. A snapshot emitted with NO deltas must hold exactly the values its
// source held — for every type family, including NULLs.
//
// Refreshing a baseline is repeated application of this operation. Any type that
// drifts by a hair per refresh (a truncated microsecond, a re-encoded blob, an
// integer that came back as 1.0) accumulates silently over the chain, and the
// only place it would ever surface is a restore.
func TestParquetSnapshot_passthroughIsValuePreserving(t *testing.T) {
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)

	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	_, out, rep := emitSnapshot(t, src, nil, &query.BinlogPos{File: "binlog.000009", Pos: 4096}, at)

	if rep.BaselineRows != 3 {
		t.Errorf("BaselineRows = %d, want 3", rep.BaselineRows)
	}
	if rep.RowsWritten != 3 {
		t.Errorf("RowsWritten = %d, want 3", rep.RowsWritten)
	}

	want := readSnapshotRows(t, src)
	got := readSnapshotRows(t, out)
	if len(got) != len(want) {
		t.Fatalf("row count: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Errorf("row %d differs after a no-delta refresh:\n got: %#v\nwant: %#v", i, got[i], want[i])
		}
	}
}

// TestParquetSnapshot_secondRefreshIsStable folds the emitted snapshot again.
// One lossless round trip does not prove the chain is stable — a transform that
// is idempotent only after the first pass would still pass the test above.
func TestParquetSnapshot_secondRefreshIsStable(t *testing.T) {
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)

	at1 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	_, first, _ := emitSnapshot(t, src, nil, &query.BinlogPos{File: "binlog.000009", Pos: 4096}, at1)

	at2 := at1.Add(time.Hour)
	_, second, _ := emitSnapshot(t, first, nil, &query.BinlogPos{File: "binlog.000009", Pos: 8192}, at2)

	want := readSnapshotRows(t, src)
	got := readSnapshotRows(t, second)
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Errorf("row %d drifted after two refreshes:\n got: %#v\nwant: %#v", i, got[i], want[i])
		}
	}
}

// TestParquetSnapshot_appliesDeltas checks that the snapshot is a state
// materialization, not a copy: an UPDATE wins over the baseline row, a DELETE
// removes it, and an event for a PK absent from the baseline is appended.
func TestParquetSnapshot_appliesDeltas(t *testing.T) {
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)

	changes := map[string]*query.ResultRow{
		pkStrForInt(1): {
			PKValues:  pkStrForInt(1),
			EventType: event.EventUpdate,
			RowAfter: map[string]any{
				"id": float64(1), "qty": float64(7), "name": "updated", "price": "99.99",
				"created": "2026-03-03 03:03:03", "day": "2026-03-03",
				"payload": nil, "ratio": float64(3.5),
			},
		},
		pkStrForInt(2): {
			PKValues:  pkStrForInt(2),
			EventType: event.EventDelete,
		},
		pkStrForInt(9): {
			PKValues:  pkStrForInt(9),
			EventType: event.EventInsert,
			RowAfter: map[string]any{
				"id": float64(9), "qty": float64(1), "name": "new row", "price": "1.00",
				"created": "2026-04-04 04:04:04", "day": "2026-04-04",
				"payload": nil, "ratio": float64(0),
			},
		},
	}

	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	_, out, rep := emitSnapshot(t, src, changes, &query.BinlogPos{File: "binlog.000009", Pos: 4096}, at)

	if rep.UpdatesApplied != 1 || rep.DeletesSkipped != 1 || rep.InsertsEmitted != 1 {
		t.Errorf("counters = updates %d / deletes %d / inserts %d, want 1/1/1",
			rep.UpdatesApplied, rep.DeletesSkipped, rep.InsertsEmitted)
	}

	byID := map[int64]map[string]any{}
	for _, row := range readSnapshotRows(t, out) {
		id, ok := row["id"].(int32)
		if !ok {
			t.Fatalf("id column came back as %T, want int32", row["id"])
		}
		byID[int64(id)] = row
	}
	if _, present := byID[2]; present {
		t.Error("id=2 was deleted in the window but is still in the snapshot")
	}
	if got := byID[1]["name"]; got != "updated" {
		t.Errorf("id=1 name = %v, want the post-UPDATE value", got)
	}
	if _, ok := byID[9]; !ok {
		t.Error("id=9 was inserted in the window but is missing from the snapshot")
	}
	if _, ok := byID[3]; !ok {
		t.Error("id=3 was never touched and must pass through")
	}
}

// parquetKV reads a Parquet file's key-value metadata.
func parquetKV(t *testing.T, path string) map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	kv := map[string]string{}
	for _, k := range []string{
		baseline.MetaKeyBinlogFile, baseline.MetaKeyBinlogPos, baseline.MetaKeyGTIDSet,
		baseline.MetaKeyCreateTableSQL, baseline.MetaKeyContentDigest, baseline.MetaKeyRowCount,
		MetaKeySnapshotProducer, MetaKeyDerivedFrom, "bintrail.snapshot_timestamp",
	} {
		if v, ok := pf.Lookup(k); ok {
			kv[k] = v
		}
	}
	return kv
}

// TestParquetSnapshot_metadata pins the emitted footer. The anchor is the whole
// feature; the two deliberate omissions are the parts a future contributor is
// most likely to "fix" by filling them in.
func TestParquetSnapshot_metadata(t *testing.T) {
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	cut := &query.BinlogPos{File: "binlog.000009", Pos: 4096}
	_, out, _ := emitSnapshot(t, src, nil, cut, at)

	kv := parquetKV(t, out)

	if kv[baseline.MetaKeyBinlogFile] != cut.File || kv[baseline.MetaKeyBinlogPos] != "4096" {
		t.Errorf("anchor = %s:%s, want %s:%d — the emitted snapshot must be anchored at the RUN's cut, "+
			"not at the source baseline's, or the next fold re-applies or skips events",
			kv[baseline.MetaKeyBinlogFile], kv[baseline.MetaKeyBinlogPos], cut.File, cut.Pos)
	}
	if kv[baseline.MetaKeyCreateTableSQL] != zooCreateTableSQL {
		t.Error("CREATE TABLE was not propagated verbatim; a snapshot anchored on this one could not emit a schema")
	}
	if got := kv["bintrail.snapshot_timestamp"]; got != at.Format(time.RFC3339) {
		t.Errorf("snapshot_timestamp = %q, want %q", got, at.Format(time.RFC3339))
	}
	if kv[MetaKeySnapshotProducer] != SnapshotProducerReconstruct {
		t.Errorf("producer = %q, want %q", kv[MetaKeySnapshotProducer], SnapshotProducerReconstruct)
	}
	if _, ok := kv[baseline.MetaKeyGTIDSet]; ok {
		t.Error("a GTID set was stamped: binlog_events.gtid is per-event, not an accumulated executed-set, " +
			"so any value here is a guess an operator would read as authoritative")
	}
	if _, ok := kv[baseline.MetaKeyContentDigest]; ok {
		t.Error("a content digest was stamped: the digest certifies fidelity against the SOURCE, " +
			"which a reconstructed snapshot never read — verify would compare it and report a mismatch that means nothing")
	}
}

// TestParquetSnapshot_anchorCarriesOverWithoutCut covers the empty-index case:
// nothing was folded, so the source's anchor is still exactly where deltas
// resume and must be carried over rather than dropped.
func TestParquetSnapshot_anchorCarriesOverWithoutCut(t *testing.T) {
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	_, out, _ := emitSnapshot(t, src, nil, nil, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))

	kv := parquetKV(t, out)
	if kv[baseline.MetaKeyBinlogFile] != "binlog.000007" || kv[baseline.MetaKeyBinlogPos] != "4" {
		t.Errorf("anchor = %s:%s, want the source's binlog.000007:4 carried over",
			kv[baseline.MetaKeyBinlogFile], kv[baseline.MetaKeyBinlogPos])
	}
}

// TestParquetSnapshot_discoverableByFindBaseline is the acceptance shape of
// #1169: the emitted directory must be found by the same discovery every
// consumer uses, with no special-casing for how it was produced.
func TestParquetSnapshot_discoverableByFindBaseline(t *testing.T) {
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)

	root := t.TempDir()
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	snapDir := filepath.Join(root, snapshotDirName(at))
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	srcMeta, err := baseline.ReadParquetMetadata(src)
	if err != nil {
		t.Fatalf("read source metadata: %v", err)
	}
	rep := &TableReport{Schema: "mydb", Table: "orders"}
	if err := mergeBaselineIntoParquet(context.Background(), mergeInput{
		LocalBaselinePath: src,
		CreateTableSQL:    zooCreateTableSQL,
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           map[string]*query.ResultRow{},
		SnapshotDir:       snapDir,
		SnapshotAt:        at,
		Cut:               &query.BinlogPos{File: "binlog.000009", Pos: 4096},
		SourceBaseline:    baselineMeta{Path: src, Time: at.Add(-time.Hour), Metadata: srcMeta},
	}, rep); err != nil {
		t.Fatalf("mergeBaselineIntoParquet: %v", err)
	}

	// An unmarked snapshot must NOT be discoverable while it is being written.
	if err := baseline.WriteIncompleteMarker(snapDir); err != nil {
		t.Fatalf("WriteIncompleteMarker: %v", err)
	}
	if _, _, _, err := FindBaseline(context.Background(), root, "mydb", "orders", at.Add(time.Hour)); err == nil {
		t.Error("an _INCOMPLETE snapshot was discovered; a crash mid-write would publish a partial baseline")
	}

	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
	path, ts, _, err := FindBaseline(context.Background(), root, "mydb", "orders", at.Add(time.Hour))
	if err != nil {
		t.Fatalf("FindBaseline did not discover the emitted snapshot: %v", err)
	}
	if filepath.Base(path) != "orders.parquet" || !strings.Contains(path, snapshotDirName(at)) {
		t.Errorf("discovered %q, want the emitted snapshot's file", path)
	}
	if !ts.Equal(at) {
		t.Errorf("discovered snapshot time = %s, want %s", ts, at)
	}

	// And it must read back as a baseline: metadata plus the anchor the next
	// fold resumes from.
	meta, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		t.Fatalf("ReadParquetMetadata on the emitted snapshot: %v", err)
	}
	if meta.BinlogFile != "binlog.000009" || meta.BinlogPos != 4096 {
		t.Errorf("anchor read back as %s:%d", meta.BinlogFile, meta.BinlogPos)
	}
	if meta.CreateTableSQL == "" {
		t.Error("CreateTableSQL missing: a reconstruct anchored here would refuse with 're-run bintrail baseline'")
	}
}

// TestParquetSnapshot_discardsPartialFileOnFailure pins the #1162 stance for the
// new writer: a table that fails mid-merge leaves nothing behind, so the
// snapshot directory never holds an unreadable file for one table next to good
// ones.
func TestParquetSnapshot_discardsPartialFileOnFailure(t *testing.T) {
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	snapDir := filepath.Join(t.TempDir(), "snap")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// A change map naming a column the baseline does not have trips the #602
	// guard — before the writer opens. Assert the stronger property: no file.
	rep := &TableReport{Schema: "mydb", Table: "orders"}
	err := mergeBaselineIntoParquet(context.Background(), mergeInput{
		LocalBaselinePath: src,
		CreateTableSQL:    zooCreateTableSQL,
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes: map[string]*query.ResultRow{
			pkStrForInt(1): {
				PKValues:  pkStrForInt(1),
				EventType: event.EventUpdate,
				RowAfter:  map[string]any{"id": float64(1), "added_later": "x"},
			},
		},
		SnapshotDir: snapDir,
		SnapshotAt:  time.Now().UTC(),
	}, rep)
	if err == nil {
		t.Fatal("expected the #602 post-baseline column guard to refuse")
	}
	if _, statErr := os.Stat(filepath.Join(snapDir, "mydb", "orders.parquet")); statErr == nil {
		t.Error("a snapshot file was left on disk for a refused table")
	}
}

// TestCheckSchemaMatchesBaseline covers the drift between a snapshot's embedded
// CREATE TABLE and the columns it actually stores. Both directions are silent
// data bugs (NULL-fill / drop), so both must refuse.
func TestCheckSchemaMatchesBaseline(t *testing.T) {
	cols := []baseline.Column{{Name: "id"}, {Name: "name"}}
	for _, tc := range []struct {
		name    string
		file    []string
		wantErr string
	}{
		{"agree", []string{"id", "name"}, ""},
		{"agree regardless of order", []string{"name", "id"}, ""},
		{"column only in the DDL", []string{"id"}, "only in the DDL: name"},
		{"column only in the file", []string{"id", "name", "extra"}, "only in the Parquet file: extra"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkSchemaMatchesBaseline(cols, tc.file, "mydb", "orders")
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr == "":
			case err == nil:
				t.Fatalf("expected an error containing %q", tc.wantErr)
			case !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestSnapshotDirName_matchesBaselineRunFormat pins the directory naming against
// what discovery parses. The name IS the snapshot's timestamp to FindBaseline,
// so this is a compatibility contract with `bintrail baseline`, not formatting.
func TestSnapshotDirName_matchesBaselineRunFormat(t *testing.T) {
	at := time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC)
	if got, want := snapshotDirName(at), "2026-02-28T00-00-00Z"; got != want {
		t.Fatalf("snapshotDirName = %q, want %q", got, want)
	}
	// Non-UTC input must normalise, or two snapshots taken at the same instant
	// from differently-configured hosts would sort against each other wrongly.
	loc := time.FixedZone("X", -5*3600)
	if got, want := snapshotDirName(at.In(loc)), "2026-02-28T00-00-00Z"; got != want {
		t.Fatalf("snapshotDirName(non-UTC) = %q, want %q", got, want)
	}
}

// TestRenderBaselineValue covers each Go type the two provenances produce, with
// the assertion at the level that matters: the rendered text is what
// baseline.Writer's converter accepts for that column.
func TestRenderBaselineValue(t *testing.T) {
	col := func(name, typ string, unsigned bool) baseline.Column {
		return baseline.Column{Name: name, MySQLType: typ, Unsigned: unsigned}
	}
	for _, tc := range []struct {
		name string
		col  baseline.Column
		in   any
		want string
		null bool
	}{
		{"nil is NULL", col("a", "int", false), nil, "", true},
		{"string verbatim", col("a", "varchar", false), "hi", "hi", false},
		{"empty string is not NULL", col("a", "varchar", false), "", "", false},
		{"int32 from a Parquet read", col("a", "int", false), int32(42), "42", false},
		{"uint64 from an UNSIGNED column", col("a", "bigint", true), uint64(18446744073709551615), "18446744073709551615", false},
		{"float64 from JSON in an int column", col("a", "int", false), float64(1234568), "1234568", false},
		{"large float64 never uses an exponent", col("a", "bigint", false), float64(1e21), "1000000000000000000000", false},
		{"double keeps its value", col("a", "double", false), float64(-2.25), "-2.25", false},
		{"float32 is not widened into noise", col("a", "float", false), float32(0.1), "0.1", false},
		{"decimal arriving as text stays text", col("a", "decimal", false), "12.34", "12.34", false},
		{"bool becomes MySQL's 1/0", col("a", "tinyint", false), true, "1", false},
		{"datetime without a fraction", col("a", "datetime", false), time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), "2026-01-02 03:04:05", false},
		{"datetime with microseconds", col("a", "datetime", false), time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC), "2026-01-02 03:04:05.123456", false},
		{"date drops the time", col("a", "date", false), time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), "2026-01-02", false},
		{"blob becomes a hex literal", col("a", "blob", false), []byte{0x00, 0x01, 0xff}, "0x0001ff", false},
		{"text bytes stay text", col("a", "text", false), []byte("hello"), "hello", false},
		{
			// Without unconditional hex encoding this would be decoded as the
			// literal 0x1234 on the way back in and lose its real bytes.
			"blob whose bytes spell a hex literal",
			col("a", "blob", false), []byte("0x1234"), "0x307831323334", false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, isNull, err := renderBaselineValue(tc.col, tc.in)
			if err != nil {
				t.Fatalf("renderBaselineValue: %v", err)
			}
			if isNull != tc.null {
				t.Fatalf("isNull = %v, want %v", isNull, tc.null)
			}
			if got != tc.want {
				t.Fatalf("rendered %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRenderBaselineValue_datetimeNormalisesToUTC pins the timezone: the Parquet
// timestamp is microseconds since the Unix epoch and the converter parses the
// text back as UTC, so rendering in the host's local zone would shift every
// timestamp by the host's offset on a refresh (the #768 failure, re-introduced
// from the other end).
func TestRenderBaselineValue_datetimeNormalisesToUTC(t *testing.T) {
	loc := time.FixedZone("UTC-5", -5*3600)
	in := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).In(loc)
	got, _, err := renderBaselineValue(baseline.Column{MySQLType: "datetime"}, in)
	if err != nil {
		t.Fatalf("renderBaselineValue: %v", err)
	}
	if got != "2026-01-02 03:04:05" {
		t.Fatalf("rendered %q in a non-UTC zone; want the UTC instant", got)
	}
}

// TestParseSchemaText_matchesParseSchema pins the new in-memory entry point
// against the file one they now share, since a divergence would silently change
// the column list of every reconstructed snapshot.
func TestParseSchemaText_matchesParseSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mydb.orders-schema.sql")
	if err := os.WriteFile(path, []byte(zooCreateTableSQL), 0o644); err != nil {
		t.Fatalf("write schema file: %v", err)
	}
	fromFile, err := baseline.ParseSchema(path)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	fromText, err := baseline.ParseSchemaText(zooCreateTableSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	if len(fromFile) != len(fromText) {
		t.Fatalf("column counts differ: %d vs %d", len(fromFile), len(fromText))
	}
	for i := range fromFile {
		if fromFile[i].Name != fromText[i].Name ||
			fromFile[i].MySQLType != fromText[i].MySQLType ||
			fromFile[i].Unsigned != fromText[i].Unsigned {
			t.Errorf("column %d differs: %+v vs %+v", i, fromFile[i], fromText[i])
		}
	}
}

// TestReconstructTables_rejectsUnknownOutputFormat keeps a typo from silently
// producing a mydumper dump where a snapshot was asked for.
func TestReconstructTables_rejectsUnknownOutputFormat(t *testing.T) {
	_, err := ReconstructTables(context.Background(), FullTableConfig{
		IndexDSN:     "user:pass@tcp(127.0.0.1:1)/idx",
		BaselineSrc:  t.TempDir(),
		Tables:       []string{"mydb.orders"},
		OutputDir:    t.TempDir(),
		OutputFormat: "parquetish",
	})
	if err == nil || !strings.Contains(err.Error(), "unknown OutputFormat") {
		t.Fatalf("error = %v, want an unknown-OutputFormat refusal", err)
	}
}

// TestReconstructTables_refusesNonEmptySnapshotDir: two runs sharing a snapshot
// timestamp would interleave tables from different folds under one anchor and
// publish the mixture as coherent.
func TestReconstructTables_refusesNonEmptySnapshotDir(t *testing.T) {
	root := t.TempDir()
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	occupied := filepath.Join(root, snapshotDirName(at))
	if err := os.MkdirAll(filepath.Join(occupied, "mydb"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(occupied, "mydb", "orders.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := ReconstructTables(context.Background(), FullTableConfig{
		IndexDSN:     "user:pass@tcp(127.0.0.1:1)/idx",
		BaselineSrc:  t.TempDir(),
		Tables:       []string{"mydb.orders"},
		At:           at,
		OutputDir:    root,
		OutputFormat: OutputFormatParquet,
	})
	if err == nil || !strings.Contains(err.Error(), "already exists and is not empty") {
		t.Fatalf("error = %v, want a refusal to write into an occupied snapshot directory", err)
	}
}
