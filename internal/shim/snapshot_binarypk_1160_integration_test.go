//go:build integration

package shim

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestSnapshotBaseline_FullTableMerge_BinaryPK pins the second gap of #1160:
// before #1155 a BINARY-keyed table's full-table _snapshot REFUSED with
// ER_NO_PARTITION_FOR_GIVEN_VALUE (1526) because reconstruct.SupportedPKType
// rejected the type; since #1155 the gate passes and the merge is ATTEMPTED —
// inside a long-lived network daemon. Neither behaviour was pinned by a test.
// This pins the new one: a full-table `_snapshot … AS OF` over a BINARY(16)-
// keyed table returns the COMPLETE row set, including the never-touched
// baseline row that only the merge can produce.
//
// Every key carries trailing 0x00 bytes — the only shape that distinguishes a
// correct canonicalization (baseline padded bytes trimmed to match pk_values'
// stripped spelling) from an inverted one; a padding-free key passes both
// directions. The ROW-image premise (fixed BINARY(n) arrives stripped,
// pk_values stores "0x"+uppercase hex for non-UTF-8 bytes) is asserted against
// a live server by reconstruct.TestBinaryPKBaselineJoin_endToEnd
// (assertPaddingStripped); the events here are hand-inserted in that pinned
// spelling, following this file's fixture convention.
func TestSnapshotBaseline_FullTableMerge_BinaryPK(t *testing.T) {
	// 16-byte keys, all ending in 0x00, all invalid UTF-8 once stripped (so
	// pk_values holds the 0x-hex spelling, not the verbatim one).
	const (
		kUntouched = "F1E2D3C4B5A69788990011223344AB00" // baseline only — the merge's proof row
		kUpdated   = "E5D4C3B2A1F0E9D8C7B6A59483720000" // baseline + post-baseline UPDATE
		kDeleted   = "DEADBEEF000000000000000000000000" // baseline + post-baseline DELETE
		kInserted  = "FACE0000000000000000000000000000" // post-baseline INSERT only
	)
	// The stripped (ROW image / pk_values) spelling of each. Spelled out
	// rather than computed so the fixture stays inspectable.
	const (
		kUpdatedStripped  = "E5D4C3B2A1F0E9D8C7B6A5948372"
		kDeletedStripped  = "DEADBEEF"
		kInsertedStripped = "FACE"
	)
	strippedB64 := func(hexStr string) string {
		raw, err := hex.DecodeString(hexStr)
		if err != nil {
			t.Fatalf("bad hex %q: %v", hexStr, err)
		}
		return base64.StdEncoding.EncodeToString(raw)
	}

	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	// Schema snapshot with COLUMN_TYPE so the merge's fixed-binary machinery
	// sees the declared width (the modern-snapshot shape).
	snapTS := snapTime.UTC().Format("2006-01-02 15:04:05")
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, ?, 'myapp', 'bkeys', 'k', 1, 'PRI', 'binary', 'binary(16)', 'NO', 0)`, snapTS)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, ?, 'myapp', 'bkeys', 'val', 2, '', 'varchar', 'varchar(32)', 'NO', 0)`, snapTS)

	// Baseline: the padded 16-byte keys as mydumper's --hex-blob 0x… literal,
	// decoded to raw bytes by the production baseline writer.
	cols := []baseline.Column{
		{Name: "k", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
		{Name: "val", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "bkeys", cols, [][]string{
		{"0x" + kUntouched, "alice"},
		{"0x" + kUpdated, "bob"},
		{"0x" + kDeleted, "carol"},
	})

	// Post-baseline events, keyed by the stripped pk_values spelling, row
	// images carrying the stripped bytes base64-encoded (as marshalRow stores
	// the []byte go-mysql delivers).
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "bkeys", 2 /*update*/, "0x"+kUpdatedStripped, nil,
		[]byte(fmt.Sprintf(`{"k":"%s","val":"bob"}`, strippedB64(kUpdatedStripped))),
		[]byte(fmt.Sprintf(`{"k":"%s","val":"bob2"}`, strippedB64(kUpdatedStripped))))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, eventTS, nil,
		"myapp", "bkeys", 3 /*delete*/, "0x"+kDeletedStripped, nil,
		[]byte(fmt.Sprintf(`{"k":"%s","val":"carol"}`, strippedB64(kDeletedStripped))), nil)
	testutil.InsertEvent(t, db, "mysql-bin.000001", 300, 400, eventTS, nil,
		"myapp", "bkeys", 1 /*insert*/, "0x"+kInsertedStripped, nil, nil,
		[]byte(fmt.Sprintf(`{"k":"%s","val":"dave"}`, strippedB64(kInsertedStripped))))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: baselineDir,
	}, slog.Default())

	res, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "bkeys", AsOf: asOf})
	if err != nil {
		t.Fatalf("full-table _snapshot over a BINARY(16) PK must attempt the merge since #1155 "+
			"(the pre-#1155 behaviour was a 1526 refusal), got: %v", err)
	}

	// Key each emitted row by the uppercase hex of its k cell. The wire cell
	// carries the raw bytes verbatim (fullTableTextCell passes []byte through).
	rawRows := rowCells(t, res.Resultset)
	// Count the RAW resultset before deduplicating into the map: a merge that
	// emits the same row twice (padded baseline row + stripped event row that
	// collapse to one key, or a byte-identical duplicate) must fail here, not
	// vanish into the map fold below.
	if len(rawRows) != 3 {
		t.Fatalf("full-table _snapshot emitted %d raw rows, want 3: %v", len(rawRows), rawRows)
	}
	got := map[string]string{}
	for _, cells := range rawRows {
		if len(cells) != 2 {
			t.Fatalf("expected 2 columns per row, got %v", cells)
		}
		got[strings.ToUpper(hex.EncodeToString([]byte(cells[0])))] = cells[1]
	}

	// CURRENT behaviour pinned, per #1160's note: the emitted PK width is
	// inconsistent by construction — a baseline pass-through row carries the
	// PADDED 16 bytes (DuckDB scan of the Parquet), while a changed/inserted
	// row carries the event image's STRIPPED bytes; fullTableTextCell returns
	// both verbatim (verify's renderCell normalizes, this path does not). A
	// mydumper restore is unaffected (MySQL re-pads on storage), but a shim
	// client running SELECT HEX(k) sees interleaved widths. Whether to
	// normalize is a product decision left open by #1160 — if it lands, update
	// the two stripped expectations below to the padded spelling.
	want := map[string]string{
		kUntouched:        "alice", // padded: never touched, only the baseline merge can produce it
		kUpdatedStripped:  "bob2",  // stripped: post-baseline UPDATE image wins
		kInsertedStripped: "dave",  // stripped: post-baseline INSERT appears
	}
	if len(got) != len(want) {
		t.Fatalf("full-table _snapshot returned %d rows %v, want %d %v — a PK that fails to join duplicates "+
			"every changed row (stale baseline row + event appended as a new PK)", len(got), got, len(want), want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("row k=0x%s = %q, want %q (full map: %v)", k, got[k], v, got)
		}
	}
	if _, deleted := got[kDeleted]; deleted {
		t.Errorf("full-table _snapshot included the post-baseline-deleted key 0x%s: %v", kDeleted, got)
	}
	if _, deleted := got[kDeletedStripped]; deleted {
		t.Errorf("full-table _snapshot included the deleted key under its stripped spelling 0x%s: %v", kDeletedStripped, got)
	}

	// Divergence anchor: the binlog-only full-table _flashback CANNOT produce
	// the never-touched row — proving the row above came from the baseline
	// merge actually running, not from the events.
	flashRes, err := h.runFullTable(TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "bkeys", AsOf: asOf})
	if err != nil {
		t.Fatalf("full-table _flashback: %v", err)
	}
	for _, cells := range rowCells(t, flashRes.Resultset) {
		if strings.ToUpper(hex.EncodeToString([]byte(cells[0]))) == kUntouched {
			t.Errorf("full-table _flashback included the never-touched baseline key — the _snapshot/_flashback divergence is lost")
		}
	}
}
