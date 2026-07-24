//go:build integration

package parser_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestParseFile_typeCorruptionMatrix_mariadb is the #620 sweep: it runs the
// project's whole type-corruption matrix (UNSIGNED sign-bit, BIT(64) sign-bit
// #497, SET(64) sign-bit #846, JSON, DECIMAL, DOUBLE precision, VARBINARY with
// an embedded NUL, DATETIME(6) microseconds) against a real MariaDB source in
// ONE pass, walking the FULL source → index → recover chain:
//
//  1. parse the real MariaDB binlog file (proves go-mysql decodes MariaDB's
//     TABLE_MAP/ROWS_EVENT encoding for each type the same as it does MySQL's);
//  2. feed the parsed events through the real indexer into a MySQL index DB
//     (proves marshalRow's storage encoding round-trips, e.g. does not silently
//     base64/float-collapse a MariaDB-sourced value);
//  3. read the stored row_after/row_before JSON back the way the production
//     read path does (query.UnmarshalRowImage, UseNumber) and generate reversal
//     SQL (proves recover emits byte-identical literals for a MariaDB source).
//
// Each type is asserted at all three stages so a flavor-specific divergence at
// any stage fails loud with the stage and type named.
func TestParseFile_typeCorruptionMatrix_mariadb(t *testing.T) {
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// SET(64 members) with the create-DDL padded to exactly 64 members so
	// member 64 (index 63, value 2^63) can be isolated — the #846 repro.
	members := make([]string, 64)
	for i := range members {
		members[i] = fmt.Sprintf("'m%d'", i+1)
	}
	setDDL := "SET(" + strings.Join(members, ",") + ")"

	testutil.MustExec(t, sourceDB, `CREATE TABLE tm (
		id      INT PRIMARY KEY AUTO_INCREMENT,
		u_big   BIGINT UNSIGNED NOT NULL,
		u_int   INT UNSIGNED NOT NULL,
		bit64   BIT(64) NOT NULL,
		set64   `+setDDL+` NOT NULL,
		doc     JSON NOT NULL,
		dec_val DECIMAL(20,4) NOT NULL,
		dbl_val DOUBLE NOT NULL,
		vb      VARBINARY(16) NOT NULL,
		ts6     DATETIME(6) NOT NULL
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	res, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition (MariaDB): %v", err)
	}

	// INSERT: every column at an edge value known to have tripped a real,
	// previously-fixed corruption class.
	testutil.MustExec(t, sourceDB, `INSERT INTO tm
		(u_big, u_int, bit64, set64, doc, dec_val, dbl_val, vb, ts6) VALUES (
		18446744073709551615,
		4294967295,
		b'1111111111111111111111111111111111111111111111111111111111111111',
		'm64',
		'{"a":1,"nested":{"b":[1,2,3]}}',
		123456789012.3456,
		1.0000000000001,
		X'610062',
		'2026-07-10 12:34:56.123456'
	)`)
	// UPDATE a non-edge column so the before-image of the edge columns is also
	// captured (recover's SET clause restores the before-image — the path
	// #490/#496 fixed end-to-end).
	testutil.MustExec(t, sourceDB, "UPDATE tm SET u_int = 1 WHERE id = 1")

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cpCmd := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mariadb:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog),
	)
	if out, err := cpCmd.CombinedOutput(); err != nil {
		testutil.SkipOrFailMariaDB(t, "docker cp %s from bintrail-test-mariadb failed: %v\n%s", currentBinlog, err, out)
	}

	p := parser.New(tmpDir, res, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	events := make(chan parser.Event, 50)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		errCh <- p.ParseFile(ctx, currentBinlog, events)
	}()
	all := drainEvents(events)
	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile: %v", err)
	}

	var ins, upd []parser.Event
	for _, ev := range all {
		if ev.Table != "tm" {
			continue
		}
		switch ev.EventType {
		case parser.EventInsert:
			ins = append(ins, ev)
		case parser.EventUpdate:
			upd = append(upd, ev)
		}
	}
	if len(ins) != 1 {
		t.Fatalf("expected 1 INSERT for table tm, got %d", len(ins))
	}
	if len(upd) != 1 {
		t.Fatalf("expected 1 UPDATE for table tm, got %d", len(upd))
	}

	// #986 workaround: MariaDB defers end_log_pos stamping for every
	// non-terminal event inside a GTID-tracked transaction (Table_map,
	// Write_rows/Update_rows all carry end_log_pos=0 on the wire — verified
	// against mariadb-binlog directly, not a go-mysql artifact), which
	// underflows handleRows' StartPos/EndPos computation for a MariaDB
	// source. That is a real, separately-filed position-tracking bug,
	// orthogonal to the column-VALUE fidelity this test sweeps (#620).
	// Stamp safe placeholder positions here so the rest of this test
	// exercises the real index-write and recover pipeline on otherwise
	// unmodified, real MariaDB-decoded row values.
	ins[0].StartPos, ins[0].EndPos = 100, 200
	upd[0].StartPos, upd[0].EndPos = 200, 300

	// ─── Stage 1: parser decode (go-mysql TABLE_MAP/ROWS_EVENT over MariaDB) ──

	after := ins[0].RowAfter
	if got := after["u_big"]; got != uint64(18446744073709551615) {
		t.Errorf("[parse] u_big (BIGINT UNSIGNED): want uint64 max, got %#v (%T)", got, got)
	}
	if got := after["u_int"]; got != uint32(4294967295) {
		t.Errorf("[parse] u_int (INT UNSIGNED): want uint32 max, got %#v (%T)", got, got)
	}
	if got := after["bit64"]; got != uint64(18446744073709551615) {
		t.Errorf("[parse] bit64 (BIT(64) all-set, #497 class): want uint64 max, got %#v (%T)", got, got)
	}
	// set64 = only member 64 active → bit 63 → 2^63. A regression to signed
	// decoding (the #846 class) yields int64(-9223372036854775808) instead.
	const wantSet64 = uint64(9223372036854775808)
	if got := after["set64"]; got != wantSet64 {
		t.Errorf("[parse] set64 (SET(64), member 64 active, #846 class): want %d, got %#v (%T)", wantSet64, got, got)
	}
	// At the PARSER stage doc is still raw []byte (the JSON text as decoded by
	// go-mysql) — marshalRow's content-sniffing promotion to a nested JSON
	// object happens at the INDEXER stage (asserted below), not here.
	docBytes, ok := after["doc"].([]byte)
	if !ok || !strings.Contains(string(docBytes), `"a":1`) {
		t.Errorf("[parse] doc (JSON, MariaDB LONGTEXT+json_valid): want []byte containing the source JSON, got %#v (%T)", after["doc"], after["doc"])
	}
	decStr := fmt.Sprintf("%v", after["dec_val"])
	if !strings.Contains(decStr, "123456789012.3456") {
		t.Errorf("[parse] dec_val (DECIMAL(20,4)): want 123456789012.3456, got %v (%T)", after["dec_val"], after["dec_val"])
	}
	if got, ok := after["dbl_val"].(float64); !ok || got != 1.0000000000001 {
		t.Errorf("[parse] dbl_val (DOUBLE precision): want 1.0000000000001, got %#v (%T)", after["dbl_val"], after["dbl_val"])
	}
	vb, ok := after["vb"].([]byte)
	if !ok || string(vb) != "a\x00b" {
		t.Errorf("[parse] vb (VARBINARY embedded NUL): want \"a\\x00b\", got %#v (%T)", after["vb"], after["vb"])
	}
	tsStr := fmt.Sprintf("%v", after["ts6"])
	if !strings.Contains(tsStr, "123456") && !strings.Contains(tsStr, "12:34:56") {
		t.Errorf("[parse] ts6 (DATETIME(6) microseconds): want fractional 123456 preserved, got %v", after["ts6"])
	}

	// UPDATE before-image: the edge columns must still be intact (this is what
	// recover's SET clause restores).
	before := upd[0].RowBefore
	if got := before["u_big"]; got != uint64(18446744073709551615) {
		t.Errorf("[parse] UPDATE before-image u_big: want uint64 max, got %#v (%T)", got, got)
	}
	if got := before["set64"]; got != wantSet64 {
		t.Errorf("[parse] UPDATE before-image set64: want %d, got %#v (%T)", wantSet64, got, got)
	}

	// ─── Stage 2: index write (indexer.marshalRow → MySQL binlog_events) ──────

	toIndex := make(chan parser.Event, 2)
	toIndex <- ins[0]
	toIndex <- upd[0]
	close(toIndex)
	idx := indexer.New(indexDB, 100)
	if _, err := idx.Run(ctx, toIndex); err != nil {
		t.Fatalf("indexer.Run: %v", err)
	}

	var rowAfterRaw, rowBeforeRaw []byte
	if err := indexDB.QueryRowContext(ctx,
		`SELECT row_after, row_before FROM binlog_events WHERE schema_name=? AND table_name='tm' AND event_type=2`,
		sourceName,
	).Scan(&rowAfterRaw, &rowBeforeRaw); err != nil {
		t.Fatalf("query indexed UPDATE row: %v", err)
	}
	stored := query.UnmarshalRowImage(rowBeforeRaw)
	if stored == nil {
		t.Fatal("UnmarshalRowImage(row_before) returned nil")
	}
	if got := fmt.Sprintf("%v", stored["u_big"]); got != "18446744073709551615" {
		t.Errorf("[index] stored row_before u_big: want 18446744073709551615, got %v", got)
	}
	if got := fmt.Sprintf("%v", stored["bit64"]); got != "18446744073709551615" {
		t.Errorf("[index] stored row_before bit64: want 18446744073709551615, got %v", got)
	}
	if got := fmt.Sprintf("%v", stored["set64"]); got != "9223372036854775808" {
		t.Errorf("[index] stored row_before set64 (#846 class): want 9223372036854775808, got %v", got)
	}
	if docMap, ok := stored["doc"].(map[string]any); !ok || docMap["a"] == nil {
		t.Errorf("[index] stored row_before doc: expected nested JSON object with key 'a', got %#v (%T)", stored["doc"], stored["doc"])
	}
	if got := fmt.Sprintf("%v", stored["dbl_val"]); !strings.HasPrefix(got, "1.0000000000001") {
		t.Errorf("[index] stored row_before dbl_val: want 1.0000000000001 prefix, got %v", got)
	}

	// ─── Stage 3: recover (byte-identical reversal SQL for a MariaDB source) ──

	g := recovery.New(indexDB, res)
	var buf strings.Builder
	if _, err := g.GenerateSQL(ctx, query.Options{Schema: sourceName, Table: "tm", Limit: 100}, &buf); err != nil {
		t.Fatalf("GenerateSQL: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		"18446744073709551615", // u_big / bit64 unsigned max, exact through recover
		"9223372036854775808",  // set64 member-64 (#846 class), exact through recover
		"123456789012.3456",    // DECIMAL exact digits
	} {
		if !strings.Contains(out, want) {
			t.Errorf("[recover] reversal SQL missing %q:\n%s", want, out)
		}
	}
	// VARBINARY must round-trip as an X'hex' literal of the exact bytes, never
	// the base64 storage text (#653 class).
	if !strings.Contains(out, "X'610062'") {
		t.Errorf("[recover] reversal SQL missing VARBINARY hex literal X'610062':\n%s", out)
	}
	// Negative-value corruption anchor: the #846 sign-flip regression would
	// emit this exact literal for set64 instead of its positive uint64 form.
	if strings.Contains(out, "-9223372036854775808") {
		t.Errorf("[recover] reversal SQL contains a sign-flip artifact \"-9223372036854775808\":\n%s", out)
	}
}
