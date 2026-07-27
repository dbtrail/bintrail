package reconstruct

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// binaryPKBaseline writes a one-column-keyed baseline holding the FULL-WIDTH
// (padded) form of each key, which is what MySQL stores and mydumper dumps.
func binaryPKBaseline(t *testing.T, dir string, rows map[string]string) string {
	t.Helper()
	cols := []baseline.Column{
		{Name: "k", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
		{Name: "val", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	path := filepath.Join(dir, "bp.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for hexKey, val := range rows {
		if err := w.WriteRow([]string{"0x" + hexKey, val}, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path
}

func binaryPKCols() []metadata.ColumnMeta {
	return []metadata.ColumnMeta{{Name: "k", DataType: "binary", ColumnType: "binary(16)", IsPK: true}}
}

// TestMergeBaselineImages_pkSpellingJoinRefused is #1158.
//
// The merge joins a baseline row to its event by the pk_values string. When the
// two sides spell a fixed BINARY(n) key differently — padded on one side,
// trailing-0x00-stripped on the other — the join misses, and the outcome is
// asymmetric:
//
//   - DELETE: the event is skipped and the stale baseline row was already
//     emitted, so a deleted row is RESURRECTED. Nothing downstream catches it;
//     the dump restores cleanly and is wrong. This is the case that makes the
//     guard worth having.
//   - UPDATE: the stale row is emitted and the event is appended, producing a
//     duplicate key that a restore rejects with 1062 — loud, but late.
//
// Note what the issue's own suggested fix — a `seen` set over emitted PKs —
// can and cannot do, since the difference is what shaped this guard:
//
//   - the UPDATE case it CAN catch, if keyed on each emitted row's
//     CANONICALIZED pk rather than on the change-map key (both rows canonicalize
//     to the stripped spelling), and it would catch a broader class of
//     canonicalization disagreements than fixed-binary padding;
//   - the DELETE case it CANNOT catch at all: only ONE row is emitted, so
//     there is no duplicate to count.
//
// The DELETE case is the one that corrupts output silently, and a `seen` set
// over emitted rows costs O(table) memory — which is what #1097 and #1107 exist
// to remove from this path. Hence comparing the two possible SPELLINGS of a
// key, at O(1), instead of the strings actually emitted.
func TestMergeBaselineImages_pkSpellingJoinRefused(t *testing.T) {
	const (
		paddedKey   = "11223344556677889900AABB00000000" // as stored / as dumped
		strippedKey = "0x11223344556677889900AABB"       // as the ROW image carries it
	)

	for _, tc := range []struct {
		name       string
		evType     event.EventType
		wantInText string
	}{
		{"deleted row would be resurrected", event.EventDelete, "kept alive past its DELETE"},
		{"updated row would be emitted twice", event.EventUpdate, "emitted twice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := binaryPKBaseline(t, t.TempDir(), map[string]string{paddedKey: "baseline"})

			// The change map is keyed by the PADDED spelling, i.e. NOT the one
			// canonicalizePKValue produces — the shape a canonicalization
			// regression creates.
			ev := &query.ResultRow{
				EventType: tc.evType, SchemaName: "db", TableName: "bp",
				PKValues: "0x" + paddedKey,
			}
			if tc.evType != event.EventDelete {
				ev.RowAfter = map[string]any{"k": mustDecodeHex(t, paddedKey), "val": "updated"}
			}

			var emitted []map[string]any
			err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
				BaselinePath: path, Schema: "db", Table: "bp", PKCols: binaryPKCols(),
				Changes: map[string]*query.ResultRow{"0x" + paddedKey: ev},
			}, func(r map[string]any) error { emitted = append(emitted, r); return nil })

			if err == nil {
				t.Fatalf("merge succeeded and emitted %d row(s); it must refuse — the baseline row and its "+
					"%v event are keyed differently, so the output would be wrong", len(emitted), tc.evType)
			}
			for _, want := range []string{"db.bp", "baseline merge", tc.wantInText, strippedKey} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error missing %q:\n%s", want, err)
				}
			}
			// Refusing after emitting the bad row would still ship it to the
			// mydumper writer, which streams.
			if len(emitted) != 0 {
				t.Errorf("refusal must fire BEFORE the stale row is emitted, emitted %d", len(emitted))
			}
		})
	}
}

// TestMergeBaselineImages_correctSpellingStillMerges is the control that keeps
// the guard from being a blunt instrument: with the canonicalization working
// (the shipping behaviour), the same fixtures must merge cleanly.
func TestMergeBaselineImages_correctSpellingStillMerges(t *testing.T) {
	const paddedKey = "11223344556677889900AABB00000000"
	path := binaryPKBaseline(t, t.TempDir(), map[string]string{paddedKey: "baseline"})

	// Keyed the way the indexer really stores it: stripped.
	changes := map[string]*query.ResultRow{
		"0x11223344556677889900AABB": {
			EventType: event.EventUpdate, SchemaName: "db", TableName: "bp",
			PKValues: "0x11223344556677889900AABB",
			RowAfter: map[string]any{"k": mustDecodeHex(t, "11223344556677889900AABB"), "val": "updated"},
		},
	}
	var emitted []map[string]any
	err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
		BaselinePath: path, Schema: "db", Table: "bp", PKCols: binaryPKCols(),
		Changes: changes,
	}, func(r map[string]any) error { emitted = append(emitted, r); return nil })
	if err != nil {
		t.Fatalf("a correctly-keyed merge must not trip the #1158 guard: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("emitted %d rows, want 1", len(emitted))
	}
	if emitted[0]["val"] != "updated" {
		t.Errorf("the event did not win over the baseline row: %v", emitted[0])
	}
}

// TestAltFixedBinaryPK_noFalseCollisions pins the property that makes the guard
// safe to fail loud on: toggling the padding is injective over what a BINARY(n)
// column can hold, so it can never make two DISTINCT real keys look like the
// same row. A guard that aborts a healthy dump would be worse than the bug.
func TestAltFixedBinaryPK_noFalseCollisions(t *testing.T) {
	pkCols := binaryPKCols()
	// Keys that differ only in where their zero bytes fall — the shapes most
	// likely to collide under a careless normalisation.
	keys := []string{
		"11223344556677889900AABB00000000",
		"11223344556677889900AABB00000001",
		"1122334455667788990000BB00000000",
		"00000000000000000000000000000000",
		"11000000000000000000000000000000",
		"B2815CC3C200FF7C0102030405060780",
	}
	canonical := map[string]string{}
	alternate := map[string]string{}
	withAlt := 0
	for _, k := range keys {
		raw := mustDecodeHex(t, k)
		pkMap, err := canonicalizePKMap(map[string]any{"k": raw}, pkCols)
		if err != nil {
			t.Fatalf("canonicalize %s: %v", k, err)
		}
		c := event.BuildPKValues(pkCols, pkMap)
		if prev, dup := canonical[c]; dup {
			t.Fatalf("two distinct keys canonicalize identically: %s and %s → %q", prev, k, c)
		}
		canonical[c] = k

		if a, ok := altFixedBinaryPK(pkCols, pkMap); ok {
			withAlt++
			if prev, dup := alternate[a]; dup {
				t.Errorf("two distinct keys produce the same ALTERNATE spelling: %s and %s → %q", prev, k, a)
			}
			alternate[a] = k
			if a == c {
				t.Errorf("key %s: alternate equals canonical %q — the toggle did nothing", k, c)
			}
			// The decisive one: an alternate must never collide with some
			// OTHER key's canonical spelling, or the guard would refuse a
			// perfectly healthy table.
			if other, clash := canonical[a]; clash && other != k {
				t.Errorf("key %s's alternate %q collides with the canonical spelling of %s — "+
					"the guard would abort a healthy dump", k, a, other)
			}
		}
	}
	// Positive anchor. Every discriminating assertion above sits inside the
	// `ok` branch, so an altFixedBinaryPK that returned false for everything
	// would skip them all and this test would pass having checked nothing.
	if want := 4; withAlt != want {
		t.Errorf("%d of %d keys produced an alternate spelling, want %d — if this dropped to 0 every "+
			"assertion above became vacuous", withAlt, len(keys), want)
	}
}

// TestMergeBaselineImages_healthyTableWithPendingEvents is the false-positive
// control that TestMergeBaselineImages_correctSpellingStillMerges cannot be.
//
// The guard only fires against an UNDRAINED change-map entry, and the scan
// drains as it goes — so a one-row/one-event fixture has an empty map by the
// time it matters, and a guard that mis-fired against OTHER rows' pending
// events would pass it green. This drives many keys through the real merge
// with events pending for only a subset, which is the only configuration in
// which a false refusal is possible.
func TestMergeBaselineImages_healthyTableWithPendingEvents(t *testing.T) {
	pkCols := binaryPKCols()

	// Keys spanning every trailing-zero count 0..15 plus interior zeros, so
	// both the "has an alternate" and "single spelling" populations are large.
	rows := map[string]string{}
	var keys []string
	for i := range 16 {
		k := strings.Repeat("A7", 16-i) + strings.Repeat("00", i)
		rows[k] = "baseline"
		keys = append(keys, k)
	}
	for i := range 8 {
		k := "5C00" + strings.Repeat("B3", 13-i) + strings.Repeat("00", i+1)
		k = k[:32]
		rows[k] = "baseline"
		keys = append(keys, k)
	}
	path := binaryPKBaseline(t, t.TempDir(), rows)

	// Events for every third key, correctly spelled (stripped), so the map is
	// still populated while later rows are being scanned.
	changes := map[string]*query.ResultRow{}
	updated := map[string]bool{}
	for i, k := range keys {
		if i%3 != 0 {
			continue
		}
		stripped := event.BuildPKValues(pkCols, mustCanonicalize(t, pkCols, mustDecodeHex(t, k)))
		changes[stripped] = &query.ResultRow{
			EventType: event.EventUpdate, SchemaName: "db", TableName: "bp",
			PKValues: stripped,
			RowAfter: map[string]any{"k": mustDecodeHex(t, k), "val": "updated"},
		}
		updated[k] = true
	}
	wantUpdated := len(changes)
	if wantUpdated < 5 {
		t.Fatalf("fixture built only %d pending events; too few to leave the map populated across the scan", wantUpdated)
	}

	var gotBaseline, gotUpdated int
	err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
		BaselinePath: path, Schema: "db", Table: "bp", PKCols: pkCols,
		Changes: changes,
	}, func(r map[string]any) error {
		switch r["val"] {
		case "baseline":
			gotBaseline++
		case "updated":
			gotUpdated++
		default:
			t.Errorf("unexpected emitted value %v", r["val"])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("the guard refused a healthy table — a false refusal is worse than the bug it cures: %v", err)
	}
	// NOTE: read before the merge — mergeBaselineImages DRAINS in.Changes.
	if gotUpdated != wantUpdated {
		t.Errorf("emitted %d updated rows, want %d", gotUpdated, wantUpdated)
	}
	if want := len(keys) - wantUpdated; gotBaseline != want {
		t.Errorf("emitted %d pass-through rows, want %d", gotBaseline, want)
	}
}

// TestMergeBaselineImages_compositeBinaryPK covers a PK with two fixed-binary
// components, one padded and one full-width. It pins that the alternate is
// built over ALL PK columns in ordinal order (a regression that toggled only
// the first, or that aliased the source map instead of copying it, is
// otherwise invisible), and that the healthy version still merges.
func TestMergeBaselineImages_compositeBinaryPK(t *testing.T) {
	pkCols := []metadata.ColumnMeta{
		{Name: "a", DataType: "binary", ColumnType: "binary(16)", IsPK: true, OrdinalPosition: 1},
		{Name: "b", DataType: "binary", ColumnType: "binary(4)", IsPK: true, OrdinalPosition: 2},
	}
	const (
		aPadded = "11223344556677889900AABB00000000" // has padding
		bFull   = "DEADBEEF"                         // no padding
	)

	writeComposite := func(t *testing.T) string {
		t.Helper()
		cols := []baseline.Column{
			{Name: "a", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
			{Name: "b", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
			{Name: "val", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		}
		path := filepath.Join(t.TempDir(), "cp.parquet")
		w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if err := w.WriteRow([]string{"0x" + aPadded, "0x" + bFull, "baseline"}, []bool{false, false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return path
	}

	canonicalPK := event.BuildPKValues(pkCols, mustCanonicalizeMulti(t, pkCols,
		map[string]any{"a": mustDecodeHex(t, aPadded), "b": mustDecodeHex(t, bFull)}))

	t.Run("mis-keyed on the padded component refuses", func(t *testing.T) {
		misKeyed := "0x" + aPadded + "|0x" + bFull
		var emitted int
		err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
			BaselinePath: writeComposite(t), Schema: "db", Table: "cp", PKCols: pkCols,
			Changes: map[string]*query.ResultRow{misKeyed: {
				EventType: event.EventDelete, SchemaName: "db", TableName: "cp", PKValues: misKeyed,
			}},
		}, func(map[string]any) error { emitted++; return nil })
		if err == nil {
			t.Fatalf("merge succeeded, emitting %d row(s); the composite key's padded component is mis-spelled "+
				"and the DELETE would be lost", emitted)
		}
		// The alternate must carry BOTH components, the toggled one and the
		// untouched one, joined in ordinal order.
		if !strings.Contains(err.Error(), misKeyed) {
			t.Errorf("error does not name the full composite alternate %q:\n%s", misKeyed, err)
		}
	})

	t.Run("correctly keyed still merges", func(t *testing.T) {
		var emitted []map[string]any
		err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
			BaselinePath: writeComposite(t), Schema: "db", Table: "cp", PKCols: pkCols,
			Changes: map[string]*query.ResultRow{canonicalPK: {
				EventType: event.EventUpdate, SchemaName: "db", TableName: "cp", PKValues: canonicalPK,
				RowAfter: map[string]any{"a": mustDecodeHex(t, aPadded), "b": mustDecodeHex(t, bFull), "val": "updated"},
			}},
		}, func(r map[string]any) error { emitted = append(emitted, r); return nil })
		if err != nil {
			t.Fatalf("a correctly-keyed composite PK must not trip the guard: %v", err)
		}
		if len(emitted) != 1 || emitted[0]["val"] != "updated" {
			t.Errorf("emitted %v, want one updated row", emitted)
		}
	})
}

// TestMergeBaselineImages_guardCoversClaimedRow pins the reason the check sits
// ABOVE the claimed/unclaimed branch rather than inside the unclaimed one.
//
// in.Changes is keyed by string, so two spellings of one row are two
// independent entries. With an entry under the canonical spelling the row takes
// the CLAIMED branch; a sibling entry under the alternate spelling would then
// survive the scan and reach the leftover tail, where a DELETE is dropped
// without a word — the row outlives its own DELETE with the guard in the room.
func TestMergeBaselineImages_guardCoversClaimedRow(t *testing.T) {
	const paddedKey = "11223344556677889900AABB00000000"
	pkCols := binaryPKCols()
	path := binaryPKBaseline(t, t.TempDir(), map[string]string{paddedKey: "baseline"})

	stripped := event.BuildPKValues(pkCols, mustCanonicalize(t, pkCols, mustDecodeHex(t, paddedKey)))
	changes := map[string]*query.ResultRow{
		// Claims the row, so without hoisting the guard never runs.
		stripped: {
			EventType: event.EventUpdate, SchemaName: "db", TableName: "bp", PKValues: stripped,
			RowAfter: map[string]any{"k": mustDecodeHex(t, "11223344556677889900AABB"), "val": "updated"},
		},
		// The sibling entry that would be silently dropped.
		"0x" + paddedKey: {
			EventType: event.EventDelete, SchemaName: "db", TableName: "bp", PKValues: "0x" + paddedKey,
		},
	}

	var emitted []map[string]any
	err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
		BaselinePath: path, Schema: "db", Table: "bp", PKCols: pkCols, Changes: changes,
	}, func(r map[string]any) error { emitted = append(emitted, r); return nil })
	if err == nil {
		t.Fatalf("merge succeeded and emitted %v — an undrained DELETE keyed under the alternate spelling "+
			"was dropped in the leftover tail while the guard sat on the unclaimed branch only", emitted)
	}
	if !strings.Contains(err.Error(), "DELETE") {
		t.Errorf("error should name the undrained DELETE:\n%s", err)
	}
}

func mustCanonicalize(t *testing.T, pkCols []metadata.ColumnMeta, k []byte) map[string]any {
	t.Helper()
	return mustCanonicalizeMulti(t, pkCols, map[string]any{"k": k})
}

func mustCanonicalizeMulti(t *testing.T, pkCols []metadata.ColumnMeta, row map[string]any) map[string]any {
	t.Helper()
	m, err := canonicalizePKMap(row, pkCols)
	if err != nil {
		t.Fatalf("canonicalizePKMap: %v", err)
	}
	return m
}

// TestAltFixedBinaryPK_inertWithoutFixedBinary keeps the guard free for every
// table it does not apply to: no fixed BINARY(n) PK, or a pre-#212 snapshot
// with no declared width to pad to, means no second lookup at all.
func TestAltFixedBinaryPK_inertWithoutFixedBinary(t *testing.T) {
	cases := []struct {
		name  string
		col   metadata.ColumnMeta
		value any
	}{
		{"int PK", metadata.ColumnMeta{Name: "k", DataType: "int"}, int64(7)},
		{"varbinary PK", metadata.ColumnMeta{Name: "k", DataType: "varbinary", ColumnType: "varbinary(16)"}, []byte{0xAA, 0xBB, 0x00}},
		{"binary PK with no declared width", metadata.ColumnMeta{Name: "k", DataType: "binary"}, []byte{0xAA, 0xBB, 0x00}},
		{"binary PK already at full width with no padding", metadata.ColumnMeta{Name: "k", DataType: "binary", ColumnType: "binary(2)"}, []byte{0xAA, 0xBB}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols := []metadata.ColumnMeta{tc.col}
			if got, ok := altFixedBinaryPK(cols, map[string]any{"k": tc.value}); ok {
				t.Errorf("altFixedBinaryPK returned %q; it must be inert here", got)
			}
		})
	}
}

func TestFixedBinaryWidth(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"binary(16)", 16},
		{"BINARY(255)", 255},
		{"binary", 0},
		{"varbinary(16)", 0},
		{"binary(0)", 0},
		{"binary(x)", 0},
		{"", 0},
	}
	for _, tc := range cases {
		if got := FixedBinaryWidth(tc.in); got != tc.want {
			t.Errorf("FixedBinaryWidth(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex fixture %q: %v", s, err)
	}
	return b
}
