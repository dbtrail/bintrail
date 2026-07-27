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
// Note what this does NOT do, because it is the trap in the issue's own
// suggested fix: a `seen` set over emitted PK strings cannot detect either
// case. The two rows carry DIFFERENT key strings — that is precisely why the
// join failed — and in the DELETE case only ONE row is emitted, so there is no
// duplicate to count. The guard has to compare the two possible spellings of
// the same key, not the strings actually emitted.
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
		{"deleted row would be resurrected", event.EventDelete, "resurrected after its DELETE"},
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
			for _, want := range []string{"db.bp", "full-table reconstruct", tc.wantInText, strippedKey} {
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
