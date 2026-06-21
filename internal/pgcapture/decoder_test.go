package pgcapture_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ─── test helpers: construct pgoutput messages directly ───────────────────────
//
// The decoder type-switches on the concrete message type, not on Message.Type(),
// so these struct literals (with the embedded baseMessage left zero) decode
// exactly as wire-parsed messages would.

var txnTime = time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)

const txnLSN = pglogrepl.LSN(0x19DF9E8)

func textCol(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Length: uint32(len(s)), Data: []byte(s)}
}
func nullCol() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
}
func toastCol() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
}
func binaryCol(b []byte) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeBinary, Length: uint32(len(b)), Data: b}
}
func tuple(cols ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{ColumnNum: uint16(len(cols)), Columns: cols}
}

func relMsg(oid uint32, schema, table string, cols ...string) *pglogrepl.RelationMessage {
	rc := make([]*pglogrepl.RelationMessageColumn, len(cols))
	for i, n := range cols {
		rc[i] = &pglogrepl.RelationMessageColumn{Name: n, DataType: 25 /* text */}
	}
	return &pglogrepl.RelationMessage{
		RelationID: oid, Namespace: schema, RelationName: table,
		ReplicaIdentity: 'f', ColumnNum: uint16(len(rc)), Columns: rc,
	}
}

func beginMsg() *pglogrepl.BeginMessage {
	return &pglogrepl.BeginMessage{FinalLSN: txnLSN, CommitTime: txnTime}
}

// pkResolver returns a PKResolver that always reports the given PK column names.
func pkResolver(pkNames ...string) pgcapture.PKResolver {
	return func(_ uint32, _, _ string) ([]metadata.ColumnMeta, error) {
		cols := make([]metadata.ColumnMeta, len(pkNames))
		for i, n := range pkNames {
			cols[i] = metadata.ColumnMeta{Name: n, OrdinalPosition: i + 1, IsPK: true}
		}
		return cols, nil
	}
}

func mustDecode(t *testing.T, d *pgcapture.Decoder, msg pglogrepl.Message) (event.Event, bool) {
	t.Helper()
	ev, emit, err := d.Decode(msg)
	if err != nil {
		t.Fatalf("Decode(%T): unexpected error: %v", msg, err)
	}
	return ev, emit
}

// ─── happy-path decoding ──────────────────────────────────────────────────────

func TestDecode_Insert(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "orders", "id", "amount"))
	mustDecode(t, d, beginMsg())

	ev, emit := mustDecode(t, d, &pglogrepl.InsertMessage{
		RelationID: 1, Tuple: tuple(textCol("42"), textCol("100")),
	})
	if !emit {
		t.Fatal("INSERT should emit an event")
	}
	if ev.EventType != event.EventInsert {
		t.Errorf("EventType = %d, want EventInsert", ev.EventType)
	}
	if ev.Schema != "public" || ev.Table != "orders" {
		t.Errorf("schema.table = %s.%s, want public.orders", ev.Schema, ev.Table)
	}
	if ev.PKValues != "42" {
		t.Errorf("PKValues = %q, want %q", ev.PKValues, "42")
	}
	if ev.RowBefore != nil {
		t.Errorf("RowBefore = %v, want nil for INSERT", ev.RowBefore)
	}
	if ev.RowAfter["id"] != "42" || ev.RowAfter["amount"] != "100" {
		t.Errorf("RowAfter = %v, want {id:42, amount:100}", ev.RowAfter)
	}
	if !ev.Timestamp.Equal(txnTime) {
		t.Errorf("Timestamp = %v, want %v (from Begin)", ev.Timestamp, txnTime)
	}
	if ev.GTID != txnLSN.String() {
		t.Errorf("GTID = %q, want %q (commit LSN)", ev.GTID, txnLSN.String())
	}
}

func TestDecode_Delete(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "orders", "id", "amount"))
	mustDecode(t, d, beginMsg())

	ev, emit := mustDecode(t, d, &pglogrepl.DeleteMessage{
		RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple: tuple(textCol("42"), textCol("100")),
	})
	if !emit || ev.EventType != event.EventDelete {
		t.Fatalf("DELETE: emit=%v type=%d, want emit EventDelete", emit, ev.EventType)
	}
	if ev.PKValues != "42" {
		t.Errorf("PKValues = %q, want %q (from before-image)", ev.PKValues, "42")
	}
	if ev.RowAfter != nil {
		t.Errorf("RowAfter = %v, want nil for DELETE", ev.RowAfter)
	}
	if ev.RowBefore["amount"] != "100" {
		t.Errorf("RowBefore = %v, want amount=100", ev.RowBefore)
	}
}

func TestDecode_Commit(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, beginMsg())

	ev, emit := mustDecode(t, d, &pglogrepl.CommitMessage{CommitLSN: txnLSN, CommitTime: txnTime})
	if !emit || ev.EventType != event.EventCommit {
		t.Fatalf("COMMIT: emit=%v type=%d, want emit EventCommit", emit, ev.EventType)
	}
	if ev.GTID != txnLSN.String() {
		t.Errorf("EventCommit.GTID = %q, want commit LSN %q", ev.GTID, txnLSN.String())
	}
	if ev.EndPos != uint64(txnLSN) {
		t.Errorf("EventCommit.EndPos = %d, want %d", ev.EndPos, uint64(txnLSN))
	}
	if !ev.Timestamp.Equal(txnTime) {
		t.Errorf("EventCommit.Timestamp = %v, want %v", ev.Timestamp, txnTime)
	}

	// After Commit the transaction is closed: a row without a fresh Begin must fail.
	_, _, err := d.Decode(&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"))})
	if err == nil {
		t.Error("expected error for row event after Commit closed the transaction")
	}
}

// ─── FIX 2 (TOAST never-drop, Option B) + FIX 1 (PK from before-image) ─────────

func TestDecode_UpdateResolvesUnchangedToastFromBeforeImage(t *testing.T) {
	// An UPDATE under RI FULL that does NOT touch a TOASTed column: the new tuple
	// carries 'u' for that column; the full old tuple ('O') carries its real value.
	// The decoder must resolve the after-image to the real value (Option B), so the
	// column does NOT appear as changed and no sentinel is persisted.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "docs", "id", "title", "body"))
	mustDecode(t, d, beginMsg())

	ev, emit := mustDecode(t, d, &pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple:     tuple(textCol("7"), textCol("old title"), textCol("BIG DOCUMENT BODY")),
		NewTuple:     tuple(textCol("7"), textCol("new title"), toastCol()),
	})
	if !emit || ev.EventType != event.EventUpdate {
		t.Fatalf("UPDATE: emit=%v type=%d", emit, ev.EventType)
	}
	if got := ev.RowBefore["body"]; got != "BIG DOCUMENT BODY" {
		t.Errorf("RowBefore[body] = %v, want real value", got)
	}
	// Option B: after-image holds the REAL unchanged value, not a sentinel.
	if got := ev.RowAfter["body"]; got != "BIG DOCUMENT BODY" {
		t.Errorf("RowAfter[body] = %v, want resolved real value (Option B)", got)
	}
	// No marker anywhere.
	assertNoToastMarker(t, ev.RowAfter)
	assertNoToastMarker(t, ev.RowBefore)
	// changed_columns must EXCLUDE body (it did not change) — the discriminator.
	changed := event.ChangedColumns(ev.RowBefore, ev.RowAfter)
	for _, c := range changed {
		if c == "body" {
			t.Errorf("changed_columns wrongly includes the untouched TOAST column: %v", changed)
		}
	}
	if len(changed) != 1 || changed[0] != "title" {
		t.Errorf("changed_columns = %v, want [title]", changed)
	}
}

func TestDecode_UpdateToastedPKUnchanged_PKFromBeforeImage(t *testing.T) {
	// FIX 1: a TOASTed PK column unchanged by the UPDATE arrives as 'u' in the new
	// tuple. PKValues MUST come from the before-image so the sentinel never enters
	// pk_values (which would produce a wrong pk_hash — silent data loss).
	d := pgcapture.NewDecoder(pkResolver("pk"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "pk", "v"))
	mustDecode(t, d, beginMsg())

	ev, _ := mustDecode(t, d, &pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple:     tuple(textCol("REAL_PK"), textCol("a")),
		NewTuple:     tuple(toastCol(), textCol("b")), // pk unchanged + toasted
	})
	if ev.PKValues != "REAL_PK" {
		t.Errorf("PKValues = %q, want %q (from before-image, never the sentinel)", ev.PKValues, "REAL_PK")
	}
	if ev.PKValues == pgcapture.UnchangedToastKey || containsToastKey(ev.PKValues) {
		t.Errorf("PKValues leaked the unchanged-TOAST marker: %q", ev.PKValues)
	}
}

// ─── null vs empty vs marker (three distinct forms) ───────────────────────────

func TestDecode_NullVsEmptyVsToastMarker(t *testing.T) {
	// RI-DEFAULT-style UPDATE with no old tuple (key unchanged) → before-image is
	// nil, so a 'u' cannot be resolved and falls back to the structural marker
	// (the never-drop floor for the out-of-support case). Exercises three distinct
	// stored forms: nil (NULL), "" (empty string), and the marker.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "a", "b", "c"))
	mustDecode(t, d, beginMsg())

	ev, _ := mustDecode(t, d, &pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeNone,
		NewTuple:     tuple(textCol("1"), nullCol(), textCol(""), toastCol()),
	})
	if ev.PKValues != "1" {
		t.Errorf("PKValues = %q, want 1 (from after-image when before absent)", ev.PKValues)
	}
	a, ok := ev.RowAfter["a"]
	if !ok || a != nil {
		t.Errorf("RowAfter[a] = %v (present=%v), want nil (NULL)", a, ok)
	}
	if ev.RowAfter["b"] != "" {
		t.Errorf("RowAfter[b] = %v, want empty string (distinct from NULL)", ev.RowAfter["b"])
	}
	marker, ok := ev.RowAfter["c"].(map[string]any)
	if !ok || marker[pgcapture.UnchangedToastKey] != true {
		t.Errorf("RowAfter[c] = %v, want unchanged-TOAST marker map", ev.RowAfter["c"])
	}
}

// ─── fail-loud invariants ─────────────────────────────────────────────────────

func TestDecode_BinaryDatumFailsLoud(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "v"))
	mustDecode(t, d, beginMsg())

	_, _, err := d.Decode(&pglogrepl.InsertMessage{
		RelationID: 1, Tuple: tuple(textCol("1"), binaryCol([]byte{0x00, 0x01})),
	})
	if err == nil {
		t.Error("expected fail-loud error for a binary tuple datum (text format only)")
	}
}

func TestDecode_UnknownRelationFailsLoud(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.InsertMessage{RelationID: 99, Tuple: tuple(textCol("1"))})
	if err == nil {
		t.Error("expected error for a row event referencing an unknown relation OID")
	}
}

func TestDecode_RowOutsideTransactionFailsLoud(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id"))
	// No Begin → no commit timestamp → would land in the wrong partition.
	_, _, err := d.Decode(&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"))})
	if err == nil {
		t.Error("expected error for a row event outside a transaction")
	}
}

func TestDecode_PKColumnDriftFailsLoud(t *testing.T) {
	// The catalog reports a PK column absent from the relation's in-band columns
	// (schema drift during stream lag) → fail loud rather than build a wrong PK.
	d := pgcapture.NewDecoder(pkResolver("ghost"), event.Filters{}, nil)
	_, _, err := d.Decode(relMsg(1, "public", "t", "id", "v"))
	if err == nil {
		t.Error("expected error when a PK column is absent from the relation columns")
	}
}

func TestDecode_PKResolverErrorFailsLoud(t *testing.T) {
	boom := pgcapture.PKResolver(func(_ uint32, _, _ string) ([]metadata.ColumnMeta, error) {
		return nil, errBoom
	})
	d := pgcapture.NewDecoder(boom, event.Filters{}, nil)
	_, _, err := d.Decode(relMsg(1, "public", "t", "id"))
	if err == nil {
		t.Error("expected error to propagate from a failing PKResolver (no silent empty PK)")
	}
}

func TestDecode_ColumnCountMismatchFailsLoud(t *testing.T) {
	// A tuple whose arity differs from its cached relation must hard-stop, not
	// silently map columns to the wrong names.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "v")) // 2 columns
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.InsertMessage{
		RelationID: 1, Tuple: tuple(textCol("1")), // 1 column
	})
	if err == nil {
		t.Error("expected error for a tuple whose column count differs from the relation")
	}
}

func TestDecode_UnchangedToastInUpdateBeforeImageFailsLoud(t *testing.T) {
	// An 'u' (unchanged-TOAST) in an UPDATE's OLD tuple means the real value — the
	// one bintrail needs for recovery — is absent. Under RI FULL this never happens
	// (PG detoasts replica-identity columns into the old tuple); if it does, fail
	// loud rather than silently store a marker in the before-image.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "body"))
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple:     tuple(textCol("1"), toastCol()), // 'u' in the BEFORE image
		NewTuple:     tuple(textCol("1"), textCol("new")),
	})
	if err == nil {
		t.Error("expected fail-loud error for an unchanged-TOAST datum in an UPDATE before-image")
	}
}

func TestDecode_UnchangedToastInDeleteBeforeImageFailsLoud(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "body"))
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.DeleteMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeOld,
		OldTuple:     tuple(textCol("1"), toastCol()), // 'u' in the DELETE before-image
	})
	if err == nil {
		t.Error("expected fail-loud error for an unchanged-TOAST datum in a DELETE before-image")
	}
}

func TestDecode_DeleteWithoutBeforeImageFailsLoud(t *testing.T) {
	// A DELETE's before-image is the only source for its reversal INSERT; a missing
	// old tuple must fail loud, not index an un-keyed, un-reversible delete.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id"))
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.DeleteMessage{RelationID: 1, OldTuple: nil})
	if err == nil {
		t.Error("expected fail-loud error for a DELETE carrying no before-image")
	}
}

func TestDecode_TruncateNotIndexed(t *testing.T) {
	// TRUNCATE has a real behavioral contract: it must be surfaced (warned) but
	// NEVER indexed as a row event (DDL replay on the PG path is out of #530 scope).
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	_, emit, err := d.Decode(&pglogrepl.TruncateMessage{RelationNum: 1, RelationIDs: []uint32{1}})
	if err != nil {
		t.Fatalf("TRUNCATE: unexpected error: %v", err)
	}
	if emit {
		t.Error("TRUNCATE must not be indexed (DDL replay out of #530 scope) — emit should be false")
	}
}

func TestNewDecoder_NilPKResolverPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected NewDecoder(nil, ...) to panic — a nil PKResolver must fail at the wiring site")
		}
	}()
	pgcapture.NewDecoder(nil, event.Filters{}, nil)
}

// ─── filters + empty PK ───────────────────────────────────────────────────────

func TestDecode_FilterSkipsNonMatching(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"),
		event.Filters{Tables: map[string]bool{"public.keep": true}}, nil)
	mustDecode(t, d, relMsg(1, "public", "skip", "id"))
	mustDecode(t, d, beginMsg())
	_, emit := mustDecode(t, d, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"))})
	if emit {
		t.Error("INSERT on a filtered-out table should not emit")
	}
}

func TestDecode_EmptyPKTable(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver(), event.Filters{}, nil) // no PK columns
	mustDecode(t, d, relMsg(1, "public", "nopk", "a"))
	mustDecode(t, d, beginMsg())
	ev, emit := mustDecode(t, d, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("x"))})
	if !emit {
		t.Fatal("INSERT on a no-PK table should still emit (empty PKValues)")
	}
	if ev.PKValues != "" {
		t.Errorf("PKValues = %q, want empty for a no-PK table", ev.PKValues)
	}
}

// ─── value-mapping round-trip gate (advisor's elevated Slice-1 acceptance) ─────

func TestDecode_TextValuesRoundTripAsStrings(t *testing.T) {
	// The highest-risk invariant: PG values stored as Go strings survive the index
	// storage round-trip as strings, NOT json.Number/bool/nil. A []byte holding
	// "123"/"true"/"null" would trip the indexer's []byte→RawMessage promotion and
	// be silently re-typed; a Go string never does. marshalRow == json.Marshal for a
	// string-only map (the promotion only fires on []byte), so json.Marshal here is
	// faithful to the real write path; query.UnmarshalRowImage is the real read path.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "num", "boolish", "nullish"))
	mustDecode(t, d, beginMsg())
	ev, _ := mustDecode(t, d, &pglogrepl.InsertMessage{
		RelationID: 1,
		Tuple:      tuple(textCol("1"), textCol("123"), textCol("true"), textCol("null")),
	})
	// Decoder side: every text value is a Go string, never []byte.
	for k, v := range ev.RowAfter {
		if _, ok := v.(string); !ok {
			t.Errorf("RowAfter[%s] is %T, want string", k, v)
		}
	}
	// Storage round-trip side: read back as strings, not coerced.
	data, err := json.Marshal(ev.RowAfter)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	back := query.UnmarshalRowImage(data)
	for _, k := range []string{"num", "boolish", "nullish"} {
		if _, ok := back[k].(string); !ok {
			t.Errorf("read-back %s is %T (%v), want string — value was coerced", k, back[k], back[k])
		}
	}
	if back["num"] != "123" || back["boolish"] != "true" || back["nullish"] != "null" {
		t.Errorf("read-back values changed: %v", back)
	}
}

// ─── small helpers ────────────────────────────────────────────────────────────

var errBoom = errBoomType("catalog unreachable")

type errBoomType string

func (e errBoomType) Error() string { return string(e) }

func assertNoToastMarker(t *testing.T, row map[string]any) {
	t.Helper()
	for k, v := range row {
		if m, ok := v.(map[string]any); ok {
			if _, isMarker := m[pgcapture.UnchangedToastKey]; isMarker {
				t.Errorf("row[%s] is the unchanged-TOAST marker but should hold the real value", k)
			}
		}
	}
}

func containsToastKey(s string) bool {
	return strings.Contains(s, pgcapture.UnchangedToastKey)
}
