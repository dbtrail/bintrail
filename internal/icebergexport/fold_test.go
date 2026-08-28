package icebergexport

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

var idPK = []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}}

func ev(id uint64, typ event.EventType, pk string, before, after map[string]any) query.ResultRow {
	return query.ResultRow{
		EventID:        id,
		EventTimestamp: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).Add(time.Duration(id) * time.Second),
		SchemaName:     "shop",
		TableName:      "orders",
		EventType:      typ,
		PKValues:       pk,
		RowBefore:      before,
		RowAfter:       after,
	}
}

func row(id string, status string) map[string]any {
	return map[string]any{"id": json.Number(id), "status": status}
}

func TestFold_netEffectPerKey(t *testing.T) {
	f := newFold("shop", "orders", idPK)
	// Page 1: insert 1, insert 2, update 2, insert 3.
	if err := f.addPage([]query.ResultRow{
		ev(1, event.EventInsert, "1", nil, row("1", "new")),
		ev(2, event.EventInsert, "2", nil, row("2", "new")),
		ev(3, event.EventUpdate, "2", row("2", "new"), row("2", "paid")),
		ev(4, event.EventInsert, "3", nil, row("3", "new")),
	}); err != nil {
		t.Fatal(err)
	}
	// Page 2 (a page boundary must not reset anything): delete 3, delete 1,
	// then re-insert 1, and insert-then-delete 4 inside one page.
	if err := f.addPage([]query.ResultRow{
		ev(5, event.EventDelete, "3", row("3", "new"), nil),
		ev(6, event.EventDelete, "1", row("1", "new"), nil),
		ev(7, event.EventInsert, "1", nil, row("1", "back")),
		ev(8, event.EventInsert, "4", nil, row("4", "new")),
		ev(9, event.EventDelete, "4", row("4", "new"), nil),
	}); err != nil {
		t.Fatal(err)
	}
	if f.events != 9 {
		t.Fatalf("events = %d, want 9", f.events)
	}
	ops := f.touched()
	if len(ops) != 4 {
		t.Fatalf("touched keys = %d, want 4 (first-touched order 1,2,3,4)", len(ops))
	}
	want := []struct {
		pk     string
		exists bool
		status string
	}{
		{"1", true, "back"}, // deleted then re-inserted: the LAST image
		{"2", true, "paid"}, // inserted then updated
		{"3", false, ""},    // inserted then deleted
		{"4", false, ""},    // inserted and deleted inside one page
	}
	for i, w := range want {
		op := ops[i]
		if got := op.PK["id"]; got != json.Number(w.pk) {
			t.Errorf("op %d key = %v, want %s", i, got, w.pk)
		}
		if (op.Row != nil) != w.exists {
			t.Errorf("op %d exists = %v, want %v", i, op.Row != nil, w.exists)
		}
		if w.exists && op.Row["status"] != w.status {
			t.Errorf("op %d status = %v, want %s", i, op.Row["status"], w.status)
		}
	}
}

func TestFold_deleteKeyComesFromBeforeImage(t *testing.T) {
	f := newFold("shop", "orders", idPK)
	if err := f.addPage([]query.ResultRow{
		ev(1, event.EventDelete, "9", map[string]any{"id": json.Number("9"), "status": "gone"}, nil),
	}); err != nil {
		t.Fatal(err)
	}
	op := f.touched()[0]
	if op.Row != nil || op.PK["id"] != json.Number("9") {
		t.Fatalf("delete op = %+v, want key 9 from the before-image and no row", op)
	}
}

func TestFold_refusals(t *testing.T) {
	cases := []struct {
		name string
		evs  []query.ResultRow
		want string
	}{
		{"PK-changing UPDATE", []query.ResultRow{
			ev(1, event.EventUpdate, "1", row("1", "a"), row("2", "a")),
		}, "PK-changing UPDATE"},
		{"DELETE without before-image", []query.ResultRow{
			ev(1, event.EventDelete, "1", nil, nil),
		}, "carries no before-image"},
		{"INSERT without after-image", []query.ResultRow{
			ev(1, event.EventInsert, "1", nil, nil),
		}, "carries no after-image"},
		{"event type 0 (drift)", []query.ResultRow{
			ev(1, 0, "1", nil, row("1", "a")),
		}, "event type 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFold("shop", "orders", idPK)
			err := f.addPage(tc.evs)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestFold_compositeKeyImage(t *testing.T) {
	pk := []metadata.ColumnMeta{{Name: "tenant", IsPK: true, DataType: "int"}, {Name: "id", IsPK: true, DataType: "int"}}
	f := newFold("shop", "orders", pk)
	if err := f.addPage([]query.ResultRow{
		ev(1, event.EventInsert, "7|1", nil, map[string]any{"tenant": json.Number("7"), "id": json.Number("1"), "v": "x"}),
	}); err != nil {
		t.Fatal(err)
	}
	op := f.touched()[0]
	if len(op.PK) != 2 || op.PK["tenant"] != json.Number("7") || op.PK["id"] != json.Number("1") {
		t.Fatalf("PK image = %v, want both key columns", op.PK)
	}
	if _, leaked := op.PK["v"]; leaked {
		t.Fatal("PK image carries a non-key column")
	}
}
