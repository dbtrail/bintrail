package console

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// TestEventDTOOmitsConnectionID is the load-bearing open-core test: the events
// API must never expose connection_id (actor attribution = paid forensics).
func TestEventDTOOmitsConnectionID(t *testing.T) {
	cid := uint32(98765)
	gtid := "uuid:1-10"
	row := query.ResultRow{
		EventID:        1,
		EventTimestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		GTID:           &gtid,
		ConnectionID:   &cid, // present on the source row…
		SchemaName:     "app",
		TableName:      "users",
		EventType:      parser.EventUpdate,
		PKValues:       "42",
		ChangedColumns: []string{"email"},
		RowBefore:      map[string]any{"email": "a@x"},
		RowAfter:       map[string]any{"email": "b@x"},
	}

	b, err := json.Marshal(toEventDTO(row))
	if err != nil {
		t.Fatal(err)
	}
	js := string(b)

	// …but must NOT cross into the redacted wire view.
	if strings.Contains(js, "connection_id") {
		t.Errorf("eventDTO JSON must not contain connection_id key: %s", js)
	}
	if strings.Contains(js, "98765") {
		t.Errorf("eventDTO JSON must not leak the connection id value: %s", js)
	}

	for _, want := range []string{
		"event_id", "schema_name", "table_name", "event_type",
		"pk_values", "changed_columns", "row_before", "row_after", "gtid",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("eventDTO JSON missing expected field %q: %s", want, js)
		}
	}

	dto := toEventDTO(row)
	if dto.EventType != "UPDATE" {
		t.Errorf("EventType = %q, want UPDATE", dto.EventType)
	}
	if dto.EventTimestamp != "2026-01-02 03:04:05" {
		t.Errorf("EventTimestamp = %q, want 2026-01-02 03:04:05", dto.EventTimestamp)
	}
}

func TestEventTypeName(t *testing.T) {
	cases := map[parser.EventType]string{
		parser.EventInsert:   "INSERT",
		parser.EventUpdate:   "UPDATE",
		parser.EventDelete:   "DELETE",
		parser.EventSnapshot: "SNAPSHOT",
		parser.EventDDL:      "UNKNOWN",
	}
	for et, want := range cases {
		if got := eventTypeName(et); got != want {
			t.Errorf("eventTypeName(%d) = %q, want %q", et, got, want)
		}
	}
}

func TestToEventDTOsNonNil(t *testing.T) {
	if got := toEventDTOs(nil); got == nil {
		t.Error("toEventDTOs(nil) should return a non-nil empty slice (JSON []), got nil")
	}
}
