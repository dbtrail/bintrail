package indexquery

import "testing"

// TestEventTypeNameVocabulary pins the exported event-type vocabulary and its
// numeric values — a persistence contract (binlog_events.event_type): never
// renumber.
func TestEventTypeNameVocabulary(t *testing.T) {
	cases := []struct {
		et   EventType
		num  uint8
		want string
	}{
		{EventInsert, 1, "INSERT"},
		{EventUpdate, 2, "UPDATE"},
		{EventDelete, 3, "DELETE"},
		{EventDDL, 4, "DDL"},
		{EventGTID, 5, "GTID"},
		{EventSnapshot, 6, "SNAPSHOT"},
		{EventType(250), 250, "UNKNOWN"},
	}
	for _, tc := range cases {
		if uint8(tc.et) != tc.num {
			t.Errorf("EventType %s = %d, want %d (persistence contract)", tc.want, tc.et, tc.num)
		}
		if got := EventTypeName(tc.et); got != tc.want {
			t.Errorf("EventTypeName(%d) = %q, want %q", tc.et, got, tc.want)
		}
	}
}
