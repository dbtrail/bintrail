package cli

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestPKChangeSuspected covers the single-row #782 classifier: when a lookup
// finds no baseline row, the earliest fetched event for the searched PK tells a
// genuinely-absent row (or a legitimate post-baseline INSERT) apart from a
// PK-changing UPDATE that stored the row under a different before-image PK.
func TestPKChangeSuspected(t *testing.T) {
	cases := []struct {
		name   string
		events []query.ResultRow
		want   bool
	}{
		{name: "no events (genuinely absent)", events: nil, want: false},
		{
			name:   "first event insert (legit post-baseline insert)",
			events: []query.ResultRow{{EventType: event.EventInsert}},
			want:   false,
		},
		{
			name:   "first event update (scenario B: PK-changed into existence)",
			events: []query.ResultRow{{EventType: event.EventUpdate}},
			want:   true,
		},
		{
			name:   "first event delete (scenario A: resurrected-then-deleted)",
			events: []query.ResultRow{{EventType: event.EventDelete}},
			want:   true,
		},
		{
			name: "insert first, then update (row origin accounted for)",
			events: []query.ResultRow{
				{EventType: event.EventInsert},
				{EventType: event.EventUpdate},
			},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pkChangeSuspected(tc.events); got != tc.want {
				t.Errorf("pkChangeSuspected = %v, want %v", got, tc.want)
			}
		})
	}
}
