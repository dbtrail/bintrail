package console

import (
	"encoding/json"
	"testing"
)

// TestVerifyStatusCarriesNoTriggerKey pins the premise the console frontend
// DERIVES history-ness from (#1417/#1425 review): renderVerifyResults treats
// any status carrying a `trigger` key as a persisted VerifyRunRecord and
// suppresses the Explain buttons — records always carry it (no omitempty),
// live statuses never do. Adding Trigger to VerifyStatus (a plausible #1191
// follow-up: "a scheduled run is in flight") would compile clean, marshal
// shadowed under VerifyRunRecord, and silently kill Explain on every LIVE
// run. This test is the boundary's Go half; the e2e's three legs all sit on
// the JS side.
//
// Scope honesty: this leg sees only a NO-omitempty addition — an omitempty
// Trigger populated mid-run would slip a zero-value marshal. If you add one,
// extend this test with a POPULATED status.
func TestVerifyStatusCarriesNoTriggerKey(t *testing.T) {
	b, err := json.Marshal(VerifyStatus{State: "running"})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["trigger"]; ok {
		t.Fatalf("VerifyStatus marshals a trigger key: %s — the console derives \"this is a "+
			"history record\" from that key's presence, so a live status carrying it silently "+
			"removes Explain from every live run; carry run provenance on VerifyRunRecord only, "+
			"or re-teach app.js renderVerifyResults first", b)
	}
	rec, err := json.Marshal(VerifyRunRecord{})
	if err != nil {
		t.Fatal(err)
	}
	var rm map[string]any
	if err := json.Unmarshal(rec, &rm); err != nil {
		t.Fatal(err)
	}
	if _, ok := rm["trigger"]; !ok {
		t.Fatalf("VerifyRunRecord no longer always carries trigger: %s — the same derivation "+
			"then misses real history records and resurrects the dead Explain buttons", rec)
	}
}
