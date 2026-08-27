package console

import (
	"strings"
	"testing"
)

// TestBackupRefreshCard_neverClaimsLiveWhileDormant pins the one thing this
// card must not do: state two opposite things about the running system.
//
// The card prints "No refresh schedule is set, so nothing runs yet" whenever
// br.enabled is false. If the provenance row says "(live)" purely because an
// override exists, the same card simultaneously tells the operator the setting
// is running and that nothing runs. That is not a wording preference: the
// setting changes the on-disk representation of their backups, and the panel is
// where consent for that is taken.
func TestBackupRefreshCard_neverClaimsLiveWhileDormant(t *testing.T) {
	// jsFunctionBody, not functionBody: it strips comment lines before walking.
	// These guards search for the rendered LABELS, and this function documents
	// its own gate in a comment that quotes them, so an unstripped body matches
	// the prose instead of the code and reports a pass on a deleted gate.
	body := jsFunctionBody(t, readAsset(t, "app.js"), "backupRefreshCard")

	if !strings.Contains(body, "(live)") {
		t.Fatal("backupRefreshCard no longer distinguishes a live setting at all; this guard covers nothing")
	}
	// The gate itself. A bare `br.source === "override" ? "... (live)"` has no
	// br.enabled between the two, which is exactly the shape this catches.
	i := strings.Index(body, "(live)")
	head := body[:i]
	j := strings.LastIndex(head, "kvRow(")
	if j < 0 {
		t.Fatal(`"(live)" is no longer rendered from a kvRow; re-point this guard`)
	}
	if !strings.Contains(head[j:], "br.enabled") {
		t.Fatalf("the (live) label is not gated on br.enabled, so the card can call a dormant setting live "+
			"while also saying nothing runs yet:\n%s", head[j:])
	}
	if !strings.Contains(body, "!br.enabled") {
		t.Fatal("the dormancy note is gone; a setting nothing consumes would look active")
	}
	// Three states, and the middle one is the whole point: --baseline-trigger
	// with no schedule runs no loop but DOES apply this to restores. Collapsing
	// it back into the dormant branch is the misreport this guard exists for.
	if !strings.Contains(body, "br.scheduled") {
		t.Fatal("the card no longer distinguishes 'no schedule' from 'nothing uses this', so a daemon whose " +
			"restores reuse files today would be told nothing runs yet")
	}
}

// TestSaveBackupRefresh_confirmsFromTheResponse: the toast must describe what
// the daemon reported back, not what was clicked.
//
// Two things make the clicked value wrong. "Use the daemon setting" does not
// know in advance what the daemon flag says, so it has no value to report. And
// a card rendered before a restart carries a stale schedule, so a confirmation
// built from it can claim a setting applies now when it does not. Reading the
// echoed DTO costs nothing and cannot be stale.
func TestSaveBackupRefresh_confirmsFromTheResponse(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "saveBackupRefresh")

	if !strings.Contains(body, "await api(") {
		t.Fatal("saveBackupRefresh no longer PUTs; this guard covers nothing")
	}
	// The response has to be captured, not discarded.
	if !strings.Contains(body, "= await api(") {
		t.Fatal("the PUT response is discarded, so the confirmation cannot describe what the daemon actually stored")
	}
	toast := body[strings.Index(body, "toast("):]
	// Qualified by the RESPONSE variable, not the bare field name. A bare
	// "carry_forward_unchanged" also matches the request body that was just
	// sent, which is precisely the stale value this guard exists to reject, and
	// a mutation swapping now.* for body.* survived until this was tightened.
	for _, want := range []string{"now.enabled", "now.carry_forward_unchanged"} {
		if !strings.Contains(body, want) {
			t.Errorf("the confirmation does not read %s from the PUT response", want)
		}
	}
	for _, banned := range []string{"next", "body.carry_forward_unchanged"} {
		if strings.Contains(toast, banned) || (banned != "next" && strings.Contains(body, banned)) {
			t.Errorf("the confirmation still branches on %q, the value that was SENT, rather than the daemon's answer", banned)
		}
	}
	if !strings.Contains(body, "renderRoute()") {
		t.Fatal("the card is not re-rendered after saving, so it would keep showing the previous state")
	}
}

// TestBaselineRefreshNote_partitionsTheTables: reused and refreshed must ADD UP
// to the run's table count, never overlap.
//
// The first version of this line printed the total beside the subset, so a run
// that reused everything rendered "5 table(s) refreshed, 5 of them reused
// unchanged" and asked the operator to do the subtraction. It also inverted the
// rule the CLI summary follows and the docs state: a reused table is not a
// refreshed one, and which tables actually cost a full rewrite is the number
// worth seeing.
func TestBaselineRefreshNote_partitionsTheTables(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "baselineRefreshNote")

	if !strings.Contains(body, "carried") {
		t.Fatal("the refresh note no longer reports reused tables; this guard covers nothing")
	}
	// The refreshed count has to be a DIFFERENCE. A bare rf.tables next to
	// rf.carried is the double count.
	if !strings.Contains(body, "- reused") && !strings.Contains(body, "-reused") {
		t.Error("the refreshed count is not the total minus the reused count, so the two overlap and " +
			"a fully reused run reads as fully refreshed")
	}
	if strings.Contains(body, `(rf.tables || 0) + " table(s) refreshed"`) {
		t.Error("the note prints the TOTAL table count as the refreshed count while also printing the " +
			"reused subset; those two numbers must partition the run")
	}
}
