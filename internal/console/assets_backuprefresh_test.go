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
		t.Fatal("the dormancy note is gone; a saved setting on a daemon with no schedule would look active")
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
