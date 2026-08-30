package console

import (
	"encoding/json"
	"os"
	"regexp"
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
	// Assert on the CONDITION, not on adjacency. "br.enabled appears somewhere
	// before (live)" is satisfied by `br.enabled && br.source === "override" ?
	// "this page (live)" : "daemon default"`, which renders a dormant override
	// as "daemon default" while the Use-the-daemon-setting button is still on
	// screen, and by "this page (live, not running yet)", which says both
	// things at once. Both of those survived the adjacency check.
	const gate = `br.enabled ? "this page (live)"`
	if !strings.Contains(body, gate) {
		t.Fatalf("the (live) label is not produced by a %s ternary, so the card can call a dormant setting "+
			"live or drop the override label entirely:\n%s", gate, body)
	}
	// And the dormant arm must not smuggle the word back in.
	k := strings.Index(body, gate) + len(gate)
	rest := body[k:]
	arm := rest[:min(len(rest), 120)]
	if strings.Contains(arm, "(live") {
		t.Errorf("the dormant arm of the ternary also says live:\n%s", arm)
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
	ti := strings.Index(body, "toast(")
	if ti < 0 {
		t.Fatal("saveBackupRefresh no longer confirms anything; this guard covers nothing " +
			"(and slicing on a missing needle would panic the whole package)")
	}
	_ = body[ti:] // the needle exists; the checks below read the whole body
	// Qualified by the RESPONSE variable, not the bare field name. A bare
	// "carry_forward_unchanged" also matches the request body that was just
	// sent, which is precisely the stale value this guard exists to reject, and
	// a mutation swapping now.* for body.* survived until this was tightened.
	for _, want := range []string{"now.enabled", "now.carry_forward_unchanged"} {
		if !strings.Contains(body, want) {
			t.Errorf("the confirmation does not read %s from the PUT response", want)
		}
	}
	// Ban the IDENTIFIERS, not the English word: "applies on the next start"
	// is correct prose and used to fail this.
	for _, banned := range []string{"next ?", "next)", "body.carry_forward_unchanged", "body.enabled"} {
		if strings.Contains(body, banned) {
			t.Errorf("the confirmation reads %q, the value that was SENT, rather than the daemon's answer", banned)
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

// TestBackupRefreshWireNamesMatchTheFrontend: the Go struct tag and the
// JavaScript that reads it are written independently and nothing else compares
// them.
//
// This is the `schema_name`/`table_name` class: renaming the tag leaves both
// sides internally consistent, compiles, and passes every test, while the
// number silently stops appearing. The tag is derived by MARSHALLING rather
// than by reading the source, so the assertion is about the bytes on the wire.
func TestBackupRefreshWireNamesMatchTheFrontend(t *testing.T) {
	js := readAsset(t, "app.js")

	raw, err := json.Marshal(BaselineStatus{State: "succeeded", Tables: 3, Carried: 2})
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if _, ok := wire["carried"]; !ok {
		t.Fatalf("BaselineStatus no longer serialises a \"carried\" key (got %s); the console reads rf.carried "+
			"and would silently stop showing reused tables", raw)
	}
	for _, ref := range []string{"rf.carried", "rst.carried"} {
		if !strings.Contains(js, ref) {
			t.Errorf("app.js does not read %s, so the reused count never reaches the page", ref)
		}
	}

	dto, err := json.Marshal(baselineRefreshDTO{CarryForwardUnchanged: true, Source: "override", Enabled: true, Scheduled: true})
	if err != nil {
		t.Fatal(err)
	}
	var d map[string]any
	if err := json.Unmarshal(dto, &d); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"carry_forward_unchanged", "source", "enabled", "scheduled"} {
		if _, ok := d[key]; !ok {
			t.Errorf("baselineRefreshDTO does not serialise %q (got %s)", key, dto)
		}
		if !strings.Contains(js, "br."+key) && !strings.Contains(js, key) {
			t.Errorf("app.js never reads %q from the refresh DTO", key)
		}
	}
}

// TestStoragePanelStillMountsTheRefreshCard: the card can be unmounted, or its
// fetch removed, with the whole suite green.
//
// The two guards above check what the card renders once it is called. Neither
// notices if nothing calls it: dropping the append makes the settings panel
// vanish, and dropping the fetch makes it render its error branch forever. A
// setting an operator cannot reach is the same as a setting that does not
// exist.
func TestStoragePanelStillMountsTheRefreshCard(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "buildStorage")
	if !strings.Contains(body, "backupRefreshCard(") {
		t.Error("buildStorage no longer mounts backupRefreshCard, so the reuse setting has no UI at all")
	}
	js := readAsset(t, "app.js")
	if !strings.Contains(js, `api("/api/baseline-refresh")`) {
		t.Error("nothing fetches /api/baseline-refresh, so the card can only ever render its error branch")
	}
}

// TestBackupRefreshCard_titleSaysWhatItDoes (#1528). This card controls one
// thing: whether an unchanged table reuses its previous file instead of being
// written again. It is a storage behaviour with no timetable in it, and it
// used to be titled "Automatic backup refresh" one route away from "Scheduled
// backups", which IS the timetable. Two names both promising "backups,
// automatically", for two unrelated settings.
//
// The third assertion is what makes this a guard rather than a spelling check:
// it fails if the collision is "fixed" by renaming the schedule instead.
func TestBackupRefreshCard_titleSaysWhatItDoes(t *testing.T) {
	js := readAsset(t, "app.js")
	body := jsFunctionBody(t, js, "backupRefreshCard")

	m := regexp.MustCompile(`card-title", text: "([^"]*)"`).FindStringSubmatch(body)
	if m == nil {
		t.Fatal("backupRefreshCard renders no card title; this guard covers nothing")
	}
	title := m[1]
	for _, banned := range []string{"Automatic", "automatic", "Schedul", "schedul", "Refresh", "refresh"} {
		if strings.Contains(title, banned) {
			t.Errorf("the card title %q contains %q: this setting has no timetable in it, and the word "+
				"puts it back beside Scheduled backups, which is the timetable", title, banned)
		}
	}
	if !strings.Contains(strings.ToLower(title), "reuse") {
		t.Errorf("the card title %q does not name what the control does (reuse a file)", title)
	}
	if !strings.Contains(js, `"Scheduled backups: none"`) {
		t.Fatal("the schedule summary is no longer called Scheduled backups; the collision was resolved from " +
			"the wrong side, and this guard would have passed on a renamed timetable")
	}
}

// TestBackupRefreshCard_prose (#1528): a line of help under a control is fine,
// a paragraph means the control explains itself instead of being clear. The
// on-state used to carry the shared-bytes consequence of a hard link in the
// card; that is a thing a reader wants while reading docs, not while flipping
// the switch, so it lives in docs/console.md now.
func TestBackupRefreshCard_prose(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "backupRefreshCard")
	if strings.Contains(body, "share the same bytes on disk") {
		t.Error("the hard-link consequence is back in the card; it belongs in docs/console.md")
	}
	if strings.Contains(body, "—") {
		t.Error("backupRefreshCard copy contains an em dash")
	}
	docs, err := os.ReadFile("../../docs/console.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(docs), "share the same bytes on disk") {
		t.Error("docs/console.md does not carry the shared-bytes consequence, so removing it from the card lost it")
	}
}

// TestBackupScheduleCard_introIsNotAnEssay (#1528): the fold's opening
// paragraph explained the producer choice in general terms directly above the
// line that names the producer for the NEXT run specifically. The general half
// is docs material; the specific half is the state the operator acts on.
func TestBackupScheduleCard_introIsNotAnEssay(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "backupScheduleCard")
	if strings.Contains(body, "with no load on your database") {
		t.Error("the fold explains the producer choice in general above the line that names it for the next run")
	}
	// The specific half must survive the cut.
	if !strings.Contains(body, "will update the latest backup from the recorded changes") {
		t.Fatal("the per-run producer line is gone, so nothing says how the next run will be made")
	}
}
