package console

import (
	"encoding/json"
	"os"
	"path/filepath"
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

// TestBackupsPageStillMountsTheRefreshCard: the card can be unmounted, or its
// fetch removed, with the whole suite green.
//
// The two guards above check what the card renders once it is called. Neither
// notices if nothing calls it: dropping the append makes the setting vanish,
// and dropping the fetch makes it render its error branch forever. A setting
// an operator cannot reach is the same as a setting that does not exist.
//
// It moved from Storage to Backups (#1543), beside the schedule it was being
// confused with, which is the pairing #1528 asked for. The guard follows the
// card rather than the page, and it checks the page it is ON so the move
// cannot be half-done: a card mounted on neither page passes a guard that
// only asks "does some function call it".
func TestBackupsPageStillMountsTheRefreshCard(t *testing.T) {
	js := readAsset(t, "app.js")
	body := jsFunctionBody(t, js, "renderBaselines")
	if !strings.Contains(body, "backupRefreshCard(") {
		t.Error("the Backups page no longer mounts backupRefreshCard, so the reuse setting has no UI at all")
	}
	// Beside the schedule, not somewhere else on the page: the whole reason
	// for the move is that the two controls have to be read together.
	sched, reuse := strings.Index(body, "backupScheduleCard("), strings.Index(body, "backupRefreshCard(")
	if sched < 0 {
		t.Fatal("the schedule card is gone from the Backups page; this guard can no longer check the pairing")
	}
	if reuse < sched && reuse >= 0 {
		t.Error("file reuse is mounted above the schedule; #1528 pairs them in that order so the timetable reads first")
	}
	if !strings.Contains(body, `api("/api/baseline-refresh")`) {
		t.Error("the Backups page does not fetch /api/baseline-refresh, so the card can only ever render its error branch")
	}
}

// The split itself (#1543): Storage held seven cards from five concerns, and
// the two halves answer different questions. A card that drifts back, or a
// half that quietly absorbs the other, is the failure this guards.
func TestStorageSplit_eachHalfHoldsOnlyItsOwnConcern(t *testing.T) {
	js := readAsset(t, "app.js")
	if strings.Contains(js, "function buildStorage(") {
		t.Fatal("buildStorage is back; Storage was split into Retention and This daemon (#1543)")
	}
	for _, tc := range []struct {
		fn    string
		want  []string
		never []string
	}{
		// What happens to your data over time. Nothing about this process.
		{"buildRetention", []string{"rotationCard(", "archivingPanel("},
			[]string{"credentialsCard(", "stagingCard(", "telemetryCard(", "backupRefreshCard(", "duckdbCard("}},
		// What this process is reaching, holding and sending. Nothing about
		// the data lifecycle.
		{"buildDaemon", []string{"credentialsCard(", "stagingCard(", "telemetryCard("},
			[]string{"rotationCard(", "archivingPanel(", "backupRefreshCard(", "duckdbCard("}},
	} {
		body := jsFunctionBody(t, js, tc.fn)
		for _, w := range tc.want {
			if !strings.Contains(body, w) {
				t.Errorf("%s does not mount %s, so that card is unreachable", tc.fn, w)
			}
		}
		for _, n := range tc.never {
			if strings.Contains(body, n) {
				t.Errorf("%s mounts %s, which belongs to the other half; the page is a drawer again", tc.fn, n)
			}
		}
	}
	// The old route must keep resolving, or every existing link and bookmark
	// lands on Overview with no explanation.
	if !strings.Contains(js, `route === "storage"`) {
		t.Error("nothing handles the old /storage route, so existing links break")
	}
	// And the DuckDB schema download has to be mounted SOMEWHERE, not merely
	// absent from the two halves above. Connect since #1549: /sql gated it on
	// a capability and a permission that are not the download's own, so
	// BINTRAIL_CONSOLE_SQL_PANEL=0 left `views` on with no route to it.
	//
	// Named explicitly rather than searched file-wide, because the failure
	// this guards is the card existing while nothing calls it.
	if !strings.Contains(jsFunctionBody(t, js, "buildConnect"), "duckdbCard(") {
		t.Error("buildConnect does not mount duckdbCard, so the schema download is unreachable")
	}
	// And it must not go back to the SQL page, which #1549 removed entirely.
	// Asserted as the page's ABSENCE rather than as "renderSQL does not mount
	// the card": jsFunctionBody fatals on a function it cannot find, so the
	// body form would fail for the wrong reason today and would only start
	// checking anything again if the page came back.
	if strings.Contains(js, "function renderSQL(") {
		t.Error("renderSQL is back in app.js; the SQL page was removed in #1549, and it is what gated this card behind the sql capability")
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
	// It sits on the Backups page beside the schedule (#1543), so the title
	// says which of the two it is and what it costs or saves. Disk is the
	// whole trade: reusing a file means two snapshots share one, so a prune
	// reports space it will not reclaim while the newer one references it.
	low := strings.ToLower(title)
	if !strings.HasPrefix(low, "backups") {
		t.Errorf("the card title %q does not start with Backups, so on the Backups page it does not say "+
			"which control it is", title)
	}
	if !strings.Contains(low, "disk") {
		t.Errorf("the card title %q does not name what the control trades (disk space)", title)
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
	// docsNoWrap: the first version of this read the file raw and went red
	// because the sentence wrapped between two lines.
	if !strings.Contains(docsNoWrap(t), "share the same bytes on disk") {
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

// docsNoWrap returns docs/console.md with every whitespace run collapsed to one
// space, so a needle cannot fail merely because the sentence wrapped between
// two lines. The first version of the guard below broke exactly that way.
func docsNoWrap(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../docs/console.md")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(strings.Fields(string(b)), " ")
}

// TestBackupScheduleCard_saysWhatARunCosts (#1528, review pass 1). Cutting the
// intro paragraph went one clause too far.
//
// With NO schedule saved yet, every per-run line lives inside `if (sch)` and
// renders nothing, so the only thing describing a run was the rate line: "each
// a full copy of every table". True of the OUTPUT and misleading about the
// INPUT, because when a previous local backup exists the run reads the recorded
// changes and never touches the database. An operator typing 30m into an empty
// form was systematically overestimating what they were about to switch on.
//
// So the clause has to sit on the UNCONDITIONAL intro, above the canEdit
// branch, and the full producer rule has to be in the docs. Both halves are
// asserted: the reuse cut is pinned on the docs side too, and an unpinned half
// is what lets a later edit delete the explanation from both places.
func TestBackupScheduleCard_saysWhatARunCosts(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "backupScheduleCard")

	const clause = "When it can, a run is built from the recorded changes"
	i := strings.Index(body, clause)
	if i < 0 {
		t.Fatalf("the intro does not say a run is usually built from the recorded changes, so an empty "+
			"form describes every run as a full copy read from the database:\n%s", body)
	}
	// Unconditional: above the canEdit branch, and therefore above every
	// `if (sch)` line, so it renders on a page with no schedule saved.
	gate := strings.Index(body, "if (!canEdit)")
	if gate < 0 {
		t.Fatal("the canEdit branch is gone; this guard can no longer tell an unconditional line from a gated one")
	}
	if i > gate {
		t.Error("the clause is below the canEdit branch, so the state it exists for (no schedule yet) does not show it")
	}
	if !strings.Contains(body[i:min(len(body), i+220)], "your database") {
		t.Error("the clause does not say what is spared, which is the whole point of stating it")
	}
	// The full rule (which producer runs when, and why) is docs work, and it
	// has to actually be there.
	docs := docsNoWrap(t)
	for _, want := range []string{
		"otherwise the newest backup is **updated from the recorded changes**",
		// The condition that actually decides the producer since #1539. It
		// used to be the S3 destination, which is why this guard asked for
		// "only a full backup uploads" — a sentence the docs must NOT carry
		// any more, because an operator who reads it configures a nightly
		// full read of production to get an off-box copy.
		"a server with no local backup directory gets a **full backup**",
		// The whole of #1539 in the docs. Without this line the page still
		// reads as if an S3 destination meant a nightly full read.
		"reads its previous snapshot straight from the bucket and uploads its result back to the same place",
	} {
		if !strings.Contains(docs, want) {
			t.Errorf("docs/console.md does not carry the producer rule (missing %q), so the cut lost it from both places", want)
		}
	}
	// And the rule it REPLACED must be gone. Stating both would be worse than
	// stating the old one alone: an operator has no way to tell which of two
	// contradicting sentences describes the build they are running.
	// And the rule it REPLACED must be gone from every page that states the
	// producer rule, not only this one: the surviving copies were in
	// dump-and-baseline.md, which the loop above does not read. Paraphrases
	// count, because an operator acts on the meaning.
	//
	// Scoped to docs/ on purpose: CHANGELOG.md narrates the old rule as
	// history, correctly, and a repo-wide grep would ban that too.
	for _, page := range []string{"console.md", "dump-and-baseline.md"} {
		body, err := os.ReadFile(filepath.Join("..", "..", "docs", page))
		if err != nil {
			t.Fatalf("read docs/%s: %v", page, err)
		}
		flat := strings.Join(strings.Fields(string(body)), " ")
		for _, banned := range []string{
			"only a full backup uploads",
			"only a full backup can upload",
			"backups that go to S3 are always full backups",
			"backups that go to S3 are full backups",
		} {
			if strings.Contains(flat, banned) {
				t.Errorf("docs/%s still says %q, which #1539 made false: the scheduled update reads the "+
					"bucket and uploads its result there", page, banned)
			}
		}
	}
}

// jsLineContaining returns the ONE source line of body that holds anchor.
//
// This exists because a byte window around a needle is not a scope. The first
// version of the guard below read body[i-200:i+260] and asked whether "IAM
// role" appeared anywhere in it. It always did: jsFunctionBody BLANKS
// whole-line comments before the brace walk, so the five comment lines above
// the shared-config arm collapse to five empty lines and pull the PRECEDING
// arm ("Using an IAM role (found an EKS service-account role)") into the
// window. Deleting the hedge from the arm under test left the guard green, on
// the pristine tree as well as the mutated one. A guard satisfied by a
// different arm is not a guard.
//
// Even one line is too wide, though, and the second draft of this guard proved
// it: `strings.Contains(line, "profile")` was satisfied by `aws.profile` in the
// arm's own CONDITION, so the assertion that the copy names both probes passed
// while the copy named one. The claim under test is the STRING the arm assigns,
// so that is the scope: everything between `summary = "` and its closing quote.
func jsArmSummary(t *testing.T, body, anchor string) string {
	t.Helper()
	i := strings.Index(body, anchor)
	if i < 0 {
		t.Fatalf("no line contains %q; the arm was renamed or removed, so every assertion below checks nothing:\n%s", anchor, body)
	}
	end := strings.IndexByte(body[i:], '\n')
	if end < 0 {
		end = len(body) - i
	}
	line := body[i : i+end]
	const assign = `summary = "`
	j := strings.Index(line, assign)
	if j < 0 {
		t.Fatalf("the arm at %q assigns no string literal to summary, so there is no claim to check:\n%s", anchor, line)
	}
	rest := line[j+len(assign):]
	k := strings.IndexByte(rest, '"')
	if k < 0 {
		t.Fatalf("unterminated summary literal at %q:\n%s", anchor, line)
	}
	// The returned span is only the FIRST string literal on the line, so
	// anything after its closing quote is invisible to the negative assertions
	// below. Two shapes exploited that and stayed green:
	//
	//	summary = "...hedged..." + " Using credentials from that file.";
	//	summary = "...hedged... \"Using credentials from that file.\"";
	//
	// Neither is contrived: stagingCard, the next function in this file, builds
	// its hint as "lit" + expr + "lit". So the first literal must be the whole
	// right-hand side: the next non-space byte after the closing quote has to be
	// the `;`. An arm that legitimately needs concatenation gets told this guard
	// cannot read it, which is the safe direction to fail.
	//
	// What that does NOT buy, and pass 4 measured it: `;` is exactly what
	// separates two statements, so
	//
	//	summary = "...hedged..."; summary = "Using credentials from ...";
	//
	// satisfies the check, and so does a `summary += " ..."` on the NEXT line
	// (this helper never looks past the anchor's own line). app.js appends to a
	// half-built sentence in eleven places — 13 `+=` sites minus one numeric
	// counter and one URL-path append — including an else-if chain shaped
	// exactly like this one. So this scope proves the hedge is in the RIGHT ARM
	// and nothing more. Keeping the retired sentences OUT is a separate,
	// span-scoped ban in the caller; see the comment there for what that ban
	// does and does not reach.
	tail := strings.TrimLeft(rest[k+1:], " \t")
	if !strings.HasPrefix(tail, ";") {
		t.Fatalf("the arm at %q does not assign ONE plain string literal, so everything after the first "+
			"literal would escape every check below. Give the arm a single literal, or teach this helper "+
			"the new shape:\n%s", anchor, line)
	}
	return rest[:k]
}

// TestCredentialsCard_armsReportWhatWasProbed (#1528, review passes 1 and 2).
//
// The card answers one question: can this daemon reach S3 at all. Two of its
// arms used to answer it with a confident claim the daemon never observed.
//
//   - shared config: selected by `aws.shared_config || aws.profile`, which is
//     TWO independent probes (a file stat and an env var), and it sits below
//     the env-key, ECS and IRSA arms. An EC2 host with an instance role and a
//     region-only ~/.aws/config lands there with no credentials in that file;
//     so does a container with AWS_PROFILE exported and no ~/.aws mounted, and
//     for that one a sentence naming only the file contradicts the card's own
//     "~/.aws config: absent" row two lines below it.
//   - access keys: AccessKeyEnv is AWS_ACCESS_KEY_ID alone. The secret key is
//     never probed, so "Using access keys" is false whenever the ID is
//     exported without it.
//
// Exactly TWO arms are covered here, and saying so is the point: the ECS and
// EKS arms still read "Using an IAM role" from environment-variable presence
// alone, which is weaker evidence than the file stat behind the shared-config
// arm. That is dbtrail#1534, deliberately out of scope for #1528, and the
// comment in credentialsCard has to keep pointing at it. A guard or a comment
// that describes "every arm" as hedged would be the same over-claim the card
// itself is being fixed for.
//
// Each assertion is scoped to the STRING that arm assigns. See jsArmSummary
// for why neither a byte window nor the whole line is narrow enough.
func TestCredentialsCard_armsReportWhatWasProbed(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "credentialsCard")

	shared := jsArmSummary(t, body, "else if (aws.shared_config")
	if strings.Contains(shared, "Using credentials from") {
		t.Error("the shared-config arm still asserts the file is what signs the requests; the daemon only " +
			"observed that the file exists, and an instance role may be doing the work")
	}
	// Both probes named, or the sentence contradicts the Raw signals rows for
	// the shape it does not mention.
	if !strings.Contains(shared, "shared ~/.aws config file") {
		t.Error("the shared-config arm no longer names the file, which is also the string the Playwright " +
			"suite matches this arm by")
	}
	if !strings.Contains(shared, "profile") {
		t.Errorf("the arm is selected by aws.profile too, and names only the file, so with AWS_PROFILE set "+
			"and no ~/.aws present the card contradicts its own \"~/.aws config: absent\" row:\n%s", shared)
	}
	if !strings.Contains(shared, "IAM role") {
		t.Errorf("the shared-config arm does not say an IAM role can still be what signs the requests:\n%s", shared)
	}

	keys := jsArmSummary(t, body, "if (aws.access_key_env)")
	if strings.Contains(keys, "Using access keys") {
		t.Error("the access-key arm asserts the keys are in use; only AWS_ACCESS_KEY_ID was probed")
	}
	if !strings.Contains(keys, "access key ID") {
		t.Errorf("the access-key arm does not say WHICH signal was seen:\n%s", keys)
	}
	if !strings.Contains(keys, "secret") {
		t.Errorf("the access-key arm does not say the secret key was not checked, which is the whole "+
			"difference between what was observed and what it used to claim:\n%s", keys)
	}

	// The Raw signals row under the summary is part of the same claim. Saying
	// access keys PLURAL are "set", two lines below a sentence that says the
	// secret key was not checked, reproduces inside one card the contradiction
	// the shared-config arm was rewritten to remove.
	if strings.Contains(body, `"access keys (env)"`) {
		t.Error(`the Raw signals row is still labelled "access keys (env)" but only AWS_ACCESS_KEY_ID is ` +
			`probed, so it contradicts the summary two lines above it`)
	}
	if !strings.Contains(body, `"access key ID (env)"`) {
		t.Error(`the Raw signals row does not name the one variable that was read (AWS_ACCESS_KEY_ID)`)
	}

	// And the two retired sentences must not appear ANYWHERE in the function.
	//
	// The arm-scoped bans above are SUBSUMED by this one — jsArmSummary returns
	// a sub-slice of the same body, so anything they can see this sees too.
	// They survive only to name WHICH arm regressed; both are t.Error and both
	// fire together. Scope is what makes the POSITIVE assertions meaningful
	// (the hedge has to be in that arm, not merely present somewhere on the
	// card); for a negative, the smaller scope is the hole. Pass 4 built four
	// shapes — a second statement after the `;`, a `summary +=` on the next
	// line, a plain reassignment, a new `else if` below — and every one stayed
	// green under the arm scope alone.
	//
	// Read from jsFunctionSpan, NOT jsFunctionBody. jsFunctionBody cuts each
	// line at its first `//` without knowing whether it is inside a string, and
	// app.js has several code lines with a URL in a literal. Under that view a
	// line like `summary = "https://x — Using credentials from ...";` is
	// truncated before the phrase and this ban passes with the sentence on
	// screen (pass 5 built exactly that). A negative assertion fails OPEN under
	// truncation; a positive one fails closed, which is why the assertions above
	// were never exposed to it.
	//
	// The span still blanks WHOLE-LINE comments, which is what keeps the
	// pristine tree green: the one live occurrence of "Using access keys" is a
	// comment line inside credentialsCard explaining what the arm used to
	// render. A TRAILING comment quoting either phrase would now trip this —
	// noise, not a false pass, and the right direction for a ban.
	//
	// What it does not cover, said plainly so nobody reads it as more: a
	// reworded claim, one assembled from concatenated fragments, or one
	// returned by a helper defined outside this function. No string guard
	// closes those; what it closes is the phrase itself coming back verbatim.
	// Neither string collides with the deliberate "Using an IAM role" in the
	// ECS and EKS arms — those are dbtrail#1534, not retired.
	span := jsFunctionSpan(t, readAsset(t, "app.js"), "credentialsCard")
	for _, retired := range []string{"Using credentials from", "Using access keys"} {
		if strings.Contains(span, retired) {
			t.Errorf("credentialsCard says %q somewhere in its body. That claim was retired in #1528: the "+
				"daemon probes presence, never use, and this is the card an operator opens precisely "+
				"because S3 is not working", retired)
		}
	}
}

// TestCredentialsCard_commentDoesNotOverclaim (#1528 pass 3). The "presence,
// not use" note was hoisted above the whole if/else chain and generalized to
// "every arm below". Two arms below say "Using an IAM role" from env-var
// presence alone. A comment that describes unhedged arms as hedged is the same
// defect as the copy this PR is fixing, one layer down, and it is the thing a
// later reader trusts instead of re-deriving.
//
// Read from the RAW asset on purpose: jsFunctionBody blanks comment lines, so
// the body view cannot see a comment at all.
func TestCredentialsCard_commentDoesNotOverclaim(t *testing.T) {
	js := readAsset(t, "app.js")
	start := strings.Index(js, "function credentialsCard(")
	if start < 0 {
		t.Fatal("credentialsCard is gone; this guard covers nothing")
	}
	end := strings.Index(js[start:], "function stagingCard(")
	if end < 0 {
		t.Fatal("cannot bound credentialsCard's source region")
	}
	region := js[start : start+end]

	if strings.Contains(region, "in every arm below") {
		t.Error("the note claims every arm reports presence rather than use; the ECS and EKS arms still " +
			"say \"Using an IAM role\" from an env var being non-empty")
	}
	if !strings.Contains(region, "#1534") {
		t.Error("the note does not point at the issue tracking the two arms that are still unhedged, so " +
			"the next reader has to rediscover which arms this PR did and did not fix")
	}
}

// TestCredentialsCard_noEmDashPlaceholders: the console's copy rule is plain
// words and no em dashes. kvRow's own empty fallback is a shared helper, but
// these two call sites pass the character in themselves.
func TestCredentialsCard_noEmDashPlaceholders(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "credentialsCard")
	if strings.Contains(body, "—") {
		t.Error("credentialsCard passes an em dash as a placeholder; say \"not set\", like the row above it")
	}
}

// TestDuckDBCard_titleDoesNotPromiseAQuery (#1528, review pass 1). The card
// hands the operator a file that explicitly does NOT run here; /sql is the page
// that runs DuckDB server-side. Two surfaces named for querying, one of which
// only downloads, is the same defect as the backup-refresh title.
//
// The card is NOT merged into /sql: the two are gated by different capabilities
// (views vs sql), so on a daemon with views on and sql off the card would
// become unreachable. Rename only.
//
// The second half is the cross-file pin: the generated views.sql tells a reader
// which control to tick, by NAME. Renaming either side alone points that file
// at a card that does not exist.
func TestDuckDBCard_titleDoesNotPromiseAQuery(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "duckdbCard")
	m := regexp.MustCompile(`card-title", text: "([^"]*)"`).FindStringSubmatch(body)
	if m == nil {
		t.Fatal("duckdbCard renders no card title; this guard covers nothing")
	}
	title := m[1]
	if strings.Contains(title, "Query in") {
		t.Errorf("the card title %q promises a query that happens somewhere else; it downloads a schema file", title)
	}
	if !strings.Contains(title, "Download") {
		t.Errorf("the card title %q does not name what the control does (download a file)", title)
	}
	if !strings.Contains(liveLegHowTo, title) {
		t.Errorf("the generated views.sql points a reader at %q, which is not this card's title (%q); "+
			"renaming one side alone sends them looking for a control that does not exist",
			liveLegHowTo, title)
	}
}
