package console

import (
	"os"
	"strings"
	"testing"
)

// The Restore "Undo" banner must describe exactly what the generated script
// reverses — and since #1411 that is ONE NAMED EVENT, not a window and not
// "the last change in a second".
//
// The history is the point of the guard, and it has three layers. renderRecover
// used to set `until` from the clicked event and never `since`, so the window
// ran from the beginning of time: undoing the third of five changes to a row
// put it back to before the FIRST. #1388 fixed the WORDING to match that.
// #1404 changed the BEHAVIOUR instead, prefilling `limit_per_pk = 1`.
//
// That left one event but not necessarily the CLICKED one. `ctx.time` is
// second-granular and the predicate was `event_timestamp <= ?`, so the cap kept
// the last event in that whole second. On a row INSERTed and DELETEd inside one
// second — WordPress `_transient_doing_cron` is the everyday case — that is the
// DELETE whichever row was clicked, and reversing a DELETE re-creates the row:
// clicking Undo on the INSERT put the row BACK, with the badge still reading
// INSERT. The banner disclosed it in words, which is why the previous version
// of this guard REQUIRED the caveat.
//
// #1411 removed the ambiguity instead: the prefill carries the server's own
// `<RFC3339Nano>|<event_id>` token for the clicked row, the engine filters on
// that identity, and the caveat became false. So the sentences this guard used
// to require are now BANNED, and the guard asserts the opposite claim. That
// inversion is the reason it is spelled out here: a reader finding the old
// caveat in git history must be able to see it was retired deliberately, not
// lost.
//
// Four drafts got the wording wrong before the behaviour was fixed:
// "Reverting this row to before this point" named a target state, "Undoing
// every change up to this point" named the clicked event as the boundary,
// "Undoing every change in this window" was accurate only while the whole
// history was reversed, and the same-second caveat was accurate only while the
// scope was a second. Hence a guard.
func TestAppJSUndoBannerStatesWhatIsReversed(t *testing.T) {
	eyebrow, detail := undoBannerText(t)
	all := eyebrow + detail

	// (a) Retired phrasings, each of which was correct when written and was
	// falsified by a later change to the behaviour. Checked against the banner
	// text ONLY — see undoBannerText: a needle in a comment can neither satisfy
	// nor trip these, which is what lets the paragraphs above quote them.
	for _, banned := range []struct{ text, why string }{
		{"Reverting this row to before this point", "names a target state the script does not produce"},
		{"up to this point", "names the clicked event as the ceiling; the ceiling was its whole second"},
		{"Undoing every change", "the prefill reverses ONE change, not every change on the row"},
		{"not only that one", "was the whole-history caveat; false since the scope became one event"},
		{"more than once inside that second", "was the same-second caveat; the anchor names the event, so the second no longer decides"},
		{"not necessarily the one you clicked", "was true of the second-granular ceiling and is now the exact opposite of what happens"},
		{"comes back rather than going away", "described the inverted outcome the anchor removed"},
		{"Latest per row is set to 1", "the prefill no longer sets it; a banner naming a control it did not touch sends the operator to clear the wrong field"},
	} {
		if strings.Contains(all, banned.text) {
			t.Errorf("the Undo banner reintroduces %q — %s.\nBanner was: %s", banned.text, banned.why, all)
		}
	}

	// (b) The eyebrow states the singular scope.
	if !strings.Contains(eyebrow, "one change") {
		t.Errorf("the Undo banner eyebrow no longer says it undoes ONE change (%q). That is what "+
			"the script does, and an eyebrow promising a window would over-state the blast radius "+
			"in the safe direction — which still teaches the operator to distrust the banner.", eyebrow)
	}

	// (c) The detail makes the IDENTITY claim, which is the one thing the
	// operator cannot verify by reading the form: `until` is visible and says
	// "that second", so without this sentence the visible fields under-describe
	// the actual scope and the banner reads as the looser of the two.
	//
	// Asserted as source fragments: the sentence is concatenated around
	// ctx.type/ctx.time, so the rendered sentence never appears contiguously.
	for _, want := range []string{"exactly this", "the one you clicked"} {
		if !strings.Contains(detail, want) {
			t.Errorf("the Undo banner detail dropped %q.\nDetail was: %s\n"+
				"Without it the banner does not distinguish the clicked event from everything "+
				"else in its second, which is the distinction #1411 exists to make.", want, detail)
		}
	}

	// (d) …and it says what is NOT reversed. "One change" alone is ambiguous
	// between "one change of this row's history" and "one change among those
	// sharing this second", and the two differ on exactly the shape that used
	// to invert.
	for _, want := range []string{"history", "that second"} {
		if !strings.Contains(detail, want) {
			t.Errorf("the Undo banner detail dropped %q.\nDetail was: %s\n"+
				"The scope is stated by exclusion — neither the rest of the row's history nor "+
				"the other changes in the clicked second — and dropping half of that leaves the "+
				"remaining half readable as the wrong one.", want, detail)
		}
	}

	// (e) The escape hatch is offered. An anchor the banner does not mention is
	// a silent narrowing — the thing #1388 was about not doing — and it is
	// narrower than any control visible in the form, so an operator who wants
	// the row's history has no field to clear and no way to guess.
	if !strings.Contains(detail, "Clear") {
		t.Error("the Undo banner no longer points at Clear. The anchor is not a visible form " +
			"field, so an operator who wants to widen the scope has nothing to edit and no way " +
			"to learn that the button beside the banner is the way out.")
	}
}

// The other half of the retirement, and it fails SILENTLY on its own: the
// removal above looks up "undo-ctx-banner", so dropping the id from the node
// leaves getElementById returning null, the `if` short-circuiting, and the
// banner surviving with nothing anywhere reporting it. Review demonstrated
// exactly that mutation passing the whole package.
//
// The id is checked, not the class: generateUndo appends a SECOND .ctx-banner
// into #recover-out for the cascade notice, so an unscoped removal would take
// whichever came first.
func TestUndoBannerCarriesTheIdItIsRetiredBy(t *testing.T) {
	data, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(data)
	start := strings.Index(js, "function renderRecover(")
	if start < 0 {
		t.Fatal("renderRecover is gone from assets/app.js — this guard covers nothing")
	}
	end := strings.Index(js[start:], "\nfunction ")
	if end < 0 {
		end = len(js) - start
	}
	fn := stripJSCommentLines(js[start : start+end])
	if !strings.Contains(fn, `id: "undo-ctx-banner"`) {
		t.Error(`the Undo banner no longer carries id: "undo-ctx-banner". aimUndoAtInstant removes ` +
			"it by that id when it replaces the scope the banner describes; without it that removal " +
			"is a no-op and the stale banner stays on screen, with no error anywhere.")
	}
}

// The banner is prose; this pins the behaviour it describes. They fail to
// different edits — dropping the prefill leaves a banner promising something
// the script no longer does, and nothing throws either way.
func TestUndoPrefillsTheEventAnchor(t *testing.T) {
	fn := functionSource(t, "renderRecover")

	if !strings.Contains(fn, "form.elements.event.value = ctx.anchor;") {
		t.Error("renderRecover's Undo prefill no longer carries the event anchor. Without it the " +
			"request falls back to `until` alone — the window has no lower bound and no per-row " +
			"cap, so the script reverses the row's ENTIRE history up to the clicked second (the " +
			"#1404 defect) while the banner says one change.")
	}
	// The ceiling has to survive alongside it, for two reasons that outlive the
	// anchor: it is what the operator reads to know WHEN, and it is the scope
	// left behind when they press Clear. Without it, Clear widens to the whole
	// index in one click.
	if !strings.Contains(fn, "form.elements.until.value = ctx.time;") {
		t.Error("renderRecover's Undo prefill no longer sets `until`. It is what the operator " +
			"reads to know when, and what bounds the scope after Clear removes the anchor.")
	}
	// And the cap must NOT come back. Two mechanisms narrowing the same scope
	// is how they drift: the anchor already admits one event, so a cap of 1
	// changes nothing until someone clears the anchor — at which point it
	// silently reinstates the #1404 behaviour under a banner that no longer
	// mentions it.
	if strings.Contains(fn, `form.elements.limit_per_pk.value = "1";`) {
		t.Error("renderRecover's Undo prefill sets limit_per_pk again. The anchor already names " +
			"one event; a second narrowing mechanism is invisible in the banner and reappears " +
			"the moment the anchor is cleared.")
	}
}

// The anchor is only as good as the identity it carries, and the failure mode
// is silent in the worst way: `e.anchor` reaching the bridge as undefined
// leaves the hidden field empty, the request unanchored, and the script back to
// reversing the row's whole history up to the clicked second — with the new
// banner claiming it reversed exactly one event.
//
// Pinned as the FIELD, not the value: rebuilding the token client-side from
// `e.event_timestamp` is the specific mistake this guards. That timestamp is
// second-granular and offset-less, so a reconstruction guesses a location, and
// guessing wrong does not fail — it names a different row.
func TestUndoBridgeCarriesTheServersAnchorToken(t *testing.T) {
	fn := functionSource(t, "undoEvent")
	if !strings.Contains(fn, "anchor: e.anchor") {
		t.Error("undoEvent no longer copies the server's anchor token onto pendingRecover. " +
			"Without it the prefill has nothing to set, the request is unanchored, and the " +
			"banner's identity claim becomes false with nothing reporting it.")
	}
}

// Both request builders must send the anchor, and the second one is the easy
// half to forget: previewRecover promises the preview lists the events the
// script will reverse. Unmirrored, the preview shows every event in the clicked
// second while the script reverses one of them — the operator reviews a list
// that is not what they are about to apply.
func TestBothRecoverRequestsCarryTheAnchor(t *testing.T) {
	for _, fnName := range []string{"generateUndo", "previewRecover"} {
		fn := functionSource(t, fnName)
		if !strings.Contains(fn, `"event"`) {
			t.Errorf("%s no longer sends the event anchor. The preview and the generated script "+
				"read the same form and must apply the same filters, or the list the operator "+
				"reviews is not the one the script acts on.", fnName)
		}
	}
}

// Editing the target must retire the anchor. It names one event of one row, so
// after a PK change it names the wrong row's event and the request comes back
// empty — a 200 with no statements, which reads as "this row has no history".
//
// Pinned because the narrowing is invisible: it moved from a field the banner
// named into a hidden one nothing names, so nothing on screen contradicts a
// stale anchor and no error is produced.
func TestEditingTheTargetRetiresTheUndoAnchor(t *testing.T) {
	fn := functionSource(t, "renderRecover")
	for _, want := range []string{`["schema", "table", "pk"]`, "clearUndoAnchor(form)"} {
		if !strings.Contains(fn, want) {
			t.Errorf("renderRecover no longer retires the anchor when the target changes: missing %q.\n"+
				"A stale anchor names the previous row's event, and the empty result reads as "+
				"'nothing to undo' rather than as a leftover filter.", want)
		}
	}
	// The retirement itself must clear the field AND the banner. Clearing only
	// the field leaves a banner claiming a single-event scope over a form that
	// no longer has one; clearing only the banner leaves the scope in place
	// with nothing describing it.
	clear := functionSource(t, "clearUndoAnchor")
	for _, want := range []struct{ line, why string }{
		{`form.elements.event.value = "";`, "the anchor stays on the wire"},
		{"pendingRecover = null;", "renderRecover rebuilds the banner from it on the next render"},
		{`document.getElementById("undo-ctx-banner")`, "the banner outlives the scope it describes"},
	} {
		if !strings.Contains(clear, want.line) {
			t.Errorf("clearUndoAnchor is missing %q — %s", want.line, want.why)
		}
	}
	// …and it must NOT clear `until`: that is the scope the operator is left
	// with, and blanking it widens a retargeted search to the whole index.
	if strings.Contains(clear, `form.elements.until.value = ""`) {
		t.Error("clearUndoAnchor blanks `until`. Retiring the single-event selection should leave " +
			"the visible window standing, not widen the search to the whole index.")
	}
}

// The banner's Clear button must retire the SELECTION, not rebuild the form.
//
// It used to navigate("recover"), which re-renders the route into a fresh empty
// form — so the one control labelled Clear wiped the target and the upper bound
// while the sentence beside it promised "search this row freely… the time you
// clicked stays as the upper bound". The mechanism that sentence describes
// existed as clearUndoAnchor and had no name in the UI.
//
// Pinned as the handler, because the copy guard cannot see this: it only
// requires the substring "Clear" in the detail, which the false sentence
// satisfied perfectly.
func TestBannerClearRetiresTheSelectionNotTheForm(t *testing.T) {
	fn := functionSource(t, "renderRecover")
	if !strings.Contains(fn, "onclick: () => clearUndoAnchor(form)") {
		t.Error("the banner's Clear button no longer calls clearUndoAnchor. Any handler that " +
			"re-renders the route builds a NEW empty form, which contradicts the banner's own " +
			"promise that the target and the upper bound survive.")
	}
	if strings.Contains(fn, `pendingRecover = null; navigate("recover")`) {
		t.Error("the banner's Clear button navigates again, wiping the target and the upper " +
			"bound the banner says it keeps.")
	}
}

// The busy modal is the only place an operator can see the anchor, because it
// has no visible field. Itemising every other filter and omitting the one that
// most determines the scope is how a request becomes unreviewable.
func TestBusyFactsItemiseTheAnchor(t *testing.T) {
	fn := functionSource(t, "recoverBusyFacts")
	if !strings.Contains(fn, "f.event") {
		t.Error("recoverBusyFacts omits the event anchor. It is the narrowest filter on the " +
			"request and the only one with no field on screen, so the busy modal is the only " +
			"place it can be reviewed before the script is generated.")
	}
}

// functionSource returns the body of a top-level function in app.js with
// whole-line comments stripped, bounded to that function.
func functionSource(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(data)
	start := strings.Index(js, "function "+name+"(")
	if start < 0 {
		t.Fatalf("%s is gone from assets/app.js — this guard covers nothing", name)
	}
	end := strings.Index(js[start:], "\nfunction ")
	if end < 0 {
		end = len(js) - start
	}
	return stripJSCommentLines(js[start : start+end])
}

// undoBannerText returns the eyebrow text and the concatenated detail source
// from the Undo banner in renderRecover.
//
// Scoped extraction rather than a file-wide substring search. A previous
// version searched all 5000+ lines with `//` comments stripped, which was
// vacuous in both directions: a required needle could be satisfied by a
// TRAILING comment on a gutted line (the strip only dropped whole-line
// comments), and the stated reason for stripping at all was false — the
// explanation it claimed to avoid firing on is lowercase and line-split, so it
// never matched.
//
// Reading the `el(...)` calls was the right idea and the first version of it
// only half-delivered: the eyebrow was parsed as a literal, but the DETAIL was
// returned as a raw source slice from the first ctx-detail span to the end of
// the append — so anything between the two spans, comments included, was back
// in the haystack. That failed in both directions, and both were reproduced:
// moving the required sentences into a comment above a gutted span kept the
// guard GREEN over a banner that stated none of them, and adding one accurate
// historical comment naming the old wording turned it RED over a correct
// banner.
//
// So the haystack is now STRING LITERALS ONLY. A comment cannot appear inside
// one, which retires the class rather than narrowing it — and a comment inside
// a text expression, where a quote WOULD be scanned, is refused outright
// instead of guessed at.
func undoBannerText(t *testing.T) (eyebrow, detail string) {
	t.Helper()
	data, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(data)

	// The banner lives in renderRecover; bound the search there so the cascade
	// panel's own ctx-eyebrow cannot be mistaken for it.
	start := strings.Index(js, "function renderRecover(")
	if start < 0 {
		t.Fatal("renderRecover is gone from assets/app.js — this guard covers nothing")
	}
	end := strings.Index(js[start:], "\nfunction ")
	if end < 0 {
		end = len(js) - start
	}
	fn := js[start : start+end]

	eyebrows := spanLiterals(t, fn, "ctx-eyebrow")
	if len(eyebrows) == 0 {
		t.Fatal("no ctx-eyebrow span in renderRecover — the Undo banner lost its heading")
	}
	details := spanLiterals(t, fn, "ctx-detail")
	if len(details) == 0 {
		t.Fatal("no ctx-detail span in renderRecover — the Undo banner lost its explanatory line")
	}
	return eyebrows[0], strings.Join(details, " ")
}

// spanLiterals returns, for every `el("span", { class: "<cls>", text: <expr> })`
// in fn, the concatenation of the STRING LITERALS in that span's text
// expression — dropping the interpolated identifiers, and structurally unable
// to pick up a comment.
//
// Literal-only is the whole point. The first detail span reads
// `"… holding this " + ctx.type + " (" + ctx.time + " UTC)"`, so a helper that
// stops at the first closing quote truncates it, and one that slices raw
// source to the end of the append swallows every comment in between. This
// walks each text expression consuming quoted runs atomically.
//
// It stops at the end of the EXPRESSION — a top-level comma or closing brace —
// not merely at the next `}`. The first version stopped at `}` alone, and
// review showed that is not the same thing: the scan ran on past `text:` into
// every later literal of the same object, so moving the required sentences
// into a sibling `title:` (which el() sets as an attribute, not as visible
// text) kept the guard GREEN over a banner reading "See tooltip." — the exact
// class the literal-only rewrite was supposed to retire, just one property
// over. A probe with `text:` written before `class:` made the reach visible:
// the extracted "detail" picked up `text: "Clear"` from the button below AND
// the "recover" literal from inside its onclick arrow body, because `{` never
// stopped the scan.
func spanLiterals(t *testing.T, fn, cls string) []string {
	t.Helper()
	marker := `class: "` + cls + `"`
	var out []string
	for pos := 0; ; {
		i := strings.Index(fn[pos:], marker)
		if i < 0 {
			return out
		}
		i += pos
		k := strings.Index(fn[i:], "text:")
		if k < 0 {
			t.Fatalf("%s span at offset %d has no text: — this guard reads span text and found none", cls, i)
		}
		p := i + k + len("text:")
		var b strings.Builder
		depth := 0
	scan:
		for p < len(fn) {
			switch fn[p] {
			case '"':
				lit, next := readJSString(t, fn, p)
				b.WriteString(lit)
				p = next
			case '(', '[':
				depth++
				p++
			case ')', ']':
				depth--
				p++
			case ',':
				// Top-level comma ends the property's value. Inside a call's
				// argument list it does not, hence the depth counter.
				if depth <= 0 {
					break scan
				}
				p++
			case '{':
				depth++
				p++
			case '}':
				if depth <= 0 {
					break scan
				}
				depth--
				p++
			case '/':
				// A comment inside the text expression would put its quoted
				// content into the haystack, which is exactly the defect this
				// helper exists to retire. Refuse rather than guess.
				if p+1 < len(fn) && (fn[p+1] == '/' || fn[p+1] == '*') {
					t.Fatalf("a comment sits inside the %s span's text expression; move it above the "+
						"el(...) call. Left here it lands in the guard's haystack and the guard "+
						"stops meaning what it says.", cls)
				}
				p++
			default:
				p++
			}
		}
		out = append(out, b.String())
		pos = p
	}
}

// readJSString consumes the double-quoted run starting at i and returns its
// contents plus the offset just past the closing quote.
func readJSString(t *testing.T, s string, i int) (string, int) {
	t.Helper()
	var b strings.Builder
	for p := i + 1; p < len(s); p++ {
		switch s[p] {
		case '\\':
			if p+1 < len(s) {
				b.WriteByte(s[p+1])
				p++
			}
		case '"':
			return b.String(), p + 1
		default:
			b.WriteByte(s[p])
		}
	}
	t.Fatalf("unterminated string literal at offset %d in renderRecover", i)
	return "", len(s)
}

// The two bridges set opposite scopes on the SAME form, and #1404 is what put
// them in conflict: Undo now prefills a per-row cap of 1, while
// "Restore to this state" reverses every change after an instant — which is
// the only reason the row lands on the state the button names.
//
// An operator who uses Undo and then Restore-to-this-state arrives with the
// leftover cap. The result is a row left in a state nobody asked for, with no
// error and a button still naming the state it did not produce. This is the
// same class as the leftover `until` the function already clears, and it is
// pinned because the two clears look like boilerplate and read as removable.
func TestRestoreToStateClearsTheUndoBridgesPerRowCap(t *testing.T) {
	data, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(data)
	start := strings.Index(js, "function aimUndoAtInstant(")
	if start < 0 {
		t.Fatal("aimUndoAtInstant is gone from assets/app.js — this guard covers nothing")
	}
	end := strings.Index(js[start:], "\nfunction ")
	if end < 0 {
		end = len(js) - start
	}
	fn := stripJSCommentLines(js[start : start+end])

	for _, want := range []struct{ line, why string }{
		{`form.elements.limit_per_pk.value = "";`,
			"a cap inherited from the Undo bridge reverses only the newest change after the instant, so the row does not land on the state shown"},
		// The strongest version of the same class, and the one that fails
		// hardest: an anchor names ONE event, while this action reverses every
		// change after the instant. Left set, the script reverses the event the
		// operator clicked in Events minutes ago and nothing else, under a
		// button naming a completely different outcome.
		{`form.elements.event.value = "";`,
			"an anchor inherited from the Undo bridge pins the script to one old event instead of everything after the instant"},
		{`form.elements.until.value = "";`,
			"a leftover upper bound silently drops the newest damage from the window"},
		// The label, not a field — and it is a claim about the fields above.
		// The banner asserts that exactly the clicked event is reversed; the
		// clears in this list make that false, so leaving it up shows a
		// one-event scope over a script that reverses everything after the
		// instant. (The same contradiction ran through the cap before #1411,
		// when the banner read "Latest per row is set to 1" — a string this
		// file now BANS at the top, which is why it is described in the past
		// tense here rather than quoted as current.)
		{`pendingRecover = null;`,
			"renderRecover rebuilds the banner from pendingRecover, so leaving it set puts the contradicting banner back on the next render"},
		{`document.getElementById("undo-ctx-banner")`,
			"the banner already on screen is not removed, so it outlives the scope it describes"},
	} {
		if !strings.Contains(fn, want.line) {
			t.Errorf("aimUndoAtInstant no longer clears a field the other bridge sets: missing %q.\n%s",
				want.line, want.why)
		}
	}
}
