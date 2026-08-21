package console

import (
	"os"
	"strings"
	"testing"
)

// The Restore "Undo" banner must describe exactly what the generated script
// reverses — and since #1404 that is ONE change, not a window.
//
// The history is the point of the guard. renderRecover's prefill sets `until`
// from the clicked event and never `since`, so the window ran from the
// beginning of time: undoing the third of five changes to a row put it back to
// before the FIRST. #1388 fixed the WORDING to match that behaviour; #1404
// changed the behaviour instead, prefilling `limit_per_pk = 1` so the script
// reverses the latest change at or before the ceiling.
//
// What did not change is the ceiling. `ctx.time` is second-granular
// (consoleTSFormat) and the predicate is `event_timestamp <= ?`, so it is the
// END of that second. On a row touched more than once inside it — a row
// INSERTed and DELETEd in the same second, like WordPress
// `_transient_doing_cron` — the reversal lands on the LAST of them, which need
// not be the one the operator clicked. `since` is no remedy: it parses at the
// same granularity. The banner has to say so.
//
// Three drafts got this wrong in the same direction before the behaviour was
// fixed: "Reverting this row to before this point" named a target state,
// "Undoing every change up to this point" named the clicked event as the
// boundary, and "Undoing every change in this window" was accurate only while
// the whole history really was reversed. Hence a guard.
func TestAppJSUndoBannerStatesWhatIsReversed(t *testing.T) {
	eyebrow, detail := undoBannerText(t)
	all := eyebrow + detail

	// (a) Retired phrasings, including the one that was correct until the
	// behaviour changed under it. Checked against the banner text ONLY — see
	// undoBannerText: a needle in a comment can neither satisfy nor trip these.
	for _, banned := range []struct{ text, why string }{
		{"Reverting this row to before this point", "names a target state the script does not produce"},
		{"up to this point", "names the clicked event as the ceiling; the ceiling is its whole second"},
		{"Undoing every change", "the prefill reverses ONE change now, not every change on the row"},
		{"not only that one", "was the whole-history caveat; with limit_per_pk=1 it is simply false"},
	} {
		if strings.Contains(all, banned.text) {
			t.Errorf("the Undo banner reintroduces %q — %s.\nBanner was: %s", banned.text, banned.why, all)
		}
	}

	// (b) The eyebrow states the singular scope.
	if !strings.Contains(eyebrow, "one change") {
		t.Errorf("the Undo banner eyebrow no longer says it undoes ONE change (%q). With "+
			"limit_per_pk prefilled to 1 that is what the script does, and an eyebrow promising a "+
			"window would over-state the blast radius in the safe direction — which still teaches "+
			"the operator to distrust the banner.", eyebrow)
	}

	// (c) The ceiling is stated. Asserted as source fragments: the sentence is
	// concatenated around ctx.type/ctx.time, so the rendered sentence never
	// appears contiguously in the source.
	for _, want := range []string{"latest change to this row", "end of the second"} {
		if !strings.Contains(detail, want) {
			t.Errorf("the Undo banner detail dropped %q.\nDetail was: %s\n"+
				"Without it a reader cannot tell which change is reversed, or how far the ceiling reaches.", want, detail)
		}
	}

	// (d) The control is named AND its escape hatch is offered. A prefill the
	// banner does not mention is a silent narrowing — the thing #1388 was
	// about not doing.
	if !strings.Contains(detail, "Latest per row is set to 1") {
		t.Error("the Undo banner no longer states that Latest per row was prefilled. The prefill " +
			"changes what the button reverses; leaving it unsaid makes it a silent narrowing.")
	}
	if !strings.Contains(detail, "clear it") {
		t.Error("the Undo banner states the prefill without saying how to undo it. The old " +
			"whole-history behaviour is one cleared field away and the operator has to know that.")
	}

	// (e) The residual imprecision, which no control fixes.
	if !strings.Contains(detail, "more than once inside that second") {
		t.Error("the Undo banner dropped the same-second caveat. With a one-second ceiling the " +
			"reversal lands on the LAST change in that second, which need not be the clicked one — " +
			"that is exactly the case this banner exists for, so the caveat is not optional.")
	}

	// (f) …and the OUTCOME of that imprecision, not only its mechanism. Stating
	// "the last of them is the one reversed" is true and still leaves the
	// reader to derive the inversion: on a row INSERTed and DELETEd inside one
	// second the cap keeps the DELETE, so clicking Undo on the INSERT
	// RE-CREATES the row instead of removing it — the opposite of the intent,
	// with the badge above still reading INSERT. Review found this riding along
	// undisclosed; deriving it is not the operator's job.
	for _, want := range []string{"not necessarily the one you clicked", "comes back"} {
		if !strings.Contains(detail, want) {
			t.Errorf("the Undo banner dropped %q.\nDetail was: %s\n"+
				"Without it the same-second caveat names a mechanism and hides the one outcome "+
				"that is inverted from what the operator asked for.", want, detail)
		}
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
func TestUndoPrefillsLatestPerRow(t *testing.T) {
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

	if !strings.Contains(fn, `form.elements.limit_per_pk.value = "1";`) {
		t.Error("renderRecover's Undo prefill no longer sets limit_per_pk. Without it the window " +
			"has no lower bound and no per-row cap, so the script reverses the row's ENTIRE history " +
			"up to the clicked second — the #1404 defect — while the banner still says one change.")
	}
	// The ceiling has to survive alongside it: limit_per_pk without `until`
	// would reverse the row's most recent change, not the clicked one.
	if !strings.Contains(fn, "form.elements.until.value = ctx.time;") {
		t.Error("renderRecover's Undo prefill no longer sets `until`. Paired with limit_per_pk=1 " +
			"that reverses the row's LATEST change instead of the one the operator clicked.")
	}
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
		{`form.elements.until.value = "";`,
			"a leftover upper bound silently drops the newest damage from the window"},
		// The label, not a field — and it is a claim about the fields above.
		// The banner states "Latest per row is set to 1" as a fact about this
		// form; the first clear in this list makes that false, so leaving it up
		// shows a one-change scope over a script that reverses everything after
		// the instant.
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
