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
// never matched. Reading the actual `el(...)` calls removes the whole class:
// comments, block or line, are simply not in the haystack.
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

	eyebrow = spanText(t, fn, "ctx-eyebrow")
	// The detail is two spans; take everything from the first to the end of
	// the append so both are covered.
	i := strings.Index(fn, `class: "ctx-detail"`)
	if i < 0 {
		t.Fatal("no ctx-detail span in renderRecover — the Undo banner lost its explanatory line")
	}
	j := strings.Index(fn[i:], "})));")
	if j < 0 {
		t.Fatal("could not find the end of the Undo banner's append")
	}
	return eyebrow, fn[i : i+j]
}

// spanText pulls the literal text of `el("span", { class: "<cls>", text: "…" })`.
func spanText(t *testing.T, fn, cls string) string {
	t.Helper()
	marker := `class: "` + cls + `", text: "`
	i := strings.Index(fn, marker)
	if i < 0 {
		t.Fatalf("no %s span with a literal text in renderRecover", cls)
	}
	rest := fn[i+len(marker):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		t.Fatalf("unterminated text literal on the %s span", cls)
	}
	return rest[:j]
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
	} {
		if !strings.Contains(fn, want.line) {
			t.Errorf("aimUndoAtInstant no longer clears a field the other bridge sets: missing %q.\n%s",
				want.line, want.why)
		}
	}
}
