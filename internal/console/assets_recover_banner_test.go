package console

import (
	"os"
	"strings"
	"testing"
)

// The Restore "Undo" banner must describe the WINDOW it reverses, never a
// target state and never a single event.
//
// renderRecover's prefill sets `until` from the clicked event and never
// `since`. `ctx.time` is second-granular (consoleTSFormat) and the predicate
// is `event_timestamp <= ?`, so the ceiling is the END OF THAT SECOND: every
// event on the row up to there is reversed, including ones that happened
// AFTER the clicked event inside the same second.
//
// The failure is not hypothetical and not a corner case. A row INSERTed and
// DELETEd inside one second (WordPress `_transient_doing_cron`) reverses BOTH
// from either entry point, and the net effect is no row at all — correct for
// the window, the opposite of what a banner naming one event announces.
// `since` is no remedy there: it parses at the same granularity.
//
// Two earlier drafts got this wrong in the same direction — "Reverting this
// row to before this point" named a target state, then "Undoing every change
// up to this point" named the clicked event as the boundary. Hence a guard.
func TestAppJSUndoBannerStatesWindowScope(t *testing.T) {
	eyebrow, detail := undoBannerText(t)

	// (a) The retired phrasings must not come back. Checked against the banner
	// text ONLY — see undoBannerText: a needle in a comment can neither
	// satisfy nor trip these.
	for _, banned := range []string{
		"Reverting this row to before this point",
		"undoing changes up to ",
		// Names the clicked event as the ceiling. It is not; the second is.
		"up to this point",
	} {
		if strings.Contains(eyebrow+detail, banned) {
			t.Errorf("the Undo banner reintroduces %q.\n"+
				"That phrasing makes the clicked event (or the state before it) the boundary. The window "+
				"actually closes at the END of that event's second, and every event on the row up to there "+
				"is reversed. State the window.", banned)
		}
	}

	// (b) The eyebrow names a window, not a point.
	if !strings.Contains(eyebrow, "window") {
		t.Errorf("the Undo banner eyebrow no longer says it is undoing a window (%q) — "+
			"without that word it reads as a single-event reversal, which is not what generateUndo does", eyebrow)
	}

	// (c) The detail states the real ceiling and that the clicked event is not
	// alone. Asserted as source fragments: the sentence is concatenated around
	// ctx.type/ctx.time, so the rendered sentence never appears contiguously in
	// the source and searching for it whole would fail on every run.
	for _, want := range []string{"end of the second", "not only that one"} {
		if !strings.Contains(detail, want) {
			t.Errorf("the Undo banner detail dropped %q.\n"+
				"Detail was: %s\nA reader cannot tell how far the reversal reaches without it.", want, detail)
		}
	}

	// (d) Since is offered AND its limit is stated. Offering it alone is worse
	// than silence on the motivating case: an operator narrows the window,
	// gets the identical script, and concludes the tool is broken.
	if !strings.Contains(detail, "Set Since to narrow the window") {
		t.Error("the Undo banner no longer points at Since — stating the window is wide without naming " +
			"the way to narrow it leaves the operator with a warning and no action")
	}
	if !strings.Contains(detail, "second-granular") {
		t.Error("the Undo banner points at Since without saying it cannot split events inside one second. " +
			"That is exactly the case this banner exists for, so the caveat is not optional.")
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
