package console

import (
	"os"
	"strings"
	"testing"
)

// The Restore "Undo" banner must describe the SCOPE of the reversal, never a
// target state. renderRecover's prefill sets `until` from the clicked event
// and never `since`, so generateUndo reverses every event on that row in an
// unbounded window: the row lands where it was when the window opened. That
// equals "the state just before this event" only when the event is the sole
// one in range.
//
// The wording this pins replaced "Reverting this row to before this point",
// which promised the single-event reading. The failure it hid is not
// hypothetical: a row INSERTed and DELETEd inside the same second (WordPress
// `_transient_doing_cron`) yields two reversals whose net effect is no row at
// all — correct for the window, the opposite of what the banner announced.
// Timestamps cannot separate same-second events, so `since` is no remedy
// there; that is what exposing limit-per-pk on this surface is for.
//
// Comments are stripped before matching. The explanation above the banner in
// app.js quotes the old wording, and served bytes include comments — matching
// raw source would let the retired phrasing pass as long as someone left it in
// a comment, and would fire on the explanation itself.
func TestAppJSUndoBannerStatesWindowScope(t *testing.T) {
	data, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := stripLineComments(string(data))

	// (a) The retired promise must not come back. It is a single unsplit
	// literal in the source it replaced, so this catches a straight revert.
	for _, banned := range []string{
		"Reverting this row to before this point",
		"undoing changes up to ",
	} {
		if strings.Contains(js, banned) {
			t.Errorf("assets/app.js reintroduces %q in the Undo banner.\n"+
				"That phrasing promises the row is restored to its state just before the clicked event, "+
				"but the prefill leaves `since` empty, so EVERY event on the row in the window is reversed. "+
				"State the scope instead.", banned)
		}
	}

	// (b) The eyebrow names the scope.
	if !strings.Contains(js, "Undoing every change up to this point") {
		t.Error("the Undo banner eyebrow no longer states that every change up to the clicked event is undone — " +
			"without it the banner implies a single-event reversal that generateUndo does not perform")
	}

	// (c) The detail says the reversal is not limited to the clicked event.
	// Asserted as the literal source fragment: the sentence is built by
	// concatenation around ctx.type/ctx.time, so the whole sentence never
	// appears contiguously and searching for it would pass vacuously.
	if !strings.Contains(js, "not only that one") {
		t.Error("the Undo banner detail dropped the clause distinguishing the whole window from the clicked event; " +
			"a reader cannot tell how many events the generated script will reverse")
	}

	// (d) The remedy stays actionable and names a control that exists on this
	// form. If the prefill ever bounds the window itself, this pairing is what
	// should force the wording to be revisited.
	if !strings.Contains(js, "set Since to narrow the window") {
		t.Error("the Undo banner no longer points at Since — stating the window is wide without naming the way to narrow it " +
			"leaves the operator with a warning and no action")
	}
}

// stripLineComments removes whole-line // comments, leaving code and string
// literals intact. Deliberately not a JS parser: it only has to keep the
// explanatory comments in app.js out of substring assertions, and a line whose
// trimmed form starts with "//" is never code.
func stripLineComments(js string) string {
	lines := strings.Split(js, "\n")
	kept := lines[:0]
	for _, ln := range lines {
		if strings.HasPrefix(strings.TrimSpace(ln), "//") {
			continue
		}
		kept = append(kept, ln)
	}
	return strings.Join(kept, "\n")
}
