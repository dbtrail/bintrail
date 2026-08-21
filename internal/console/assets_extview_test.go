package console

import (
	"regexp"
	"strings"
	"testing"
)

// extRenderCall returns the text of app.js's `mod.render(...)` call — the
// contract handed to an extension view — and the file line it starts on.
//
// The CALL is extracted rather than the file scanned, and that is load-bearing
// rather than tidy. The comment directly above it explains the contract and
// therefore names every token this file greps for; a whole-file match would
// pass with the call gutted and the needles left sitting in the prose. Whole
// line comments inside the extracted region are blanked as a belt, since the
// region will grow.
func extContractBody(t *testing.T) (string, int) {
	t.Helper()
	js := readAsset(t, "app.js")
	i := strings.Index(js, "function extContract(")
	if i < 0 {
		t.Fatal("app.js: no extContract( — the extension contract is gone or was renamed. Both " +
			"extension surfaces build their argument here; if it moved, this guard checks nothing.")
	}
	open := strings.IndexByte(js[i:], '{')
	if open < 0 {
		t.Fatalf("app.js:%d: extContract has no body", lineOf(js, i))
	}
	depth, end := 0, -1
	for k := i + open; k < len(js); k++ {
		switch js[k] {
		case '{':
			depth++
		case '}':
			if depth--; depth == 0 {
				end = k
			}
		}
		if end >= 0 {
			break
		}
	}
	if end < 0 {
		t.Fatalf("app.js:%d: unbalanced braces in extContract", lineOf(js, i))
	}
	// stripJSCommentLines is line-oriented, so a TRAILING comment inside the
	// body survives it — and a needle parked in one would satisfy every check
	// below over a gutted contract. The sibling helper was hardened against the
	// same shape; this region is a small object literal with no regex literals
	// in it, so cutting at `//` is safe here in a way it is not file-wide.
	body := stripJSCommentLines(js[i+open : end+1])
	var out []string
	for _, ln := range strings.Split(body, "\n") {
		if c := strings.Index(ln, "//"); c >= 0 {
			ln = ln[:c]
		}
		out = append(out, ln)
	}
	return strings.Join(out, "\n"), lineOf(js, i)
}

// bothSurfacesUseTheContract pins that neither extension loader hand-rolls its
// own argument. A surface that stopped calling extContract would keep working
// and silently lose whatever the contract later grows.
func bothSurfacesUseTheContract(t *testing.T) {
	t.Helper()
	js := stripJSCommentLines(readAsset(t, "app.js"))
	calls := regexp.MustCompile(`mod\.render\(`).FindAllStringIndex(js, -1)
	if len(calls) != 2 {
		t.Fatalf("app.js: expected 2 mod.render( call sites (settings panel + view), found %d. "+
			"A new extension surface must build its argument with extContract too — and then this "+
			"number needs bumping, which is the point of pinning it: nothing else in the repo "+
			"notices a surface being added or removed. Two shapes are NOT counted and are stated "+
			"rather than guarded: an aliased render reference (`const r = mod.render`), and a "+
			"module binding named anything but `mod` — this matches the literal `mod.render(`, so "+
			"`module.render(...)` is invisible to it rather than merely uncounted.", len(calls))
	}
	for _, c := range calls {
		// The call's own parens, not the rest of the line. Two reasons, both
		// demonstrated: a trailing `// was extContract(...)` satisfied a
		// line-wide search over a fully hand-rolled argument, and a correct
		// call wrapped across lines was accused of being one.
		depth, end := 0, -1
		for k := c[1] - 1; k < len(js); k++ {
			switch js[k] {
			case '(':
				depth++
			case ')':
				if depth--; depth == 0 {
					end = k
				}
			}
			if end >= 0 {
				break
			}
		}
		if end < 0 {
			t.Fatalf("app.js:%d: unbalanced parens in this mod.render call", lineOf(js, c[0]))
		}
		if !strings.Contains(js[c[0]:end+1], "extContract(") {
			t.Errorf("app.js:%d: this extension surface hand-rolls its render argument instead of "+
				"calling extContract. The two surfaces would then drift, and only the extension "+
				"built against the richer one would notice. Note this catches a REPLACED argument, "+
				"not a spread that then strips keys; console-e2e's ext-view and ext-settings legs "+
				"cover the shape an extension actually receives, on both surfaces.", lineOf(js, c[0]))
		}
	}
}

// An extension view is built in a different repo on its own release cadence.
// Whatever the console hands it is a promise; whatever it does not is an
// internal. app.js is a classic script, so an extension COULD reach a widget
// as a window global — and nothing would catch a rename, because the two sides
// never compile together. So the widgets that ARE shared travel through the
// contract, and this pins them there.
func TestExtensionViewContractCarriesTheSharedWidgets(t *testing.T) {
	bothSurfacesUseTheContract(t)
	call, line := extContractBody(t)

	for _, want := range []struct{ key, why string }{
		{"apiBase", "the data plane's base path"},
		{"api", "the console's authed fetch"},
		{"ui", "the shared-widget namespace"},
	} {
		if !regexp.MustCompile(`\b` + want.key + `\b`).MatchString(call) {
			t.Errorf("app.js:%d: the extension-view contract no longer passes %q (%s). Removing a key "+
				"from this call breaks an extension built against it, and the break is a MISSING "+
				"widget rather than an error — nothing throws.", line, want.key, want.why)
		}
	}

	// Bound to the builder itself, not to a wrapper: a wrapper is a second
	// spelling that a rename can leave pointing somewhere else, which is the
	// whole failure this guard exists for.
	if !regexp.MustCompile(`dateField\s*:\s*fieldDateInput\b`).MatchString(call) {
		t.Errorf("app.js:%d: ui.dateField is not bound to fieldDateInput. The extension view's date "+
			"fields come from here; bind the builder directly so a rename cannot leave the two "+
			"spellings pointing at different widgets.", line)
	}
}

// The shared widget has to stay the console's OWN widget. Handing an extension
// a builder the console itself stopped using would let the two drift while
// every check above stays green — the operator would see one date picker in
// Restore and a different one in the extension, which is the outcome sharing
// it was meant to prevent.
func TestSharedDateFieldIsTheOneTheConsoleUsesItself(t *testing.T) {
	js := stripJSCommentLines(readAsset(t, "app.js"))

	if !strings.Contains(js, "function fieldDateInput(") {
		t.Fatal("app.js: fieldDateInput is not declared — the extension contract points at nothing")
	}
	// Not preceded by `.`, so `window.fieldDateInput(` or any other qualified
	// reference is not miscounted as one of the console's own uses.
	uses := regexp.MustCompile(`(^|[^.\w])fieldDateInput\(`).FindAllStringIndex(js, -1)
	own := 0
	for _, u := range uses {
		if !strings.HasSuffix(js[:u[1]-len("fieldDateInput(")], "function ") {
			own++
		}
	}
	// Pinned exactly, not floored. A floor of two tolerated migrating four of
	// the six away — a third of the widget's uses gone with the guard silent —
	// and the drift this exists to catch ("one date picker in Restore, another
	// in the extension") starts at the FIRST one, not the fifth. An exact pin
	// makes a deliberate change a one-line acknowledged edit instead of
	// something that slides past.
	const wantOwn = 6
	if own != wantOwn {
		t.Errorf("app.js: fieldDateInput has %d call site(s) in the console's own views, expected %d. "+
			"It is the widget handed to extension views through ui.dateField; if the console stops "+
			"using it the two surfaces drift and only the extension notices. If the change was "+
			"deliberate, update this number — that edit is the acknowledgement.", own, wantOwn)
	}
}
