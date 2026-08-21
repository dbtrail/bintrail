package console

import (
	"regexp"
	"strings"
	"testing"
)

// Time-travel's "Show state" / "Show history" results render in a dialog
// (#1405), not in the strip between the filter form and the reversal panel.
//
// The e2e proves this against a real browser, and that is the primary guard.
// These are the parts of the claim a browserless run can still hold, and they
// are the parts most likely to be undone by accident: the inline containers
// still EXIST for form validation, so re-pointing a result back at them is a
// one-word edit that nothing else notices.
//
// The division of labour was measured, not assumed. Renaming only the
// DECLARATION of the dialog's warnings container — leaving its two references
// dangling — keeps every check here green: JS has no compile-time reference
// check, `node --check` is syntax only, and reading text cannot see it. That
// mutation run against the browser suite fails, but at a waitForSelector
// timeout that aborts the remaining ~110 scenarios (144/145 instead of
// 255/255), so neither half of the pair is redundant and neither is enough.

// jsFunctionBody returns the brace-balanced body of a top-level function, with
// whole-line comments blanked. Every needle below is also a word in the prose
// above the code it describes, so a whole-file scan would pass over a gutted
// function; scoping to the body is what makes the checks mean anything.
func jsFunctionBody(t *testing.T, js, name string) string {
	t.Helper()
	i := strings.Index(js, "function "+name+"(")
	if i < 0 {
		t.Fatalf("app.js: no function %s( — it was renamed or removed. Everything below "+
			"asserts about its body, so a guard that cannot find it checks nothing.", name)
	}
	open := strings.IndexByte(js[i:], '{')
	if open < 0 {
		t.Fatalf("app.js:%d: %s has no body", lineOf(js, i), name)
	}
	depth, end := 0, -1
	for k := i + open; k < len(js) && end < 0; k++ {
		switch js[k] {
		case '{':
			depth++
		case '}':
			if depth--; depth == 0 {
				end = k
			}
		}
	}
	if end < 0 {
		t.Fatalf("app.js:%d: unbalanced braces in %s", lineOf(js, i), name)
	}
	body := stripJSCommentLines(js[i+open : end+1])
	var out []string
	for _, ln := range strings.Split(body, "\n") {
		if c := strings.Index(ln, "//"); c >= 0 {
			ln = ln[:c]
		}
		out = append(out, ln)
	}
	return strings.Join(out, "\n")
}

// The validation refusal is deliberately NOT in the dialog: "schema, table and
// pk are all required" is a complaint about the form, and it belongs beside
// the form. Everything after that early return is a RESULT — including a 422
// gap refusal, which answers the request rather than correcting the fields.
const stateValidationReturn = `required — fill them in above.`

func runStateAfterValidation(t *testing.T) string {
	t.Helper()
	body := jsFunctionBody(t, readAsset(t, "app.js"), "runState")
	i := strings.Index(body, stateValidationReturn)
	if i < 0 {
		t.Fatalf("runState: the inline validation refusal (%q) is gone. This guard splits the "+
			"function there to separate 'the form is wrong' from 'here is your answer'; "+
			"without the split it would forbid the one inline render that is correct.",
			stateValidationReturn)
	}
	return body[i:]
}

func TestStateResultsRenderInDialog(t *testing.T) {
	region := runStateAfterValidation(t)

	if !strings.Contains(region, "openModal(") {
		t.Error("runState: no openModal( after the validation return — the reconstructed state " +
			"is not going into a dialog. #1405: the output is unbounded and is read on the way " +
			"to the reversal script, so rendering it inline pushes that script off screen.")
	}

	// #state-out / #state-warnings survive for the validation refusal above.
	// Past that point the only thing runState may do with them is empty them:
	// any other use means a result is being painted back into the strip the
	// dialog exists to keep clear. This is the single-word regression.
	inline := regexp.MustCompile(`\b(out|warns)\b`)
	for _, m := range inline.FindAllStringIndex(region, -1) {
		// clear(out) / clear(warns) are the permitted mentions.
		pre := region[:m[0]]
		if strings.HasSuffix(pre, "clear(") {
			continue
		}
		line := strings.TrimSpace(lineAround(region, m[0]))
		t.Errorf("runState: %q is used after the validation return, in %q. The inline "+
			"containers are for the form-validation refusal only; a result rendered there "+
			"lands back under the filter form (#1405).", region[m[0]:m[1]], line)
	}
}

func TestStateDialogKeepsRestoreReachable(t *testing.T) {
	region := runStateAfterValidation(t)

	// Two halves of one claim, and neither is worth anything alone. Appending
	// the action outside the scrolling body only helps if the BODY is what
	// scrolls; capping the body only helps if the action is not inside it.
	// A wide row's state table and a busy row's timeline both exceed the
	// viewport, and "Restore to this state" is why the dialog was opened.
	if !strings.Contains(region, ".panel.append(") {
		t.Error("runState: the restore action is not appended to the dialog PANEL. Inside " +
			".modal-body it scrolls away with the state table it belongs to (#1405).")
	}

	css := readAsset(t, "style.css")
	rule := cssRule(t, css, ".state-modal .modal-body")
	for _, want := range []string{"max-height", "overflow-y: auto"} {
		if !strings.Contains(rule, want) {
			t.Errorf(".state-modal .modal-body is missing %q (rule: %q). Without it the panel "+
				"grows past the viewport instead of scrolling, and the action row pinned "+
				"outside it goes off screen with everything else.", want, rule)
		}
	}
}

// cssRule returns the declaration block of the first rule whose selector list
// matches exactly, so a longer selector that merely contains it cannot satisfy
// the lookup.
func cssRule(t *testing.T, css, selector string) string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(selector) + `\s*\{([^}]*)\}`)
	m := re.FindStringSubmatch(css)
	if m == nil {
		t.Fatalf("style.css: no rule for %q", selector)
	}
	return m[1]
}

func lineAround(s string, pos int) string {
	start := strings.LastIndexByte(s[:pos], '\n') + 1
	end := strings.IndexByte(s[pos:], '\n')
	if end < 0 {
		return s[start:]
	}
	return s[start : pos+end]
}
