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
	// Whole-line comments go BEFORE the brace walk, not after. The walk counts
	// braces in raw source, so a `{` inside a comment in the body unbalances it
	// and the extracted region ends in the wrong place — and this change added
	// two dozen lines of comment to the very function being walked. Stripping
	// first costs nothing and removes the hazard for the shape that actually
	// grows here.
	//
	// Residual, stated rather than papered over: a TRAILING comment carrying an
	// unbalanced brace still confuses the count. Handling that needs a real
	// tokenizer, and the failure is loud (a Fatalf about unbalanced braces or a
	// missing needle), not silent.
	js = stripJSCommentLines(js)
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
	body := js[i+open : end+1]
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
const stateValidationReturn = `required; fill them in above.`

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
	//
	// Two separate scans, because the identifiers and the ids fail differently.
	//
	// The identifier scan runs over CODE ONLY. Run over the raw region it
	// reports ordinary English: this dialog's own descriptions are in here, and
	// so is a catch branch where "timed out" is a live thing to write. Review
	// demonstrated it with one plausible added sentence — "Older changes have
	// aged out of the index" — which produced a confident, wholly fabricated
	// alarm about inline rendering. A guard that cries wolf over prose gets
	// weakened by the next person to hit it, so the literals are blanked (not
	// removed: blanking preserves every offset, so the reported line is still
	// the real one).
	code := blankStringLiterals(region)
	inline := regexp.MustCompile(`\b(out|warns)\b`)
	for _, m := range inline.FindAllStringIndex(code, -1) {
		// clear(out) / clear(warns) are the permitted mentions.
		if strings.HasSuffix(code[:m[0]], "clear(") {
			continue
		}
		line := strings.TrimSpace(lineAround(region, m[0]))
		t.Errorf("runState: %q is used after the validation return, in %q. The inline "+
			"containers are for the form-validation refusal only; a result rendered there "+
			"lands back under the filter form (#1405).", code[m[0]:m[1]], line)
	}

	// …and the ids scan the literals the first one just blanked. Blanking alone
	// would open the shape it closed: re-looking-up the container by id
	// (`renderStateAt(document.getElementById("state-out"), data)`) puts the
	// name only inside a string, where the identifier scan can no longer see
	// it. Narrowing a guard is how false negatives get in.
	for _, id := range []string{"state-out", "state-warnings"} {
		// Bare, not quote-wrapped. A CSS selector puts a `#` in front of the id
		// — `$("#state-out", VIEW())`, which is runState's own first line and
		// this file's idiom for the lookup — so requiring the quote directly
		// before the name missed the exact shape this check is named after.
		// Neither pass saw it: the identifier scan had already blanked the
		// literal. Hyphenated names cannot appear as JS identifiers, so
		// dropping the anchor costs no precision.
		if strings.Contains(region, id) {
			t.Errorf("runState: %q is looked up again after the validation return. Those "+
				"containers hold the form-validation refusal only; a result sent back to one "+
				"lands under the filter form (#1405).", id)
		}
	}
}

func TestStateDialogKeepsRestoreReachable(t *testing.T) {
	region := runStateAfterValidation(t)

	// Two halves of one claim about SHOW STATE, and neither is worth anything
	// alone. Appending the action outside the scrolling body only helps if the
	// BODY is what scrolls; capping the body only helps if the action is not
	// inside it. A wide row's state table exceeds the viewport, and
	// "Restore to this state" is why the dialog was opened.
	//
	// Scoped to the branch, not the function. The claim does NOT hold for Show
	// history and must not be written as if it did: there every node carries
	// its own restore button, which belongs beside the node it names and
	// scrolls with it. Asserting over the whole region would state a property
	// of one mode as a property of both — and the CSS comment beside this rule
	// said exactly that until review caught it.
	if i := strings.Index(region, "renderStateAt("); i < 0 {
		t.Error("runState no longer calls renderStateAt — this check is scoped to the Show state " +
			"branch and can no longer find it.")
	} else if !strings.Contains(region[i:], ".panel.append(") {
		t.Error("runState: the Show state action is not appended to the dialog PANEL. Inside " +
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

// blankStringLiterals replaces the CONTENTS of every quoted run with spaces,
// preserving length so reported offsets stay real.
//
// A regex was tried first and was not enough. `"[^"\n]*"` mis-pairs across an
// escaped quote and across a single-quoted string that contains double quotes,
// letting prose back into the identifier scan — and the second shape already
// exists in this function (`$('[name="state_at"]', VIEW())`). Both were shown
// producing the fabricated alarm the blanking exists to stop.
//
// One property to preserve if this is touched: the scanned region begins
// MID-LITERAL, because runStateAfterValidation splits on a fragment of the
// validation message, so the opening quote of that literal is outside the
// region. Closing an unterminated run at the newline instead of swallowing the
// rest of the function is what makes that safe.
func blankStringLiterals(s string) string {
	out := []byte(s)
	for i := 0; i < len(out); i++ {
		q := out[i]
		if q != '"' && q != '\'' && q != '`' {
			continue
		}
		for j := i + 1; j < len(out); j++ {
			if out[j] == '\\' && q != '`' && j+1 < len(out) && out[j+1] != '\n' {
				out[j], out[j+1] = ' ', ' '
				j++
				continue
			}
			if out[j] == q {
				i = j
				break
			}
			if out[j] == '\n' && q != '`' {
				i = j
				break
			}
			out[j] = ' '
		}
	}
	return string(out)
}

func lineAround(s string, pos int) string {
	start := strings.LastIndexByte(s[:pos], '\n') + 1
	end := strings.IndexByte(s[pos:], '\n')
	if end < 0 {
		return s[start:]
	}
	return s[start : pos+end]
}
