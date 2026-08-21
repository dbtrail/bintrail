package console

import (
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// sanitizeCSS blanks out comment bodies and string literals, preserving both
// byte offsets and newlines so every match position and line number computed
// against the result is valid for the original file.
//
// Blanking rather than deleting is what keeps every offset and line number
// reported below valid against the ORIGINAL file.
//
// An earlier version of this comment also argued blanking was needed so a
// commented-out declaration still satisfied the `[;{\s]` prefix. That was
// wrong, and it is recorded rather than quietly dropped because of how it
// failed: `{` is itself in the prefix class, so `.a{/*c*/animation: x 1s}` is
// caught either way. It read as a verified rationale because nobody ran it.
//
// Two hazards it forecloses. Neither is present in style.css today, and both
// were demonstrated by review rather than observed in the wild: a comment
// quoting `@media (prefers-reduced-motion: ...)` read as a real at-rule, and a
// `content: "{"` declaration desynchronising the brace scanner so a block's
// computed end lands hundreds of lines past its real one.
func sanitizeCSS(css string) string {
	out := []byte(css)
	blank := func(i int) {
		if out[i] != '\n' {
			out[i] = ' '
		}
	}
	for i := 0; i < len(css); i++ {
		switch {
		case css[i] == '/' && i+1 < len(css) && css[i+1] == '*':
			blank(i)
			blank(i + 1)
			// Scanning from i+2, not i: starting at the opening `/` let the
			// terminator check match a `*` belonging to the opener, so
			// `.a>*/* c */ {` closed early and leaked the comment's braces
			// into the scan — the exact desync this function prevents.
			// (`/*/` on its own is unaffected; both versions close it at the
			// third character. Worth stating, because it was the example
			// originally given and it was the wrong one.)
			i += 2
			for ; i < len(css); i++ {
				blank(i)
				if css[i] == '/' && css[i-1] == '*' {
					break
				}
			}
		case css[i] == '"' || css[i] == '\'':
			q := css[i]
			i++
			for ; i < len(css) && css[i] != q; i++ {
				if css[i] == '\\' && i+1 < len(css) {
					blank(i)
					i++
				}
				blank(i)
			}
		}
	}
	return string(out)
}

// atRuleRanges returns the byte ranges of every block introduced by needle.
func atRuleRanges(t *testing.T, css, needle string) [][2]int {
	t.Helper()
	var out [][2]int
	for i := 0; ; {
		j := strings.Index(css[i:], needle)
		if j < 0 {
			return out
		}
		start := i + j
		open := strings.IndexByte(css[start:], '{')
		if open < 0 {
			t.Fatalf("%s at line %d has no opening brace", needle, lineOf(css, start))
		}
		depth, k, closed := 0, start+open, false
		for ; k < len(css); k++ {
			if css[k] == '{' {
				depth++
			} else if css[k] == '}' {
				if depth--; depth == 0 {
					closed = true
				}
			}
			if closed {
				break
			}
		}
		if !closed {
			// Failing loudly rather than returning a range that swallows the
			// rest of the file: that shape makes every later declaration look
			// guarded, so the guard would pass by covering everything.
			t.Fatalf("unbalanced braces in the %s block at line %d", needle, lineOf(css, start))
		}
		out = append(out, [2]int{start, k})
		i = k + 1
	}
}

// guardRanges is atRuleRanges plus the merge check, so both tests get it.
//
// atRuleRanges fails loudly on ONE brace error, but a stray `{` inside a guard
// paired with a stray `}` later balances into a single range spanning the
// file — at which point everything looks guarded. Comparing the range count to
// the needle count catches that without re-architecting the scanner.
//
// It catches the merge only when the swallowed region still CONTAINS another
// occurrence of the needle. A merge that also removed the swallowed at-rule
// keeps both counts in step and passes; that needs a brace fault and a
// refactor in the same edit, and is not claimed to be covered.
func guardRanges(t *testing.T, css, needle string) [][2]int {
	t.Helper()
	rs := atRuleRanges(t, css, needle)
	if want := strings.Count(css, needle); len(rs) != want {
		t.Fatalf("found %d %s blocks but %d occurrences of the at-rule: a brace error has merged "+
			"blocks, so this guard cannot tell guarded from unguarded", len(rs), needle, want)
	}
	return rs
}

func lineOf(css string, pos int) int { return strings.Count(css[:pos], "\n") + 1 }

func within(rs [][2]int, p int) bool {
	for _, r := range rs {
		if p >= r[0] && p <= r[1] {
			return true
		}
	}
	return false
}

const (
	noPrefNeedle = "@media (prefers-reduced-motion: no-preference)"
	reduceNeedle = "@media (prefers-reduced-motion: reduce)"
	// Where feedback ends and animation begins, in milliseconds.
	motionThresholdMs = 300
)

var (
	animRE = regexp.MustCompile(`(?:^|[;{\s])animation(?:-name)?\s*:`)
	// The longhand is matched too. `animation(-name)?` already covered its
	// counterpart, and the asymmetry was the whole bug: `transition-property:
	// transform; transition-duration: .45s` produced zero findings.
	tranRE = regexp.MustCompile(`(?:^|[;{\s])transition(?:-property)?\s*:`)
	// Properties whose transition physically moves something on screen.
	// Colour and shadow transitions are deliberately absent: reduced-motion is
	// about vestibular motion, and banning a 150ms colour fade would make the
	// rule one nobody could follow.
	geomRE = regexp.MustCompile(`(?i)\b(transform|all|translate|scale|rotate|top|left|right|bottom|width|height|margin|padding|inset|gap)\b`)
	// `\btop\b` matches INSIDE `border-top-color`, because `-` is a non-word
	// character on both sides — so the rule above WOULD fire on a pure colour
	// transition, contradicting its own comment. Nothing in style.css triggers
	// it today; this is defensive. A guard that invents alarms
	// against correct code is the one that gets deleted, so the non-geometric
	// suffixed properties are removed before matching. Dropping every
	// hyphenated name instead would also lose `max-height` and `margin-left`,
	// which genuinely do move things.
	nonGeomPropRE = regexp.MustCompile(`(?i)\b[a-z-]*-(color|style|shadow|image|radius)\b`)
	// Case-insensitive: `400MS` is valid CSS. The prefix class admits `:` so a
	// duration written flush against the colon (`transition:.4s transform`) is
	// still found.
	durRE = regexp.MustCompile(`(?i)(?:^|[\s(,:])(\d*\.?\d+)(ms|s)\b`)
	// Balanced groups are blanked before the value is split on commas: the
	// commas inside `cubic-bezier(.4, 0, .2, 1)` tore one transition into four
	// fragments, and a duration written AFTER the timing function landed in a
	// fragment naming no property, so it escaped entirely.
	parenGroupRE   = regexp.MustCompile(`\([^()]*\)`)
	longhandPropRE = regexp.MustCompile(`transition-property\s*:([^;}]*)`)
	longhandDurRE  = regexp.MustCompile(`transition-duration\s*:([^;}]*)`)
	kfDeclRE       = regexp.MustCompile(`@keyframes\s+([\w-]+)`)
	animValueRE    = regexp.MustCompile(`(?:^|[;{\s])animation(?:-name)?\s*:([^;}]*)`)
)

// declValue returns the declaration starting at from, up to its terminator.
func declValue(css string, from int) string {
	end := strings.IndexAny(css[from:], ";}")
	if end < 0 {
		return css[from:]
	}
	return css[from : from+end]
}

func blankParens(s string) string {
	for {
		next := parenGroupRE.ReplaceAllStringFunc(s, func(m string) string { return strings.Repeat(" ", len(m)) })
		if next == s {
			return s
		}
		s = next
	}
}

// transitionIsMotion reports whether any comma-separated segment of a
// transition value both moves geometry AND runs long enough to read as
// animation rather than feedback.
//
// The threshold exists because a blanket "no transform transitions outside the
// guard" would be a rule nobody follows. Of the seven sitting outside the
// guard when this was written (count today and you find more, because three
// are now inside it), five are direct-manipulation feedback: a 2px icon nudge on hover,
// a 1px button press, a rotating caret, an 8% swatch hover scale, and — the
// largest thing the threshold waives, so worth naming rather than implying —
// a 16px settings-toggle knob slide. At a third of a second a transition has
// stopped being feedback; that is where the timeline node's .45s slide-in
// sits, an entrance of exactly the class `rise` is already guarded for.
//
// The other declaration above the line, `.tl-dot`'s .3s, is currently inert:
// no cascade-level transform change ever applies to that element, and the
// dot's scale-up is the `dotpop` ANIMATION — animation output does not start a
// transition. Guarded anyway rather than deleted; the cost is one line, and
// the day something does move the dot the guard is already there.
func transitionIsMotion(value string) (bool, string) {
	for _, seg := range strings.Split(blankParens(value), ",") {
		if !geomRE.MatchString(nonGeomPropRE.ReplaceAllString(seg, "")) {
			continue
		}
		m := durRE.FindStringSubmatch(seg)
		if m == nil {
			// No LITERAL duration. A `var(--slow)` duration is waived here,
			// not proven safe.
			continue
		}
		if ms, ok := durationMs(seg); ok && ms >= motionThresholdMs {
			return true, strings.TrimSpace(seg)
		}
	}
	return false, ""
}

// durationMs reads the first literal time in seg, which per spec is the
// duration (a second time would be the delay).
func durationMs(seg string) (float64, bool) {
	m := durRE.FindStringSubmatch(seg)
	if m == nil {
		return 0, false
	}
	ms, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, false
	}
	if !strings.EqualFold(m[2], "ms") {
		ms *= 1000
	}
	return ms, true
}

// longhandIsMotion answers the same question for the split form, where the
// duration lives in a declaration of its own.
//
// The first version of this branch simply treated any geometry named by a
// transition-property as motion, duration unread. That was not a possible
// false alarm but a certain one — `transition-property: transform;
// transition-duration: .1s` is feedback by the very threshold this file
// spends a paragraph justifying, and it would have been flagged. Pairing them
// costs a rule-block scan.
//
// A transition-property with NO transition-duration is deliberately not
// motion: the initial value is 0s, so the property is listed and nothing
// transitions.
func longhandIsMotion(css string, pos int) (bool, string) {
	block := enclosingRule(css, pos)
	pm := longhandPropRE.FindStringSubmatch(block)
	if pm == nil || !geomRE.MatchString(nonGeomPropRE.ReplaceAllString(pm[1], "")) {
		return false, ""
	}
	dm := longhandDurRE.FindStringSubmatch(block)
	if dm == nil {
		return false, ""
	}
	for _, seg := range strings.Split(blankParens(dm[1]), ",") {
		if ms, ok := durationMs(seg); ok && ms >= motionThresholdMs {
			return true, "transition-property:" + strings.TrimSpace(pm[1]) +
				"; transition-duration:" + strings.TrimSpace(dm[1])
		}
	}
	return false, ""
}

// enclosingRule returns the declaration block containing pos.
func enclosingRule(css string, pos int) string {
	start := strings.LastIndexAny(css[:pos], "{}") + 1
	end := strings.IndexByte(css[pos:], '}')
	if end < 0 {
		return css[start:]
	}
	return css[start : pos+end]
}

// Every motion declaration in style.css must sit inside the reduced-motion
// guard, and a reduce block must never itself animate.
//
// Two legal shapes, and this test is what makes "two" true rather than
// aspirational (#1392 found three, one of them — scaling durations by a
// --motion variable that was never zeroed — guarding nothing at all):
//
//  1. motion lives in @media (prefers-reduced-motion: no-preference); the rule
//     outside it is the resting state.
//  2. @media (prefers-reduced-motion: reduce) may carry a STATIC alternative
//     for an indicator whose animation carried nearly all of its signal (the coverage
//     spinner dims instead of spinning), and nothing else.
//
// This test enforces the negative half of shape 2 — no animation, no long
// geometry transition inside a reduce block — and cannot enforce "nothing
// else". Nor is it "every motion declaration" in the fullest sense: it knows
// two property families, so `scroll-behavior: smooth`, view transitions and
// anything driven from JavaScript are invisible to it.
//
// Where the rest is checked, precisely, because "the e2e covers it" was itself
// wrong once: scenario 17c in console-e2e renders the guarded rules in a real
// browser and asserts the RESTING state each one leaves behind — a cascade
// question no text scan can answer. Scenario 17d drives renderTimeline under
// both media states and pins the stagger app.js schedules in JavaScript, which
// neither this test nor 17c can reach. The two scrollIntoView call sites that
// read the same matchMedia helper have no E2E guard: their smooth/auto
// difference is a browser-internal scroll with no observable DOM state. The
// Go test below is what holds them.
//
// The console is opened during incidents. Someone whose OS asks for reduced
// motion is not asking for a nicer console; motion sickness while reading a
// recovery plan is the failure this prevents.
func TestReducedMotionHasOneShape(t *testing.T) {
	raw, err := os.ReadFile("assets/style.css")
	if err != nil {
		t.Fatal(err)
	}
	css := sanitizeCSS(string(raw))

	noPref := guardRanges(t, css, noPrefNeedle)
	reduce := guardRanges(t, css, reduceNeedle)
	if len(noPref) == 0 {
		t.Fatal("style.css has no prefers-reduced-motion: no-preference block — this guard covers nothing")
	}
	// atRuleRanges fails loudly on ONE brace error, but a stray `{` inside a
	// guard paired with a stray `}` later balances out into a single range
	// spanning most of the file — at which point everything looks guarded.
	// Comparing the range count to the needle count catches the swallowing
	// without re-architecting the scanner.

	ws := regexp.MustCompile(`\s+`)
	report := func(pos int, why string) {
		t.Errorf("style.css:%d: %s\n  %s", lineOf(css, pos),
			strings.TrimSpace(ws.ReplaceAllString(declValue(css, pos), " ")), why)
	}

	for _, m := range animRE.FindAllStringIndex(css, -1) {
		switch {
		case within(reduce, m[0]):
			report(m[0], "a reduce block must not animate. Move the animation into the "+
				"no-preference block; keep only the static alternative here.")
		case !within(noPref, m[0]):
			report(m[0], "animates outside "+noPrefNeedle+". Move it in, and make sure the rule "+
				"left outside is the RESTING state — an animation with forwards/both fill often "+
				"leaves the base rule holding the START frame, which then becomes permanent.")
		}
	}

	for _, m := range tranRE.FindAllStringIndex(css, -1) {
		val := declValue(css, m[0])
		motion, seg := transitionIsMotion(val)
		if strings.Contains(val, "transition-property") {
			motion, seg = longhandIsMotion(css, m[0])
		}
		if !motion {
			continue
		}
		switch {
		case within(reduce, m[0]):
			report(m[0], "a reduce block must not transition geometry: "+seg)
		case !within(noPref, m[0]):
			report(m[0], "`"+seg+"` moves geometry for "+strconv.Itoa(motionThresholdMs)+
				"ms or longer, which reads as animation rather than feedback. Move the "+
				"transition into "+noPrefNeedle+"; leave the final position outside it so the "+
				"element still ARRIVES without the animation.")
		}
	}

	// The third shape #1392 retired. A duration multiplier reads like a global
	// motion switch, so the next person scales a new animation by it and
	// believes that honours the setting — but nothing ever set it to 0, and no
	// media query referenced it.
	if i := strings.Index(css, "--motion"); i >= 0 {
		t.Errorf("style.css:%d: --motion is back. It was deleted in #1392 because it was never "+
			"zeroed under reduce, so scaling a duration by it guarded nothing while looking "+
			"like it did. Put the rule in the no-preference block instead.", lineOf(css, i))
	}
}

// Every @keyframes must be driven from inside the reduced-motion guard.
//
// This is the half the rule above structurally cannot see. "Motion may not sit
// outside the guard" is satisfied perfectly by having no motion at all, so
// deleting a guarded declaration — the ordinary way a refactor loses an
// animation — passes it green. Nine such deletions were demonstrated during
// review, every assertion still passing.
//
// Anchoring on the keyframes rather than on a list of names in a test keeps
// the two in step automatically: a new animation is covered the moment its
// keyframes land, and an intentionally retired one is removed in the same
// commit as its declaration or this fails.
//
// Two deletions it still cannot see, both covered by console-e2e 17c instead,
// which reads computed durations and animation-names in a real browser: a
// guarded TRANSITION has no keyframes to orphan, and a keyframe with two
// drivers stays driven when one goes — `rise` is declared on both
// `.view-enter > *` and `.ov-stat`.
func TestEveryKeyframeIsDrivenFromInsideTheGuard(t *testing.T) {
	raw, err := os.ReadFile("assets/style.css")
	if err != nil {
		t.Fatal(err)
	}
	css := sanitizeCSS(string(raw))
	noPref := guardRanges(t, css, noPrefNeedle)

	declared := map[string]int{}
	for _, m := range kfDeclRE.FindAllStringSubmatchIndex(css, -1) {
		declared[css[m[2]:m[3]]] = m[0]
	}
	if len(declared) == 0 {
		t.Fatal("style.css declares no @keyframes — this guard covers nothing")
	}

	driven := map[string]bool{}
	for _, m := range animValueRE.FindAllStringSubmatchIndex(css, -1) {
		if !within(noPref, m[0]) {
			continue
		}
		for _, tok := range strings.FieldsFunc(css[m[2]:m[3]], func(r rune) bool {
			return r == ' ' || r == ',' || r == '\t' || r == '\n'
		}) {
			if _, ok := declared[tok]; ok {
				driven[tok] = true
			}
		}
	}

	var orphans []string
	for name := range declared {
		if !driven[name] {
			orphans = append(orphans, name)
		}
	}
	sort.Strings(orphans)
	for _, name := range orphans {
		t.Errorf("style.css:%d: @keyframes %s is never driven from inside %s.\n"+
			"  Either it lost its animation declaration (a deletion no other guard here can "+
			"see), the declaration sits outside the guard, or the keyframes are dead code and "+
			"should go in this commit.", lineOf(css, declared[name]), name, noPrefNeedle)
	}
}

// Motion started from JavaScript must consult the setting itself.
//
// style.css cannot reach it, and the CSS guard's success is what makes this
// necessary rather than optional: removing a transition under `reduce` stops
// the movement being SMOOTH, but a timer that schedules the position change
// still moves things. The timeline reveal was exactly that — the guarded CSS
// left every node snapping into place one at a time across ~2.8 seconds.
//
// Both anchors name a visual effect rather than a mechanism, so retuning the
// stagger or hoisting the gate into a local does not disarm them: `behavior:
// "smooth"` is the only scroll animation an element API offers, and `"in"` is
// the entrance class the stylesheet's .tl-node rules key on.
//
// Both scope to the enclosing FUNCTION, not the line. Line scoping looked
// tighter and was worse twice: the value pattern could not cross the `)` in
// `prefersReducedMotion()`, so it matched NEITHER real call site, and the
// ordinary hoist — `const reduced = prefersReducedMotion()` — would have
// false-alarmed for naming the gate on a different line.
//
// What this cannot do is notice a NEW kind of JavaScript motion. The CSS
// guards extend themselves because every animation must declare @keyframes;
// there is no equivalent anchor here, so `el.animate([...])` or a fresh
// `setTimeout` reveal ships unguarded. Filed rather than bolted on: a
// badly-chosen heuristic anchor cries wolf, which is how a guard gets deleted.
func TestJavaScriptMotionConsultsThePreference(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	const gate = "prefersReducedMotion"

	for _, m := range regexp.MustCompile(`\bbehavior:\s*[^;\n]*["']smooth["']`).FindAllStringIndex(js, -1) {
		if !strings.Contains(enclosingFunc(js, m[0]), gate) {
			t.Errorf("app.js:%d: %s\n  Smooth scrolling is a vestibular trigger and this one is on "+
				"the Restore path. Pass %s() ? \"auto\" : \"smooth\".",
				lineOf(js, m[0]), strings.TrimSpace(lineAt(js, m[0])), gate)
		}
	}

	// Renaming the entrance class would leave this loop with nothing to iterate
	// and no complaint — the shape both CSS guards above refuse to have. The
	// smooth-scroll loop deliberately gets no equivalent check: removing the
	// last smooth scroll is a legitimate edit, whereas the timeline reveal
	// going away silently means this test stopped covering its subject.
	entrance := regexp.MustCompile(`classList\.add\("in"\)`).FindAllStringIndex(js, -1)
	if len(entrance) == 0 {
		t.Fatal(`app.js no longer applies the "in" entrance class — either the timeline reveal is ` +
			`gone or the class was renamed, and either way this guard now covers nothing`)
	}
	for _, m := range entrance {
		if !strings.Contains(enclosingFunc(js, m[0]), gate) {
			t.Errorf("app.js:%d: the entrance class is applied in a function that never consults %s().\n"+
				"  Guarding the CSS only removes the SMOOTHNESS; a scheduled class change still moves "+
				"content. Apply it to every node at once when the preference is set.",
				lineOf(js, m[0]), gate)
		}
	}
}

// funcBoundaryRE matches every top-level declaration that starts a function
// body in app.js.
//
// `\nfunction ` alone was not enough and the gap was invisible: app.js has 56
// `async function` declarations, so the window computed for the timeline
// reveal ran 72 lines past the end of renderTimeline and swallowed the whole
// of renderStatus. A gate written in THAT function satisfied the check — the
// exact failure the scoping exists to prevent.
var funcBoundaryRE = regexp.MustCompile(`(?m)^(?:async\s+)?function\s|^(?:const|let|var|class)\s`)

// enclosingFunc returns the body of the top-level declaration containing pos,
// bounded at the next one.
func enclosingFunc(js string, pos int) string {
	start, end := 0, len(js)
	for _, l := range funcBoundaryRE.FindAllStringIndex(js, -1) {
		if l[0] <= pos {
			start = l[0]
			continue
		}
		end = l[0]
		break
	}
	return js[start:end]
}

// The classifier decides which transitions the whole-file guard polices, and
// until now it was driven only by the real stylesheet — so every branch it
// does not currently reach was untested, including one that would have been
// wrong the first time it fired.
//
// The threshold cases are the point: 250ms is feedback and 300ms is motion,
// and getting that backwards produces either a guard that misses entrances or
// one that alarms on a hover nudge.
func TestTransitionIsMotionClassifies(t *testing.T) {
	for _, tc := range []struct {
		decl string
		want bool
		why  string
	}{
		{"transition: transform .12s var(--ease)", false, "button press, feedback"},
		{"transition: color .18s, transform .25s var(--ease-out)", false, "icon nudge, feedback"},
		{"transition: transform .3s var(--ease-out)", true, "exactly the threshold, inclusive"},
		{"transition: transform .45s var(--ease-out)", true, "timeline entrance"},
		{"transition: all .5s", true, "`all` includes transform"},
		{"transition: all .1s", false, "`all`, but too short to be motion"},
		{"transition: border-top-color .4s", false, "colour only — \\btop\\b must not match inside it"},
		{"transition: border-top-width .4s", true, "width is geometry; the strip must not eat it"},
		{"transition: box-shadow 2s", false, "shadow paints depth, it does not move"},
		{"transition: background-image 2s", false, "not geometry"},
		{"transition: transform cubic-bezier(.4, 0, .2, 1) .45s", true, "duration after the timing function"},
		{"transition: transform .35s cubic-bezier(.4, 0, .2, 1)", true, "commas inside the group must not split it"},
		{"transition: transform .1s .5s", false, "the second time is the DELAY, not the duration"},
		{"transition: transform 400MS ease", true, "units are case-insensitive"},
		{"transition:.4s transform", true, "duration flush against the colon"},
		{"transition: TRANSFORM .5s", true, "property names are case-insensitive"},
		{"transition: opacity .5s, transform 2s", true, "one moving segment is enough"},
		{"transition: opacity .5s, color 2s", false, "no segment moves"},
		{"transition: transform var(--slow)", false, "no literal duration: waived, not proven safe"},
	} {
		if got, _ := transitionIsMotion(tc.decl); got != tc.want {
			t.Errorf("transitionIsMotion(%q) = %v, want %v — %s", tc.decl, got, tc.want, tc.why)
		}
	}
}

// The longhand form is judged from its rule block, because its duration is a
// separate declaration.
//
// The no-duration row is the one that matters and it is not a technicality:
// `transition-duration`'s initial value is 0s, so a property listed without
// one transitions nothing. Treating the property alone as motion — which the
// first version of this branch did — flags a rule that cannot animate.
func TestTransitionLonghandNeedsItsDuration(t *testing.T) {
	for _, tc := range []struct {
		css  string
		want bool
		why  string
	}{
		{".x { transition-property: transform; transition-duration: .45s; }", true, "the escape this branch exists for"},
		{".x { transition-property: transform; transition-duration: .1s; }", false, "short: feedback, same threshold as the shorthand"},
		{".x { transition-property: transform; }", false, "no duration: initial value 0s, nothing transitions"},
		{".x { transition-duration: 2s; transition-property: transform; }", true, "declaration order must not matter"},
		{".x { transition-property: color; transition-duration: 2s; }", false, "long, but colour does not move"},
		{".x { transition-property: transform; transition-duration: 100ms, 2s; }", true, "any listed duration over the line"},
	} {
		pos := strings.Index(tc.css, "transition-property")
		if got, _ := longhandIsMotion(tc.css, pos); got != tc.want {
			t.Errorf("longhandIsMotion(%q) = %v, want %v — %s", tc.css, got, tc.want, tc.why)
		}
	}
}
