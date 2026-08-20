package console

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// sanitizeCSS blanks out comment bodies and string literals, preserving both
// byte offsets and newlines so every match position and line number computed
// against the result is valid for the original file.
//
// Blanking rather than deleting is what makes the whole-file scan safe. Two
// concrete hazards it removes, both of which produced wrong answers in earlier
// hand-written versions of this audit: a comment quoting `@media
// (prefers-reduced-motion: ...)` was picked up as a real at-rule, and a
// `content: "{"` declaration desynchronised the brace scanner so a block's
// computed end landed hundreds of lines past its real one.
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
			for ; i < len(css); i++ {
				blank(i)
				if css[i] == '/' && i > 0 && css[i-1] == '*' && i > 1 {
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

func lineOf(css string, pos int) int { return strings.Count(css[:pos], "\n") + 1 }

func within(rs [][2]int, p int) bool {
	for _, r := range rs {
		if p >= r[0] && p <= r[1] {
			return true
		}
	}
	return false
}

var (
	animRE = regexp.MustCompile(`(?:^|[;{\s])animation(?:-name)?\s*:`)
	tranRE = regexp.MustCompile(`(?:^|[;{\s])transition\s*:`)
	// Properties whose transition physically moves something on screen.
	// Colour and shadow transitions are deliberately absent: reduced-motion is
	// about vestibular motion, and banning a 150ms colour fade would make the
	// rule one nobody could follow.
	geomRE = regexp.MustCompile(`\b(transform|all|translate|scale|rotate|top|left|right|bottom|width|height|margin|padding|inset|gap)\b`)
	durRE  = regexp.MustCompile(`(?:^|[\s(,])(\d*\.?\d+)(ms|s)\b`)
)

// transitionIsMotion reports whether any comma-separated segment of a
// transition value both moves geometry AND runs long enough to read as
// animation rather than feedback.
//
// The threshold exists because a blanket "no transform transitions outside the
// guard" would be a rule nobody follows: five of the seven this audit first
// surfaced were a 2px icon nudge on hover, a 1px button press, and a rotating
// caret — direct-manipulation feedback, not the large sustained motion
// prefers-reduced-motion is about. At a third of a second a transition has
// stopped being feedback; that is where the two genuine entrance animations in
// this file (a timeline node sliding in, its dot scaling up) sit, and both are
// entrances, exactly the class `rise` is already guarded for.
func transitionIsMotion(value string) (bool, string) {
	for _, seg := range strings.Split(value, ",") {
		if !geomRE.MatchString(seg) {
			continue
		}
		m := durRE.FindStringSubmatch(seg)
		if m == nil {
			continue // no duration: nothing transitions
		}
		ms, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			continue
		}
		if m[2] == "s" {
			ms *= 1000
		}
		if ms >= motionThresholdMs {
			return true, strings.TrimSpace(seg)
		}
	}
	return false, ""
}

// Where feedback ends and animation begins, in milliseconds.
const motionThresholdMs = 300

func declValue(css string, from int) string {
	end := strings.IndexAny(css[from:], ";}")
	if end < 0 {
		return css[from:]
	}
	return css[from : from+end]
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
//     for an indicator whose animation was the only signal (the coverage
//     spinner dims instead of spinning), and nothing else.
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

	noPref := atRuleRanges(t, css, "@media (prefers-reduced-motion: no-preference)")
	reduce := atRuleRanges(t, css, "@media (prefers-reduced-motion: reduce)")
	if len(noPref) == 0 {
		t.Fatal("style.css has no prefers-reduced-motion: no-preference block — this guard covers nothing")
	}

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
			report(m[0], "animates outside @media (prefers-reduced-motion: no-preference). "+
				"Move it in, and make sure the rule left outside is the RESTING state — an "+
				"animation with forwards/both fill often leaves the base rule holding the "+
				"START frame, which then becomes permanent.")
		}
	}

	for _, m := range tranRE.FindAllStringIndex(css, -1) {
		motion, seg := transitionIsMotion(declValue(css, m[0]))
		if !motion {
			continue
		}
		switch {
		case within(reduce, m[0]):
			report(m[0], "a reduce block must not transition geometry: "+seg)
		case !within(noPref, m[0]):
			report(m[0], "`"+seg+"` moves geometry for "+strconv.Itoa(motionThresholdMs)+
				"ms or longer, which reads as animation rather than feedback. Move the "+
				"transition into @media (prefers-reduced-motion: no-preference); leave the "+
				"final position outside it so the element still ARRIVES without the animation.")
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
