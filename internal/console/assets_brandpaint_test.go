package console

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The class that opts a single Overview tile into the brand gradient (#1385).
// Named once because a guard below pins both ENDS of it: the CSS must define
// it and app.js must grant it. Rename one side only and the gradient silently
// stops appearing.
const brandOptInClass = "ov-stat-num"

func readAsset(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile("assets/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// brandSection returns the #1385 brand-warmth block of style.css with comment
// BYTES blanked, plus the file line its first byte sits on.
//
// Blanking rather than deleting keeps every offset inside the section aligned
// with the original text, so a finding can be reported at its real file line.
// It is also required rather than tidy: the section's prose names --delete
// while explaining why the deletes tile is excluded, and the semantics guard
// below treats that token as a finding.
func brandSection(t *testing.T) (string, int) {
	t.Helper()
	css := readAsset(t, "style.css")
	const marker = "Brand warmth: chrome only (#1385)"
	i := strings.Index(css, marker)
	if i < 0 {
		t.Fatal("the #1385 brand-warmth section header is gone — this guard no longer covers anything")
	}
	if n := strings.Count(css, marker); n > 1 {
		t.Fatalf("the %q marker appears %d times — everything under the later one would sit "+
			"outside these guards", marker, n)
	}
	// Back up to the banner that OPENS the section. Slicing at the marker
	// instead does NOT run the comment away — sanitizeCSS only reacts to an
	// opening `/*`, and starting mid-body it never sees one, so brace balance
	// and every at-rule count come out identical either way. What it does leave
	// behind is the MARKER, now readable as CSS, and the marker contains
	// `#1385` — which the colour-literal guard below reads as a four-digit hex
	// colour and reports the section for writing. Backing up to the banner puts
	// the marker back inside a comment where it belongs.
	//
	// It is load-bearing in the other direction too: without it the header
	// prose reaches the SELECTOR checks, and the wordmark guard is satisfied by
	// the words ".brand-name" in the exclusion table rather than by the rule.
	// Drop the back-up and drop the selector together, and that guard passes.
	if b := strings.LastIndex(css[:i], "/* ======"); b >= 0 {
		i = b
	}
	section := css[i:]
	if j := strings.Index(section[1:], "/* ======"); j > 0 {
		section = section[:j+1]
	}
	return sanitizeCSS(section), strings.Count(css[:i], "\n") + 1
}

var (
	// `background-clip: text` has exactly one purpose, so it needs no context
	// to be recognised. Anchored on `text` so the four `content-box` uses on
	// the scrollbar thumbs are not swept in.
	clipToTextRE = regexp.MustCompile(`(?:-webkit-)?background-clip\s*:\s*text\b`)
	// The leading class is what separates the `color` property from the nine
	// `border-color` / `background-color` declarations that also end in
	// `transparent` and are entirely ordinary. Group 1 ends where `color`
	// begins.
	transparentInkRE = regexp.MustCompile(`(^|[^-a-zA-Z])color\s*:\s*transparent\b`)
	// The other spelling, and it needs its own pattern rather than riding on
	// the one above — the leading-class guard that excludes `border-color`
	// excludes this too, because it is also hyphen-prefixed. Missing it would
	// be worse than an oversight: the section's own prose names
	// -webkit-text-fill-color as the tempting alternative, so it is the
	// substitution a reader is most likely to reach for.
	transparentFillRE = regexp.MustCompile(`(?:-webkit-)?text-fill-color\s*:\s*transparent\b`)
)

// clipSupportsRanges returns the ranges of the @supports blocks that actually
// test for background-clip.
//
// Matching the bare `@supports` keyword was not enough, and the gap was not
// theoretical: an `@supports (display: grid)` wrapper around a transparent-ink
// rule satisfied the keyword check completely while being exactly the state
// the check exists to forbid. The two real call sites also spell the condition
// differently — one wraps the disjunction in an extra pair of parens — so this
// reads the condition rather than matching a fixed string.
func clipSupportsRanges(t *testing.T, css string) [][2]int {
	t.Helper()
	var out [][2]int
	for _, r := range guardRanges(t, css, "@supports") {
		prelude := css[r[0]:]
		if b := strings.IndexByte(prelude, '{'); b >= 0 {
			prelude = prelude[:b]
		}
		// Polarity is not optional. `@supports not (background-clip: text)`
		// mentions the feature and means the exact opposite: its body applies
		// only on the engines that CANNOT paint over transparent ink, which is
		// the failure this whole guard exists for. A prelude that mentions the
		// feature under a `not` is therefore not a guard, and the declarations
		// inside it get reported like any other unguarded ones.
		if clipToTextRE.MatchString(prelude) && !regexp.MustCompile(`\bnot\b`).MatchString(prelude) {
			out = append(out, r)
		}
	}
	return out
}

// topLevelRules splits a block body into (prelude, body) pairs at depth 0, so
// a nested block cannot contribute its declarations as though they were
// selectors.
func topLevelRules(body string) [][2]string {
	var out [][2]string
	depth, start, openAt := 0, 0, 0
	for i := 0; i < len(body); i++ {
		switch body[i] {
		case '{':
			if depth == 0 {
				openAt = i
			}
			depth++
		case '}':
			if depth--; depth == 0 {
				out = append(out, [2]string{body[start:openAt], body[openAt+1 : i]})
				start = i + 1
			}
		}
	}
	return out
}

// gradientRuleSelectors returns the selector list of the rule that actually
// PAINTS the gradient — not every class mentioned somewhere in the section.
//
// The distinction is not pedantic; a mention-based check was already satisfied
// once by prose, and then a second time by a rule. Both painted classes now
// appear on TWO rules — the gradient and the width fix beside it — so
// "is this class in the section" stays true with the class removed from the
// only rule that paints it. Verified by mutation: that shape passed.
func gradientRuleSelectors(t *testing.T, section string) []string {
	t.Helper()
	rs := guardRanges(t, section, "@supports")
	if len(rs) != 1 {
		t.Fatalf("expected exactly one @supports block in the brand section, found %d", len(rs))
	}
	open := strings.IndexByte(section[rs[0][0]:], '{') + rs[0][0]
	for _, rule := range topLevelRules(section[open+1 : rs[0][1]]) {
		if !strings.Contains(rule[1], "background-image") {
			continue
		}
		var out []string
		for _, sel := range strings.Split(rule[0], ",") {
			if s := strings.Join(strings.Fields(sel), " "); s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	t.Fatal("no rule inside the brand section's @supports block declares background-image: " +
		"the gradient is not painted at all, and every selector check below would be vacuous")
	return nil
}

// paintsGradient compares selectors for EQUALITY, not containment.
//
// That is what closes the prefix trap an earlier version of this guard fell
// into: a plain Contains reported the pair as intact when the selector had
// been renamed to `.ov-stat-number`, because the old name is a prefix of the
// new one. Exact comparison against a parsed selector list rules out the whole
// family, extension at either end included.
func paintsGradient(sels []string, class string) bool {
	for _, s := range sels {
		if s == "."+class {
			return true
		}
	}
	return false
}

// Clipping a gradient to text requires painting the ink transparent, so where
// the clip does not take effect the text is GONE rather than merely unstyled.
// Every declaration that creates that state must stay behind an @supports test
// for background-clip.
//
// Scoped to the WHOLE stylesheet, not to the #1385 section. The failure is a
// property of the technique, not of one feature: the login panel's title
// (added long before this section) uses the identical pair, and a section-
// scoped guard silently covered neither it nor anything appended below the
// section's own banner.
//
// The failure is also invisible to the author. Every engine on a developer's
// machine satisfies the test, so an unguarded copy looks perfect locally and
// blanks the element on whatever does not.
func TestTransparentInkStaysBehindABackgroundClipSupportsTest(t *testing.T) {
	css := sanitizeCSS(readAsset(t, "style.css"))

	guarded := clipSupportsRanges(t, css)
	if len(guarded) == 0 {
		t.Fatal("style.css has no @supports test for background-clip, yet it clips gradients to " +
			"text: the declarations that make the ink transparent are unconditional, so any engine " +
			"without background-clip:text renders those elements as blank space")
	}

	for _, re := range []*regexp.Regexp{clipToTextRE, transparentFillRE} {
		for _, m := range re.FindAllStringIndex(css, -1) {
			if within(guarded, m[0]) {
				continue
			}
			t.Errorf("style.css:%d: %q is not inside an @supports test for background-clip.",
				lineOf(css, m[0]), strings.TrimSpace(css[m[0]:m[1]]))
		}
	}
	for _, m := range transparentInkRE.FindAllStringSubmatchIndex(css, -1) {
		at := m[3] // end of group 1 == start of `color`
		if within(guarded, at) {
			continue
		}
		t.Errorf("style.css:%d: %q is not inside an @supports test for background-clip.\n"+
			"Transparent ink with no gradient to replace it is invisible text, not a missing effect.\n"+
			"If this declaration is NOT gradient text, it is hiding an element by making its ink "+
			"vanish — say so at the site, because this guard cannot tell the two apart.",
			lineOf(css, at), strings.TrimSpace(css[at:m[1]]))
	}
}

// The brand palette decorates chrome. It may not restate the semantic colors,
// and it may not introduce a colour of its own that drifts from the tokens.
func TestBrandSectionPaintsFromTokensAndNeverSemantics(t *testing.T) {
	section, base := brandSection(t)
	at := func(i int) int { return base + strings.Count(section[:i], "\n") }

	if m := regexp.MustCompile(`--(insert|update|delete)\b`).FindStringIndex(section); m != nil {
		t.Errorf("style.css:%d: the brand section references %s. INSERT=mint / UPDATE=blue / "+
			"DELETE=red are decided in :root and this section decorates chrome — a brand rule that "+
			"repeats a semantic token is one edit away from overriding it.",
			at(m[0]), section[m[0]:m[1]])
	}

	// Every colour here must come from a brand token. A literal silently stops
	// tracking whatever it was copied from the day that token moves — the same
	// reasoning the --brand-wash-* comment gives for using color-mix. All the
	// notations are listed because banning only oklch() left the hex form
	// wide open, and the hex values sit in a comment beside the tokens, which
	// is an active invitation to paste one.
	literal := regexp.MustCompile(`#[0-9a-fA-F]{3,8}\b|\b(?:oklch|oklab|lab|lch|rgba?|hsla?|hwb)\(`)
	if m := literal.FindStringIndex(section); m != nil {
		t.Errorf("style.css:%d: the brand section writes the colour literal %q. Derive from the "+
			"existing brand tokens (var(--brand-headline), color-mix) so the palette stays one "+
			"source of truth — every contrast figure in this section's header is measured against "+
			"those tokens.", at(m[0]), section[m[0]:m[1]])
	}

	// The section is paint-only by construction, which is what lets it exist
	// alongside the motion section instead of inside it. A PLACEMENT rule, not
	// a coverage backstop: the whole-file reduced-motion guard would catch a
	// long transition written here on its own, but it deliberately classifies
	// anything under 300ms as direct-manipulation feedback rather than
	// animation, so a short one would land here seen by nothing else.
	if m := regexp.MustCompile(`\b(animation|transition)(-[a-z]+)?\s*:`).FindStringIndex(section); m != nil {
		t.Errorf("style.css:%d: the brand section declares %q. It is paint-only; anything that "+
			"moves belongs in the motion section, where it sits inside the reduced-motion block.",
			at(m[0]), strings.TrimSpace(section[m[0]:m[1]]))
	}
}

// stripJSCommentLines blanks whole-line comments and /* … */ regions, working
// A LINE AT A TIME.
//
// Deliberately not a JavaScript lexer, and the file is the argument: app.js
// holds a regex literal containing a double quote (`/[",\r\n]/`) and another
// containing an escaped slash (`/^\//`), plus four URLs with `//` inside
// string literals. A character scanner tracking quote state desyncs on the
// first of those and then blanks arbitrary code — silently, and in a guard.
// Line granularity cannot desync at all.
//
// Two earlier shapes were both wrong, each in the direction the other was
// right. Counting raw bytes made an ordinary documentation line naming the
// class read as a second grant site. Narrowing to quoted strings fixed that
// and opened two holes: a grant written in a TEMPLATE literal stopped
// counting (57 backticks in this file, so not exotic), and English
// contractions around the class name — "Don't rename ov-stat-num, it's …" —
// became string delimiters, so the false alarm came back in prose the
// codebase writes constantly.
//
// Residual, stated rather than papered over: a mention in a TRAILING comment
// on a line of code still counts. The failure message says to move it.
func stripJSCommentLines(js string) string {
	var b strings.Builder
	inBlock := false
	for _, line := range strings.Split(js, "\n") {
		t := strings.TrimSpace(line)
		switch {
		case inBlock:
			if strings.Contains(line, "*/") {
				inBlock = false
			}
		case strings.HasPrefix(t, "//"):
		case strings.HasPrefix(t, "/*"):
			if !strings.Contains(t[2:], "*/") {
				inBlock = true
			}
		default:
			b.WriteString(line)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// The Overview tile's opt-in class must exist on both sides, and app.js must
// grant it from exactly one place.
//
// One place is the invariant that matters, not a style rule: that single
// expression is where the gradient is withheld from the tiles that cannot take
// it. A second grant site elsewhere in app.js would bypass the gate without
// touching it.
//
// What this does NOT cover, verified by mutation rather than assumed:
// rewriting the gate IN PLACE so the class is granted unconditionally keeps
// the count at one and passes here. Nothing in the text of either file
// distinguishes a correct gate from an inverted one — that failure is a
// rendered result, and scenario 17e in test/console-e2e reads it back off a
// real danger tile. Nor does it reach markup outside app.js; the count is
// scoped to the file it reads.
func TestBrandOptInClassIsGrantedInExactlyOnePlace(t *testing.T) {
	section, _ := brandSection(t)
	if !paintsGradient(gradientRuleSelectors(t, section), brandOptInClass) {
		t.Fatalf("the gradient rule no longer lists .%s — app.js still adds the class, so the "+
			"Overview counts render with no gradient and nothing reports it", brandOptInClass)
	}

	js := stripJSCommentLines(readAsset(t, "app.js"))
	grant := regexp.MustCompile(regexp.QuoteMeta(brandOptInClass) + `\b`)
	switch n := len(grant.FindAllString(js, -1)); {
	case n == 0:
		t.Fatalf("style.css styles .%s but app.js never applies it: the rule is dead and the "+
			"Overview counts are unstyled", brandOptInClass)
	case n > 1:
		t.Errorf("app.js names %q %d times outside a comment line. It is granted in one place on "+
			"purpose — that expression is the gate that withholds the gradient from the semantic "+
			"`danger` tile and from any modifier below the large-text bar, and a second site "+
			"bypasses it. If one of these IS just documentation, move it to its own comment line.",
			brandOptInClass, n)
	}
}

// The sidebar wordmark is the OTHER half of what #1385 paints, and it was
// covered by nothing at all: dropping `.brand-name` from the gradient rule
// left every guard and every browser scenario green while the wordmark
// silently reverted to flat ink. "No gradient" is also what an unstyled
// element looks like, so a screenshot does not catch it either.
//
// Unlike the tile, this class is static markup, so the pair to pin is the
// stylesheet against index.html rather than against app.js.
func TestWordmarkKeepsItsGradient(t *testing.T) {
	const wordmark = "brand-name"

	section, _ := brandSection(t)
	if !paintsGradient(gradientRuleSelectors(t, section), wordmark) {
		t.Errorf("the gradient rule no longer lists .%s. The sidebar wordmark is one of the two "+
			"surfaces this section exists for; dropping it is silent everywhere else, and the "+
			"class still appearing on the width rule beside it does not count.", wordmark)
	}
	if !regexp.MustCompile(`class="[^"]*\b` + wordmark + `\b`).MatchString(readAsset(t, "index.html")) {
		t.Errorf("index.html no longer carries class=%q, so the wordmark loses BOTH its base rule "+
			"and its gradient and renders as unstyled inherited text.", wordmark)
	}
}
