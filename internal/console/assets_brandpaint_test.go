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
// It is also required rather than tidy: this section's prose names properties
// and at-rules that the guards below treat as findings.
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
	// opening `/*`, and starting mid-body it never sees one. The damage is the
	// opposite shape: the header PROSE survives unblanked and is read as CSS,
	// and it quotes both `@supports` and `color: transparent`. That turns
	// guardRanges' count check into a Fatal blaming a brace error that does not
	// exist, and invents a finding for a sentence.
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
		if clipToTextRE.MatchString(prelude) {
			out = append(out, r)
		}
	}
	return out
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

	for _, m := range clipToTextRE.FindAllStringIndex(css, -1) {
		if within(guarded, m[0]) {
			continue
		}
		t.Errorf("style.css:%d: %q is not inside an @supports test for background-clip.",
			lineOf(css, m[0]), strings.TrimSpace(css[m[0]:m[1]]))
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
	// alongside the motion section instead of inside it. Motion here would also
	// escape that section's reduced-motion guard.
	if m := regexp.MustCompile(`\b(animation|transition)(-[a-z]+)?\s*:`).FindStringIndex(section); m != nil {
		t.Errorf("style.css:%d: the brand section declares %q. It is paint-only; anything that "+
			"moves belongs in the motion section, where the reduced-motion guard covers it.",
			at(m[0]), strings.TrimSpace(section[m[0]:m[1]]))
	}
}

// cssRef and jsRef match a class as a WHOLE token, never as a substring.
//
// A plain Contains passed happily when the CSS selector was renamed to
// `.ov-stat-number`, because the old name is a PREFIX of the new one — so the
// guard reported the pair as intact at the exact moment it came apart. The
// trailing \b is what closes that; it does not (and need not) reject a name
// EXTENDED at the front, since `-` is not a word character.
func cssRef(class string) *regexp.Regexp {
	return regexp.MustCompile(`\.` + regexp.QuoteMeta(class) + `\b`)
}

// jsGrantRE counts the class inside STRING literals only.
//
// Counting raw bytes made an ordinary documentation line — one that merely
// named the class in a comment — read as a second grant site, and the test
// accused it of bypassing the gate. A guard that invents alarms against
// correct code is the one that gets deleted. Both quote styles are accepted so
// switching them is not an alarm either.
//
// Known limit, stated rather than papered over: a comment that puts the class
// in QUOTES still counts. That is a much narrower surface than any mention,
// and it does not warrant a JavaScript parser here.
func jsGrantRE(class string) *regexp.Regexp {
	return regexp.MustCompile(`["'][^"'\n]*\b` + regexp.QuoteMeta(class) + `\b[^"'\n]*["']`)
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
	if !cssRef(brandOptInClass).MatchString(section) {
		t.Fatalf("the brand section no longer styles .%s — app.js still adds the class, so the "+
			"Overview counts render with no gradient and nothing reports it", brandOptInClass)
	}

	js := readAsset(t, "app.js")
	switch n := len(jsGrantRE(brandOptInClass).FindAllString(js, -1)); {
	case n == 0:
		t.Fatalf("style.css styles .%s but app.js never applies it: the rule is dead and the "+
			"Overview counts are unstyled", brandOptInClass)
	case n > 1:
		t.Errorf("app.js puts %q in %d string literals. It is granted in one place on purpose — "+
			"that expression is the gate that withholds the gradient from the semantic `danger` "+
			"tile and from any modifier below the large-text bar; a second site bypasses it.",
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
	if !cssRef(wordmark).MatchString(section) {
		t.Errorf("the #1385 brand section no longer paints .%s. The sidebar wordmark is one of the "+
			"two surfaces this section exists for; dropping it is silent everywhere else.", wordmark)
	}
	if !regexp.MustCompile(`class="[^"]*\b` + wordmark + `\b`).MatchString(readAsset(t, "index.html")) {
		t.Errorf("index.html no longer carries class=%q, so the wordmark loses BOTH its base rule "+
			"and its gradient and renders as unstyled inherited text.", wordmark)
	}
}
