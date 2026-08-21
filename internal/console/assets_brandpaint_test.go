package console

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The class that opts a single Overview tile into the brand gradient. Named
// once because two guards below pin the two ENDS of it: the CSS must define
// it and app.js must grant it from exactly one place. Rename one side only and
// the gradient silently stops appearing, which no visual test would notice
// because "no gradient" is also what an unstyled tile looks like.
const brandOptInClass = "ov-stat-num"

// brandSection returns the #1385 brand-warmth block of style.css with comment
// BYTES blanked, plus the file line its first byte sits on.
//
// Blanking rather than deleting keeps every offset inside the section aligned
// with the original text, so a finding can be reported at its real file line.
// It is also required rather than tidy: this section's own prose quotes
// `color: transparent` and names `--delete`, both of which two guards below
// treat as failures.
func brandSection(t *testing.T) (string, int) {
	t.Helper()
	data, err := os.ReadFile("assets/style.css")
	if err != nil {
		t.Fatal(err)
	}
	css := string(data)
	const marker = "Brand warmth: chrome only (#1385)"
	i := strings.Index(css, marker)
	if i < 0 {
		t.Fatal("the #1385 brand-warmth section header is gone — this guard no longer covers anything")
	}
	if n := strings.Count(css, marker); n > 1 {
		t.Fatalf("the %q marker appears %d times — everything under the later one would sit "+
			"outside these guards", marker, n)
	}
	// Back up to the banner that OPENS the section: the marker sits inside a
	// /* … */ block, so slicing at the marker itself would leave an
	// unterminated comment and blank the entire rest of the file.
	if b := strings.LastIndex(css[:i], "/* ======"); b >= 0 {
		i = b
	}
	section := css[i:]
	if j := strings.Index(section[1:], "/* ======"); j > 0 {
		section = section[:j+1]
	}
	return sanitizeCSS(section), strings.Count(css[:i], "\n") + 1
}

// Clipping a gradient to text requires painting the ink transparent, so where
// the clip does not take effect the text is GONE rather than merely unstyled.
// Both declarations that create that state must stay behind the @supports test
// that proves paint arrives to replace the ink.
//
// The failure is invisible to the author: every engine on a developer's
// machine satisfies the test, so an unguarded copy of these declarations looks
// perfect locally and blanks the sidebar wordmark on whatever does not.
func TestGradientTextStaysBehindItsSupportsTest(t *testing.T) {
	section, base := brandSection(t)

	supports := guardRanges(t, section, "@supports")
	if len(supports) == 0 {
		t.Fatal("the brand section has no @supports block: the declarations that make text " +
			"transparent are unconditional, so any engine without background-clip:text renders " +
			"the wordmark and the Overview counts as blank space")
	}

	risky := regexp.MustCompile(`(?:-webkit-)?background-clip\s*:\s*text|color\s*:\s*transparent`)
	for _, m := range risky.FindAllStringIndex(section, -1) {
		if within(supports, m[0]) {
			continue
		}
		t.Errorf("style.css:%d: %q sits outside the @supports test.\n"+
			"Transparent ink with no gradient to replace it is invisible text, not a missing effect.",
			base+strings.Count(section[:m[0]], "\n"), strings.TrimSpace(section[m[0]:m[1]]))
	}
}

// The brand palette decorates chrome. It may not restate, and may not be
// mistaken for, the semantic colors — and it may not introduce a colour of its
// own that drifts from the tokens.
func TestBrandSectionPaintsFromTokensAndNeverSemantics(t *testing.T) {
	section, base := brandSection(t)

	if m := regexp.MustCompile(`--(insert|update|delete)\b`).FindStringIndex(section); m != nil {
		t.Errorf("style.css:%d: the brand section references %s. INSERT=mint / UPDATE=blue / "+
			"DELETE=red are decided in :root and this section decorates chrome — a brand rule that "+
			"repeats a semantic token is one edit away from overriding it.",
			base+strings.Count(section[:m[0]], "\n"), section[m[0]:m[1]])
	}

	// Re-typing a colour here would silently stop tracking the token it was
	// copied from the day that token moves — the same reasoning the
	// --brand-wash-* comment gives for using color-mix.
	if i := strings.Index(section, "oklch("); i >= 0 {
		t.Errorf("style.css:%d: the brand section writes a raw oklch() literal. Derive from the "+
			"existing brand tokens (var(--brand-headline), color-mix) so the palette stays one "+
			"source of truth.", base+strings.Count(section[:i], "\n"))
	}

	// The section is paint-only by construction, which is what lets it exist
	// alongside the motion section instead of inside it. Motion here would also
	// escape that section's transform-only rule.
	if m := regexp.MustCompile(`\b(animation|transition)(-[a-z]+)?\s*:`).FindStringIndex(section); m != nil {
		t.Errorf("style.css:%d: the brand section declares %q. It is paint-only; anything that "+
			"moves belongs in the motion section, where the reduced-motion guard covers it.",
			base+strings.Count(section[:m[0]], "\n"), strings.TrimSpace(section[m[0]:m[1]]))
	}
}

// The opt-in class must exist on both sides, and app.js must grant it from
// exactly one place.
//
// One place is the invariant that matters, not a style rule: that single
// expression is where the gradient is withheld from the tiles that cannot take
// it — the semantic `danger` tile, and any tile carrying a modifier that puts
// it below WCAG's large-text bar. A second grant site elsewhere in app.js
// would bypass that gate without touching it.
//
// What this does NOT cover, verified by mutation rather than assumed: rewriting
// the gate IN PLACE so the class is granted unconditionally keeps the count at
// one and passes here. Nothing in the text of either file distinguishes a
// correct gate from an inverted one — that failure is a rendered result, and
// scenario 17e in test/console-e2e reads it back off a real danger tile.
func TestBrandOptInClassIsGrantedInExactlyOnePlace(t *testing.T) {
	section, _ := brandSection(t)

	// Matched as a whole token, never as a substring. A plain Contains passed
	// happily when the CSS selector was renamed to `.ov-stat-number`, because
	// the old name is a PREFIX of the new one — so the guard reported the pair
	// as intact at the exact moment it had come apart.
	cssRef := regexp.MustCompile(`\.` + regexp.QuoteMeta(brandOptInClass) + `\b`)
	jsRef := regexp.MustCompile(regexp.QuoteMeta(brandOptInClass) + `\b`)

	if !cssRef.MatchString(section) {
		t.Fatalf("the brand section no longer styles .%s — app.js still adds the class, so the "+
			"Overview counts render with no gradient and nothing reports it", brandOptInClass)
	}

	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	switch n := len(jsRef.FindAllString(string(js), -1)); {
	case n == 0:
		t.Fatalf("style.css styles .%s but app.js never applies it: the rule is dead and the "+
			"Overview counts are unstyled", brandOptInClass)
	case n > 1:
		t.Errorf("app.js mentions %q %d times. It is granted in one place on purpose — that "+
			"expression is the gate that withholds the gradient from the semantic `danger` tile "+
			"and from any modifier below the large-text bar; a second site bypasses it.",
			brandOptInClass, n)
	}
}
