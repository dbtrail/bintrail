package console

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// motionSection returns the #1385 motion block of style.css, bounded at the
// next section header.
//
// Bounded, not "to end of file": an unbounded tail would silently extend both
// guards over any section appended later, so a perfectly ordinary future
// section that sets `background:` would fail the color guard here.
//
// Scoped to this section rather than the whole stylesheet, and that is
// deliberate — though for a different reason than when it was written. The
// original reason (style.css honoured reduced motion three inconsistent ways,
// so a whole-file checker cried wolf) is gone: #1392 settled on one shape plus
// a narrow reduce-block exception, and TestReducedMotionHasOneShape now audits
// the entire file.
//
// What survives is a STRICTER local rule. The whole-file guard tolerates short
// geometry transitions anywhere, because a 120ms hover nudge is feedback and
// banning it outright is a rule nobody follows. This section exists for no
// reason except to add decorative motion, so here EVERY transition is motion —
// any duration, and colour and opacity too, both of which the whole-file guard
// ignores by design — and none may sit outside the guard.
func motionSection(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("assets/style.css")
	if err != nil {
		t.Fatal(err)
	}
	css := string(data)
	const marker = "Motion: chrome only (#1385)"
	i := strings.Index(css, marker)
	if i < 0 {
		t.Fatal("the #1385 motion section header is gone — this guard no longer covers anything")
	}
	// Only the FIRST marker was found before, so a second banner further down
	// sat outside both guards entirely.
	if strings.Count(css, marker) > 1 {
		t.Fatalf("the %q marker appears %d times — a duplicate puts everything under the later "+
			"one outside these guards", marker, strings.Count(css, marker))
	}
	// Back up to the banner that OPENS this section: the marker sits inside a
	// /* … */ block, so slicing at the marker leaves an unterminated comment
	// whose header prose then reaches the property matcher as if it were code.
	if b := strings.LastIndex(css[:i], "/* ======"); b >= 0 {
		i = b
	}
	section := css[i:]
	// The next banner, if any, ends this section — without this bound both
	// guards would silently extend over every section appended later.
	if j := strings.Index(section[1:], "/* ======"); j > 0 {
		section = section[:j+1]
	}
	// Stripped ONCE, here, so both guards see the same code-only text. It also
	// removes the phantom-range hazard: a comment quoting the no-preference
	// at-rule used to make the brace scanner treat the next `{` as a block.
	return stripCSSComments(section)
}

// animDeclRE matches what can START motion: the animation shorthand, its -name
// longhand (you cannot run an animation without one of the two), and
// `transition`. Transition was the hole this section argues about at length in
// its own comment and then did not enforce — a transition IS animation, and
// moving the tile's transition outside the guard passed green.
var animDeclRE = regexp.MustCompile(`(animation(-name)?|transition)\s*:`)

// Every animation this section adds must sit inside its reduced-motion guard.
//
// The failure mode is invisible to the author: anyone whose own OS setting is
// "no preference" — nearly everyone — sees an unguarded rule work perfectly.
// The console is opened during incidents, so honouring the setting is not a
// nicety.
func TestMotionSectionIsReducedMotionGuarded(t *testing.T) {
	section := motionSection(t)

	guarded := noPreferenceRanges(t, section)
	if len(guarded) == 0 {
		t.Fatal("the #1385 motion section has no prefers-reduced-motion: no-preference block")
	}

	for _, m := range animDeclRE.FindAllStringIndex(section, -1) {
		if inRanges(guarded, m[0]) {
			continue
		}
		line := strings.TrimSpace(lineAt(section, m[0]))
		t.Errorf("the #1385 motion section animates outside its reduced-motion guard:\n  %s\n"+
			"Move the rule inside @media (prefers-reduced-motion: no-preference).", line)
	}
}

// noPreferenceRanges returns the byte ranges of EVERY no-preference block in
// the section.
//
// Two failures the single-block version had, both demonstrated by review:
// it took only the FIRST such block, so adding a second — an ordinary edit —
// reported its contents as unguarded; and if the brace scan never returned to
// depth 0 it left `end` at the section end, at which point nothing could be
// flagged and the guard failed OPEN. One `{` inside a comment or a
// `content: "{"` string was enough. Unbalanced braces are now a hard failure
// rather than a silent pass.
func noPreferenceRanges(t *testing.T, section string) [][2]int {
	t.Helper()
	const needle = "@media (prefers-reduced-motion: no-preference)"
	var out [][2]int
	for i := 0; ; {
		j := strings.Index(section[i:], needle)
		if j < 0 {
			return out
		}
		start := i + j
		open := strings.Index(section[start:], "{")
		if open < 0 {
			t.Fatalf("no-preference block at offset %d has no opening brace", start)
		}
		depth, k := 0, start+open
		closed := false
		for ; k < len(section); k++ {
			switch section[k] {
			case '{':
				depth++
			case '}':
				depth--
				if depth == 0 {
					closed = true
				}
			}
			if closed {
				break
			}
		}
		if !closed {
			t.Fatalf("unbalanced braces in the no-preference block at offset %d — this guard "+
				"cannot tell guarded from unguarded here, so it fails loudly instead of passing", start)
		}
		out = append(out, [2]int{start, k})
		i = k + 1
	}
}

func inRanges(ranges [][2]int, pos int) bool {
	for _, r := range ranges {
		if pos >= r[0] && pos <= r[1] {
			return true
		}
	}
	return false
}

func lineAt(s string, pos int) string {
	start := strings.LastIndexByte(s[:pos], '\n') + 1
	end := strings.IndexByte(s[pos:], '\n')
	if end < 0 {
		return s[start:]
	}
	return s[start : pos+end]
}

// The motion section must not move color. The brand palette's "never encode
// data" boundary and the semantic INSERT/UPDATE/DELETE tokens are decided in
// :root; a decorative rule repainting a data surface would undo that quietly,
// and "make it feel alive" is exactly the change that invites it.
//
// box-shadow is the documented exception: it carries a color but paints depth,
// not meaning.
func TestMotionSectionAnimatesGeometryOnly(t *testing.T) {
	// motionSection already strips comments — these needles are PROPERTY NAMES
	// and the section's own commentary explains why several are absent (the
	// note about not using opacity contains the literal "opacity:").
	section := motionSection(t)

	// `color:` substring-matches every *-color longhand (border-color,
	// outline-color, accent-color, caret-color, text-decoration-color).
	banned := []string{
		"color:",
		"background:", "background-image:",
		"filter:", "backdrop-filter:",
		"text-shadow:", "mix-blend-mode:", "background-blend-mode:", "border-image:",
		// Shorthands that carry a color.
		"border:", "outline:",
	}
	for _, prop := range banned {
		if strings.Contains(section, prop) {
			t.Errorf("the motion section sets %q. It is limited to transform and box-shadow: "+
				"repainting a surface here can silently break the semantic DELETE=red mapping or the "+
				"contrast floors the palette comment pins.", prop)
		}
	}

	// opacity is banned here for a different reason than the colors above, so
	// it gets its own message. Overview replaces tiles as fetches land; a tile
	// whose entrance starts transparent flashes at every replaceWith, and
	// the `rise` keyframes comment already states the invariant — content is
	// ALWAYS visible.
	if strings.Contains(section, "opacity:") {
		t.Error("the motion section animates opacity. Tiles are REPLACED as their fetch lands, so an " +
			"entrance that starts transparent flashes on every swap; keep the entrance transform-only, " +
			"as `rise` is.")
	}

	// The highest-impact miss the property list cannot catch: redefining a
	// custom property here repaints its token EVERYWHERE, which is precisely
	// what this guard exists to prevent.
	// Not anchored to line start: `transform: translateX(2px); --delete: red;`
	// is valid CSS on one line, and an anchored pattern missed it — verified by
	// mutation. `var(--ease)` cannot match because the colon is required.
	if regexp.MustCompile(`--[a-z0-9-]+\s*:`).MatchString(section) {
		t.Error("the motion section redefines a custom property. A `--delete:` or `--panel-bg:` here " +
			"repaints that token across the whole console — the exact failure this guard exists to " +
			"prevent, and invisible to a property-name check.")
	}
}

// stripCSSComments removes /* … */ blocks. CSS has no line comments, so this
// is the whole grammar that matters here.
func stripCSSComments(css string) string {
	var b strings.Builder
	for {
		i := strings.Index(css, "/*")
		if i < 0 {
			b.WriteString(css)
			return b.String()
		}
		b.WriteString(css[:i])
		j := strings.Index(css[i:], "*/")
		if j < 0 {
			return b.String()
		}
		css = css[i+j+2:]
	}
}
