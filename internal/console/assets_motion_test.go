package console

import (
	"os"
	"strings"
	"testing"
)

// motionSection returns the #1385 motion block of style.css.
//
// Scoped to that section on purpose. An earlier version of this guard tried to
// police the WHOLE stylesheet and got it wrong: the file legitimately uses
// three different shapes (a no-preference block, a dedicated reduce block that
// sets animation:none for the same selector, and duration scaling), and a
// checker that knows only the first manufactures findings against correct
// code. A guard that cries wolf is worse than no guard. The pre-existing
// unguarded animations that audit did surface are filed separately rather than
// silently absorbed here.
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
	return css[i:]
}

// Every animation this section adds must sit inside its reduced-motion guard.
//
// The failure mode is invisible to the author: someone whose own OS setting is
// "no preference" — nearly everyone — sees an unguarded rule work perfectly.
// The console is opened during incidents, so honouring the setting is not a
// nicety.
func TestMotionSectionIsReducedMotionGuarded(t *testing.T) {
	section := motionSection(t)

	const guard = "@media (prefers-reduced-motion: no-preference)"
	g := strings.Index(section, guard)
	if g < 0 {
		t.Fatal("the #1385 motion section has no prefers-reduced-motion: no-preference block")
	}
	open := strings.Index(section[g:], "{")
	if open < 0 {
		t.Fatal("malformed no-preference block in the #1385 motion section")
	}
	depth, end := 0, g+open
	for ; end < len(section); end++ {
		if section[end] == '{' {
			depth++
		} else if section[end] == '}' {
			depth--
			if depth == 0 {
				break
			}
		}
	}

	for idx, line := range strings.Split(section, "\n") {
		if !strings.Contains(line, "animation:") {
			continue
		}
		// Offset of this line within the section.
		off := 0
		for i, l := range strings.Split(section, "\n") {
			if i == idx {
				break
			}
			off += len(l) + 1
		}
		if off < g || off > end {
			t.Errorf("the #1385 motion section animates outside its reduced-motion guard:\n  %s\n"+
				"Move the rule inside @media (prefers-reduced-motion: no-preference).", strings.TrimSpace(line))
		}
	}
}

// The motion section must not introduce color. The brand palette's "never
// encode data" boundary and the semantic INSERT/UPDATE/DELETE tokens are
// decided in :root; a decorative rule repainting a data surface would undo
// that quietly, and "make it feel alive" is exactly the change that invites it.
// box-shadow is the documented exception: it carries a color but paints depth,
// not meaning.
func TestMotionSectionAnimatesGeometryOnly(t *testing.T) {
	section := motionSection(t)
	for _, prop := range []string{"color:", "background:", "background-color:", "border-color:", "fill:", "stroke:"} {
		if strings.Contains(section, prop) {
			t.Errorf("the motion section sets %q. It is limited to transform/opacity/shadow: "+
				"repainting a surface here can silently break the semantic DELETE=red mapping or the "+
				"contrast floors the palette comment pins.", prop)
		}
	}
}
