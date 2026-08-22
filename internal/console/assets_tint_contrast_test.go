package console

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"testing"
)

// The #1421 tint palette's ONE hard constraint, as a measurement rather than a
// promise: every -deep text partner holds >= 4.5:1 on its own tint and on
// white. The site's own deep values were measured FAILING this (3.68-3.84:1),
// which is why the console carries darkened partners instead of copying the
// site's — so a well-meaning "sync the colors with the site" edit is exactly
// the change this test exists to catch.
//
// The pairs are read from style.css itself, not restated here: a hardcoded
// copy of the hex values would go green when the file changes.
func TestTintDeepPartnersHoldTheAAFloor(t *testing.T) {
	css := readStyleCSS(t)
	for _, hue := range []string{"violet", "sun", "pink", "orange"} {
		deep, tint := cssHexToken(t, css, hue+"-deep"), cssHexToken(t, css, hue+"-tint")
		for surface, bg := range map[string]string{"tint": tint, "white": "#FFFFFF"} {
			if r := wcagRatioHex(deep, bg); r < 4.5 {
				t.Errorf("--%s-deep %s on %s (%s) = %.2f:1, below the 4.5:1 floor. The site's "+
					"palette fails this on purpose-built marketing surfaces; the console's text "+
					"discipline is the part of #1421 that was non-negotiable.", hue, deep, surface, bg, r)
			}
		}
	}
}

// The BODY TEXT that stands on a tint — not just the decorative pills. The
// first cut of this file asserted only the -deep partners while --ink-3 data
// text (the PK and changed-columns cells) sat on the violet tint at 4.20:1;
// review measured it, and the fix re-pointed that text to --ink-2. This pins
// the tokens each tint ground actually carries: --ink-2 must clear the floor
// on both structure tints (the violet panel's data text), and --ink-3 on sun
// (the coverage footer). --ink-3 on violet is deliberately NOT asserted — it
// FAILS (4.20), which is exactly why nothing may use it there.
func TestTintGroundsHoldTheAAFloorForTheirBodyText(t *testing.T) {
	css := readStyleCSS(t)
	ink2 := cssOklchToken(t, css, "ink-2")
	ink3 := cssOklchToken(t, css, "ink-3")
	violet := cssHexToken(t, css, "violet-tint")
	sun := cssHexToken(t, css, "sun-tint")
	for _, c := range []struct {
		ink, name, tint, ground string
	}{
		{ink2, "ink-2", violet, "violet-tint"},
		{ink2, "ink-2", sun, "sun-tint"},
		{ink3, "ink-3", sun, "sun-tint"},
	} {
		if r := wcagRatioHex(c.ink, c.tint); r < 4.5 {
			t.Errorf("--%s (%s) on --%s (%s) = %.2f:1, below the 4.5:1 floor — body text on "+
				"this tint ground must hold AA, or the tint has to lighten", c.name, c.ink, c.ground, c.tint, r)
		}
	}
}

// The tint-aware dividers: --line-soft measures 1.01 on the violet tint —
// visually identical, the whole Recent-changes list rendering as one block —
// which is why each tint carries its own divider token. 1.15 is the floor
// (--line-soft on white, the baseline hairline idiom, sits at 1.17).
func TestTintDividersStayVisible(t *testing.T) {
	css := readStyleCSS(t)
	for _, hue := range []string{"violet", "sun"} {
		line, tint := cssHexToken(t, css, hue+"-line"), cssHexToken(t, css, hue+"-tint")
		if r := wcagRatioHex(line, tint); r < 1.15 {
			t.Errorf("--%s-line %s on --%s-tint %s = %.3f:1, under the 1.15 hairline floor — "+
				"the rows lose their separation on the tinted panel", hue, line, hue, tint, r)
		}
	}
}

// Sun must not BE the warning ground. The site's butter (#FFF6D8) measured
// 1.008 against --ochre-bg — the same color — so the Activity-by-table panel
// wore the console's warn register. 1.02 refuses identity while allowing the
// registers to stay neighbors; warn surfaces still must never nest inside a
// sun card (the modifier block's comment carries that rule).
func TestSunTintIsNotTheWarnGround(t *testing.T) {
	css := readStyleCSS(t)
	sun := cssHexToken(t, css, "sun-tint")
	ochreBG := cssOklchToken(t, css, "ochre-bg")
	if r := wcagRatioHex(sun, ochreBG); r < 1.02 {
		t.Errorf("--sun-tint %s vs --ochre-bg (%s) = %.3f:1 — the data tint and the warning "+
			"ground have collapsed into one color", sun, ochreBG, r)
	}
}

func readStyleCSS(t *testing.T) []byte {
	t.Helper()
	css, err := os.ReadFile("assets/style.css")
	if err != nil {
		t.Fatal(err)
	}
	return css
}

func cssHexToken(t *testing.T, css []byte, name string) string {
	t.Helper()
	m := regexp.MustCompile(`--` + name + `:\s*(#[0-9A-Fa-f]{6})`).FindSubmatch(css)
	if m == nil {
		t.Fatalf("--%s not found as a hex literal in style.css — if it moved to another "+
			"notation, teach this test to read it rather than deleting the assertion", name)
	}
	return string(m[1])
}

// cssOklchToken reads an oklch(L C H) token and converts it to sRGB hex, so
// the ink ramp (declared in oklch) can be measured against the tint grounds
// (declared in hex) with the same WCAG arithmetic.
func cssOklchToken(t *testing.T, css []byte, name string) string {
	t.Helper()
	m := regexp.MustCompile(`--` + name + `:\s*oklch\(([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\)`).FindSubmatch(css)
	if m == nil {
		t.Fatalf("--%s not found as an oklch() literal in style.css — if it moved to another "+
			"notation, teach this test to read it rather than deleting the assertion", name)
	}
	f := func(b []byte) float64 {
		v, err := strconv.ParseFloat(string(b), 64)
		if err != nil {
			t.Fatalf("--%s: bad oklch component %q", name, b)
		}
		return v
	}
	return oklchToHex(f(m[1]), f(m[2]), f(m[3]))
}

// oklchToHex implements the standard OKLab→linear-sRGB matrices (Björn
// Ottosson's reference constants, the same ones the browser uses), clamped
// and gamma-encoded to 8-bit hex.
func oklchToHex(l, c, h float64) string {
	hr := h * math.Pi / 180
	a, bb := c*math.Cos(hr), c*math.Sin(hr)
	l_ := l + 0.3963377774*a + 0.2158037573*bb
	m_ := l - 0.1055613458*a - 0.0638541728*bb
	s_ := l - 0.0894841775*a - 1.2914855480*bb
	ll, mm, ss := l_*l_*l_, m_*m_*m_, s_*s_*s_
	r := 4.0767416621*ll - 3.3077115913*mm + 0.2309699292*ss
	g := -1.2684380046*ll + 2.6097574011*mm - 0.3413193965*ss
	b2 := -0.0041960863*ll - 0.7034186147*mm + 1.7076147010*ss
	enc := func(v float64) int {
		v = math.Max(0, math.Min(1, v))
		if v <= 0.0031308 {
			v *= 12.92
		} else {
			v = 1.055*math.Pow(v, 1/2.4) - 0.055
		}
		return int(math.Round(v * 255))
	}
	return fmt.Sprintf("#%02X%02X%02X", enc(r), enc(g), enc(b2))
}

func wcagRatioHex(a, b string) float64 {
	la, lb := relLum(a), relLum(b)
	if la < lb {
		la, lb = lb, la
	}
	return (la + 0.05) / (lb + 0.05)
}

func relLum(hex string) float64 {
	var lin [3]float64
	for i := 0; i < 3; i++ {
		v, err := strconv.ParseUint(hex[1+2*i:3+2*i], 16, 8)
		if err != nil {
			panic(fmt.Sprintf("bad hex %q", hex))
		}
		c := float64(v) / 255
		if c <= 0.04045 {
			lin[i] = c / 12.92
		} else {
			lin[i] = math.Pow((c+0.055)/1.055, 2.4)
		}
	}
	return 0.2126*lin[0] + 0.7152*lin[1] + 0.0722*lin[2]
}
