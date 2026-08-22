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
// white. The site's own deep values were measured FAILING this (3.7-3.8:1),
// which is why the console carries darkened partners instead of copying the
// site's — so a well-meaning "sync the colors with the site" edit is exactly
// the change this test exists to catch.
//
// The pairs are read from style.css itself, not restated here: a hardcoded
// copy of the hex values would go green when the file changes.
func TestTintDeepPartnersHoldTheAAFloor(t *testing.T) {
	css, err := os.ReadFile("assets/style.css")
	if err != nil {
		t.Fatal(err)
	}
	token := func(name string) string {
		m := regexp.MustCompile(`--` + name + `:\s*(#[0-9A-Fa-f]{6})`).FindSubmatch(css)
		if m == nil {
			t.Fatalf("--%s not found as a hex literal in style.css — if it moved to another "+
				"notation, teach this test to read it rather than deleting the assertion", name)
		}
		return string(m[1])
	}
	for _, hue := range []string{"violet", "sun", "pink", "orange"} {
		deep, tint := token(hue+"-deep"), token(hue+"-tint")
		for surface, bg := range map[string]string{"tint": tint, "white": "#FFFFFF"} {
			if r := wcagRatioHex(deep, bg); r < 4.5 {
				t.Errorf("--%s-deep %s on %s (%s) = %.2f:1, below the 4.5:1 floor. The site's "+
					"palette fails this on purpose-built marketing surfaces; the console's text "+
					"discipline is the part of #1421 that was non-negotiable.", hue, deep, surface, bg, r)
			}
		}
	}
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
