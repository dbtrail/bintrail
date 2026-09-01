package console

import (
	"regexp"
	"strings"
	"testing"
)

// The newest backup row is the one tinted row on the Backups page, and the
// tint is load-bearing: it marks the copy Time-travel and a restore actually
// read. Everything the row carries therefore sits on --violet-tint instead of
// on the page ground, and three of its children were below their floor the
// moment the tint went on: the age at --ink-3 (4.20:1), the location chip and
// the chevron at --ink-4 (2.80:1), plus a pill whose --surface-3 ground on
// violet is 1.09:1 -- the mark was invisible on the only row that wears it.
//
// TestTintGroundsHoldTheAAFloorForTheirBodyText already measures the TOKENS
// and its comment says outright that --ink-3 on violet fails "which is
// exactly why nothing may use it there". Nothing checked that no SELECTOR
// used it there, so every one of these shipped green. This is that check:
// it walks what the row actually renders and resolves the cascade the way
// the browser does, overrides first.
const latestRowGround = "violet-tint"

// What the newest row renders. Kept explicit because one of these is NOT
// visible from the row builder: .bk-where is produced by backupWhereChip(),
// several hundred lines away, and a regex over the builder block would miss
// exactly the child that was worst off. latestRowChildrenAreAllListed below
// is the anti-rot half -- a class appended directly to the row fails until
// it is named here.
var latestRowChildren = []string{
	"tag-pill",  // "Newest"
	"stg-name",  // the timestamp
	"stg-rel",   // the age
	"stg-dest",  // tables / binlog coordinates
	"chip-mon",  // staleness, newest row only
	"bk-where",  // from backupWhereChip()
	"bk-chev",   // the expand affordance
}

func TestNewestBackupRowClearsItsFloorOnTheTint(t *testing.T) {
	css := string(readStyleCSS(t))
	ground := cssHexToken(t, []byte(css), latestRowGround)
	for _, class := range latestRowChildren {
		tok := effectiveDecl(css, class, "color")
		if tok == "" {
			continue // inherits from the row; the row's own ink is covered by the tint-token test
		}
		// A child may bring its own ground (the pill does, and must: see the
		// .tcard-violet precedent). Measure against whatever it actually sits
		// on, not against the row, or the correct fix reads as a failure.
		ground, groundName := ground, latestRowGround
		if bg := effectiveDecl(css, class, "background"); bg != "" {
			ground, groundName = anyToken(t, css, bg), bg
		}
		if r := wcagRatioHex(anyToken(t, css, tok), ground); r < 4.5 {
			t.Errorf(".stg-row-latest .%s renders var(--%s) on --%s = %.2f:1, below the 4.5:1 "+
				"floor. The tint is not optional (it marks the copy in use), so the fix is to "+
				"override this child under .stg-row-latest, not to drop the tint.", class, tok, groundName, r)
		}
	}
}

// Hover used to swap the row to --surface-2, which is LIGHTER than the tint:
// pointing at the newest row removed the one thing that distinguished it.
func TestNewestBackupRowKeepsItsTintOnHover(t *testing.T) {
	css := string(readStyleCSS(t))
	rule := ruleBody(css, ".stg-row-latest.bk-expandable:hover")
	if rule == "" {
		t.Fatal("no hover rule scoped to .stg-row-latest: the generic .bk-expandable:hover sets " +
			"background:var(--surface-2), which is lighter than --violet-tint, so hovering the " +
			"newest row erases the mark the tint exists to carry")
	}
	if !strings.Contains(rule, "var(--"+latestRowGround+")") {
		t.Errorf("the newest row's hover no longer repaints --%s: %q", latestRowGround, rule)
	}
}

// Anti-rot: every class literal appended to the row must be covered above.
func TestNewestBackupRowChildrenAreAllListed(t *testing.T) {
	js := readAsset(t, "app.js")
	// Anchored on stg-row-latest, not on stg-row: the latter is a shared list
	// class and starting there swept in every earlier panel on the page.
	start := strings.Index(js, `"stg-row" + (idx === 0 ? " stg-row-latest"`)
	end := -1
	if start >= 0 {
		if rel := strings.Index(js[start:], "list.append(row, detail);"); rel >= 0 {
			end = start + rel
		}
	}
	if start < 0 || end <= start {
		t.Fatal("could not find the backups row builder in app.js; if it moved, re-point this test " +
			"rather than deleting it")
	}
	known := map[string]bool{}
	for _, c := range latestRowChildren {
		known[c] = true
	}
	for _, m := range regexp.MustCompile(`class: "([^"]+)"`).FindAllStringSubmatch(js[start:end], -1) {
		for _, c := range strings.Fields(m[1]) {
			if c == "stg-row" || c == "stg-row-latest" || c == "bk-detail" ||
				c == "mono" || c == "chip" || c == "bk-expandable" || known[c] {
				continue
			}
			t.Errorf("the newest backup row renders .%s, which no contrast check covers. Add it to "+
				"latestRowChildren so its colour is measured against the tint.", c)
		}
	}
}

// effectiveDecl resolves prop for class the way the cascade does: an override
// scoped under .stg-row-latest wins over the class's own rule. Returns the
// token name, or "" when the class never sets prop (it inherits, and the row
// itself is checked by the tint-token test).
func effectiveDecl(css, class, prop string) string {
	for _, sel := range []string{`.stg-row-latest .` + class, `.` + class} {
		if body := ruleBody(css, sel); body != "" {
			if m := regexp.MustCompile(prop + `:\s*var\(--([a-z0-9-]+)\)`).FindStringSubmatch(body); m != nil {
				return m[1]
			}
		}
	}
	return ""
}

// ruleBody returns the declarations of the first rule whose selector list
// contains sel as a whole selector.
func ruleBody(css, sel string) string {
	// Comments are stripped first: splitting raw CSS on "}" leaves the comment
	// that documents a rule glued to the front of its selector, so every
	// commented rule reads as absent -- which is how this test first reported
	// a defect that had already been fixed two lines above it.
	css = regexp.MustCompile(`(?s)/\*.*?\*/`).ReplaceAllString(css, "")
	for _, block := range strings.Split(css, "}") {
		open := strings.Index(block, "{")
		if open < 0 {
			continue
		}
		for _, s := range strings.Split(block[:open], ",") {
			if strings.TrimSpace(s) == sel {
				return block[open+1:]
			}
		}
	}
	return ""
}

// anyToken reads a token that may be written as either oklch() or hex.
func anyToken(t *testing.T, css, name string) string {
	t.Helper()
	if regexp.MustCompile(`--` + name + `:\s*oklch\(`).MatchString(css) {
		return cssOklchToken(t, []byte(css), name)
	}
	return cssHexToken(t, []byte(css), name)
}

// The pill is NOT a contrast case: its default ink-2 on surface-3 is 6.74:1
// and always read fine. What it loses on the tint is its edge, and white
// barely improves that (1.09 -> 1.18). It takes the white ground for one
// reason only, and it is the reason this test exists: .tcard-violet .tag-pill
// already does, and the same pill on the same tint must not have two looks.
// A ratio assertion cannot express that, so it is checked structurally.
func TestNewestBackupRowPillMatchesTheOtherVioletPill(t *testing.T) {
	css := string(readStyleCSS(t))
	want := ruleBody(css, ".tcard-violet .tag-pill")
	if want == "" {
		t.Fatal("no .tcard-violet .tag-pill rule: this test compares the newest backup row's pill " +
			"against it, so if that rule moved, re-point this rather than deleting it")
	}
	got := ruleBody(css, ".stg-row-latest .tag-pill")
	if got == "" {
		t.Fatal("the newest backup row's pill no longer overrides its ground. It sits on the same " +
			"--violet-tint as the .tcard-violet pill and must look the same there")
	}
	if norm(got) != norm(want) {
		t.Errorf("the two pills on --violet-tint have drifted apart:\n  .tcard-violet    %s\n  .stg-row-latest  %s", norm(want), norm(got))
	}
}

func norm(decls string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(decls, "\n", " ")), " ")
}
