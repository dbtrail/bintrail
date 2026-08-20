package console

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

var navRouteRE = regexp.MustCompile(`data-route="([a-z-]+)"`)

// Every nav entry must name a route the router knows.
//
// The failure this prevents is silent: navigate() falls back to "overview" for
// an unknown route, so a sidebar link with a typo'd or retired data-route
// still renders a page — the wrong one — with no error anywhere. #1384 added
// two routes across two files, which is exactly the shape that drifts.
func TestNavEntriesNameKnownRoutes(t *testing.T) {
	html, err := os.ReadFile("assets/index.html")
	if err != nil {
		t.Fatal(err)
	}
	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}

	routes := parseRoutesConst(t, string(js))
	dispatch := string(js)

	matches := navRouteRE.FindAllStringSubmatch(string(html), -1)
	if len(matches) == 0 {
		t.Fatal("no data-route attributes found in assets/index.html — the selector this guard " +
			"depends on changed, so it is no longer checking anything")
	}
	for _, m := range matches {
		route := m[1]
		if !routes[route] {
			t.Errorf("nav entry data-route=%q is not in the ROUTES list in app.js — navigate() "+
				"silently falls back to Overview, so this link would render the wrong page with no error", route)
		}
		// A route in ROUTES with no dispatch arm renders Overview too, via the
		// switch default. Both halves must exist for a nav entry to work.
		if !strings.Contains(dispatch, `case "`+route+`":`) {
			t.Errorf("route %q has a nav entry but no case in renderRoute's switch — it would "+
				"fall through to the default and render Overview", route)
		}
	}

	// The Protect group specifically: it is the one whose panels were moved out
	// of another view, so a half-revert (nav removed, routes left, or vice
	// versa) is plausible.
	for _, want := range []string{"baselines", "verification"} {
		if !routes[want] {
			t.Errorf("route %q is missing from ROUTES", want)
		}
		if !strings.Contains(string(html), `data-route="`+want+`"`) {
			t.Errorf("no nav entry for route %q — the view exists but nothing links to it", want)
		}
	}
}

// parseRoutesConst reads the ROUTES array literal out of app.js. Parsing the
// source rather than hardcoding the list keeps this guard honest: a hardcoded
// copy would drift from the thing it claims to check.
func parseRoutesConst(t *testing.T, js string) map[string]bool {
	t.Helper()
	const marker = "const ROUTES = ["
	i := strings.Index(js, marker)
	if i < 0 {
		t.Fatal("could not find the ROUTES declaration in assets/app.js")
	}
	rest := js[i+len(marker):]
	j := strings.Index(rest, "]")
	if j < 0 {
		t.Fatal("unterminated ROUTES array in assets/app.js")
	}
	out := map[string]bool{}
	for _, part := range strings.Split(rest[:j], ",") {
		// Strip whitespace, comments and quotes. Entries may be spread over
		// several lines with // comments between them.
		for _, line := range strings.Split(part, "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "//") {
				continue
			}
			if name := strings.Trim(line, `"' `); name != "" {
				out[name] = true
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("parsed zero routes from the ROUTES array — the parser broke, not the code")
	}
	return out
}
