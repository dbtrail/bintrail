package console

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// One `route: "slug",` entry of the DOCS_PAGES literal. Keys are bare route
// names (the ROUTES vocabulary: lowercase, no quotes), values are docs slugs.
var docsPageEntryRE = regexp.MustCompile(`^([a-z]+):\s*"([a-z0-9-]+)",?$`)

// Every page-header Docs link (#1450) must name a page that exists.
//
// The link is a plain <a> to www.dbtrail.com/docs/<slug>/, and the site serves
// the repo's docs/<slug>.md at that path. Nothing at runtime checks the target
// (air-gapped consoles are first class, so the link never fetches), which means
// a renamed or deleted doc would ship as a 404 link with no error anywhere. The
// repo file is the checkable proxy: this walks the table and fails when a file
// is gone. It also pins the wiring, because a table nobody reads guards
// nothing: pageHead must build the link through docsLink, and docsLink must
// read DOCS_PAGES and open a new tab without handing it the opener.
func TestDocsLinksNameExistingPages(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)

	pages := parseDocsPages(t, js)
	routes := parseRoutesConst(t, js)
	docsDir := repoDocsDir(t)

	for route, slug := range pages {
		if !routes[route] {
			t.Errorf("DOCS_PAGES key %q is not in the ROUTES list — pageHead looks the link up by "+
				"route, so this entry can never match and the view it was meant for gets no link", route)
		}
		path := filepath.Join(docsDir, slug+".md")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("DOCS_PAGES[%q] = %q, but %s does not exist (%v): the %s page header would "+
				"link to https://www.dbtrail.com/docs/%s/, a 404", route, slug, path, err, route, slug)
		}
	}

	// Wiring. The table is only useful if the header actually consults it.
	head := jsFunctionBody(t, js, "pageHead")
	if !strings.Contains(head, "docsLink(") {
		t.Error("pageHead no longer calls docsLink(): the DOCS_PAGES table is unread and no " +
			"view carries its Docs link")
	}
	link := jsFunctionBody(t, js, "docsLink")
	for _, want := range []string{"DOCS_PAGES[", `target: "_blank"`, `rel: "noopener"`, "DOCS_BASE +"} {
		if !strings.Contains(link, want) {
			t.Errorf("docsLink() lost %s — the header link must come from the table, open in a new "+
				"tab, and not hand the docs site a window.opener", want)
		}
	}
	if !strings.Contains(js, `const DOCS_BASE = "https://www.dbtrail.com/docs/";`) {
		t.Error("DOCS_BASE is not the /docs/ root of www.dbtrail.com; every slug in DOCS_PAGES " +
			"is checked against docs/<slug>.md on the assumption that the site serves it there")
	}
}

// parseDocsPages reads the DOCS_PAGES object literal out of app.js. Parsing
// the source rather than hardcoding the list keeps this guard honest: a
// hardcoded copy would drift from the thing it claims to check.
//
// Comments are stripped per line BEFORE the closing brace is located, so prose
// in the explanatory comment can neither end the literal early nor be read as
// an entry. Every remaining non-blank line must be an entry the regex can
// read: an entry written in another shape (a quoted key, a computed value)
// would otherwise drop out of the guard silently, and the guard would pass
// while that one link went unchecked.
func parseDocsPages(t *testing.T, js string) map[string]string {
	t.Helper()
	const marker = "const DOCS_PAGES = {"
	i := strings.Index(js, marker)
	if i < 0 {
		t.Fatal("could not find the DOCS_PAGES declaration in assets/app.js — the page-header " +
			"Docs links have no table to check, or it was renamed without moving this guard")
	}
	rest := js[i+len(marker):]
	var code strings.Builder
	for _, line := range strings.Split(rest, "\n") {
		if c := strings.Index(line, "//"); c >= 0 {
			line = line[:c]
		}
		code.WriteString(line)
		code.WriteString("\n")
	}
	body := code.String()
	j := strings.Index(body, "}")
	if j < 0 {
		t.Fatal("unterminated DOCS_PAGES literal in assets/app.js")
	}
	out := map[string]string{}
	for n, line := range strings.Split(body[:j], "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		m := docsPageEntryRE.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("DOCS_PAGES line %d is not a `route: \"slug\",` entry this guard can read: %q "+
				"— rewrite it in that shape or the link it defines is never checked", n+1, line)
		}
		if _, dup := out[m[1]]; dup {
			t.Errorf("DOCS_PAGES names route %q twice", m[1])
		}
		out[m[1]] = m[2]
	}
	if len(out) == 0 {
		t.Fatal("parsed zero entries from DOCS_PAGES — the parser broke, not the code")
	}
	return out
}

// repoDocsDir locates docs/ relative to THIS file, not the working directory:
// `go test` runs the package from its own directory, but a runner that sets
// -C or a wrapper that cds elsewhere would otherwise turn every slug into a
// false "missing" finding.
func repoDocsDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller gave no file path for this test")
	}
	dir := filepath.Join(filepath.Dir(file), "..", "..", "docs")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("docs/ is not two levels above internal/console (%v) — the repo layout moved and "+
			"this guard can no longer find the pages it checks", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".md") {
			return dir
		}
	}
	t.Fatalf("%s holds no .md files — wrong directory, so every slug would read as missing", dir)
	return ""
}
