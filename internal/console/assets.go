package console

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// assetsFS embeds the static frontend (HTML/CSS/JS + brand images). The
// console ships as a single Go binary with no Node build step — these files
// are vanilla and dependency-free (see assets/VENDOR.md). Only the served
// files are embedded; VENDOR.md stays as source-tree documentation and is
// not exposed.
//
//go:embed assets/index.html assets/app.js assets/style.css assets/logo.png assets/favicon.png
var assetsFS embed.FS

// assetHandler serves the embedded frontend rooted at the assets/ directory,
// so "/" resolves to index.html and "/app.js", "/style.css" resolve directly.
//
// The SPA routes with history.pushState ("/events", "/recover", …), so a
// reload or deep link arrives here as a path that is not an embedded file.
// Those get the index.html shell and the frontend router restores the view
// (routeFromLocation in app.js). The fallback is limited to paths whose last
// segment has no extension: a missing real asset ("/favicon.ico", a stale
// "/app.js.map") must stay a 404, not silently become HTML. Treating any
// Stat error as "not a file" is safe only on embed.FS, which has no
// ErrPermission/transient-IO class — revisit if sub ever comes from disk.
func assetHandler() http.Handler {
	sub, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		// Unreachable: the embed directive guarantees the directory exists.
		panic(err)
	}
	files := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if p != "" && !strings.Contains(path.Base(p), ".") {
			if _, err := fs.Stat(sub, p); err != nil {
				http.ServeFileFS(w, r, sub, "index.html")
				return
			}
		}
		files.ServeHTTP(w, r)
	})
}
