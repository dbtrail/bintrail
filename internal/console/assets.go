package console

import (
	"embed"
	"io/fs"
	"net/http"
)

// assetsFS embeds the static frontend (HTML/CSS/JS). The console ships as a
// single Go binary with no Node build step — these files are vanilla and
// dependency-free (see assets/VENDOR.md). Only the three served files are
// embedded; VENDOR.md stays as source-tree documentation and is not exposed.
//
//go:embed assets/index.html assets/app.js assets/style.css
var assetsFS embed.FS

// assetHandler serves the embedded frontend rooted at the assets/ directory,
// so "/" resolves to index.html and "/app.js", "/style.css" resolve directly.
func assetHandler() http.Handler {
	sub, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		// Unreachable: the embed directive guarantees the directory exists.
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}
