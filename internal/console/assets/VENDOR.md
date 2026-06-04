# Vendored frontend assets

**None.**

The bintrail console frontend (`index.html`, `app.js`, `style.css`) is written
in vanilla HTML, CSS, and JavaScript with **zero third-party dependencies** — no
frameworks, no bundler, no Node build step. The files are embedded directly into
the Go binary via `//go:embed` (see `../assets.go`).

This keeps `make build` a pure Go build (CGO for DuckDB, no JS toolchain) and
keeps the supply chain trivial to audit: everything served to the browser lives
in this directory and is reviewed in-tree.

If a third-party asset is ever added, list it here with its name, version,
license, and source URL, and confirm its license permits redistribution.
