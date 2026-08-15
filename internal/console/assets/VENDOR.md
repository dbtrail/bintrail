# Vendored frontend assets

The bintrail console frontend (`index.html`, `app.js`, `style.css`) is written
in vanilla HTML, CSS, and JavaScript with **zero third-party code
dependencies** — no frameworks, no bundler, no Node build step. The files are
embedded directly into the Go binary via `//go:embed` (see `../assets.go`).

This keeps `make build` a pure Go build (CGO for DuckDB, no JS toolchain) and
keeps the supply chain trivial to audit: everything served to the browser lives
in this directory and is reviewed in-tree.

## Fonts (`fonts/`)

The console loads its three brand typefaces from vendored, subset woff2 files —
never from a CDN. The console makes zero external requests by design, so the
`@font-face` rules in `style.css` reference only these embedded files (with the
`system-ui` stack as fallback via `font-display: swap`).

| File | Family | Style | Size |
|---|---|---|---|
| `bricolage-grotesque-latin.woff2` | Bricolage Grotesque v1.001 (Google Fonts v9) | variable: opsz 12–96, wght 200–800 | 71 KB |
| `geist-latin.woff2` | Geist v1.800 (Google Fonts v5) | variable: wght 100–900 | 26 KB |
| `ibm-plex-mono-400-latin.woff2` | IBM Plex Mono v2.3 (Google Fonts v20) | 400 | 9 KB |
| `ibm-plex-mono-500-latin.woff2` | IBM Plex Mono v2.3 (Google Fonts v20) | 500 | 9 KB |
| `ibm-plex-mono-600-latin.woff2` | IBM Plex Mono v2.3 (Google Fonts v20) | 600 | 9 KB |

Provenance: downloaded from Google Fonts' static woff2 endpoints
(`fonts.gstatic.com`, latin unicode-range files), then further subset with
fonttools `pyftsubset` (`--flavor=woff2 --layout-features='*'`) to basic latin
plus common punctuation:

```
U+0020-007E, U+00A0-00FF, U+0131, U+0152-0153, U+2013-2014, U+2018-2019,
U+201C-201D, U+2022, U+2026, U+2039-203A, U+2212, U+2713
```

The same range is declared as `unicode-range` on each `@font-face`; glyphs
outside it (non-latin data values) render in the system fallback stack.
Italic faces are not vendored — the two italic uses in the UI (SQL comments,
NULL placeholders) render as synthesized oblique.

License: all three families are licensed under the **SIL Open Font License 1.1**
(OFL-1.1, <https://openfontlicense.org>), which permits redistribution and
subsetting, including bundling into a binary, provided the fonts are not sold
by themselves. Copyright holders:

- Bricolage Grotesque — © 2022 The Bricolage Grotesque Project Authors
  (<https://github.com/ateliertriay/bricolage>)
- Geist — © 2023 Vercel, in collaboration with basement.studio
  (<https://github.com/vercel/geist-font>)
- IBM Plex Mono — © 2017 IBM Corp. (<https://github.com/IBM/plex>)

If any other third-party asset is ever added, list it here with its name,
version, license, and source URL, and confirm its license permits
redistribution.
