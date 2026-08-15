#!/usr/bin/env bash
#
# CI guard for the OFL-1.1 metadata inside the vendored console fonts.
#
# internal/console/assets/fonts ships subset woff2 builds of three OFL-1.1
# families, embedded into the bintrail-console binary (#1355). OFL-1.1 §2
# requires every distributed copy to carry both the copyright notice and the
# license; the woff2 name tables are the machine-readable half of that story
# (the human-readable half is the web-fonts section of THIRD-PARTY-NOTICES,
# pinned by scripts/check-notices.sh). The v0.55.0 pipeline silently dropped
# name ID 14 (license URL) because pyftsubset's default --name-IDs keeps only
# IDs 0-6 (#1360). This guard fails if any vendored font loses its license
# name records again, and if a woff2 appears or disappears without this
# list — and therefore VENDOR.md and the notices header — being updated.
#
# Requires python3 only; installs fonttools+brotli into a throwaway venv so it
# behaves the same on a dev machine and on a CI runner.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FONTS_DIR="$REPO_ROOT/internal/console/assets/fonts"

fail() { echo "ERROR: $*" >&2; exit 1; }

# The exact set of vendored fonts. Adding or removing a family means updating
# this list AND internal/console/assets/VENDOR.md AND the web-fonts section of
# scripts/notices-header.txt (then `make notices`).
EXPECTED=(
  bricolage-grotesque-latin.woff2
  geist-latin.woff2
  ibm-plex-mono-400-latin.woff2
  ibm-plex-mono-500-latin.woff2
  ibm-plex-mono-600-latin.woff2
)

for f in "${EXPECTED[@]}"; do
  [ -f "$FONTS_DIR/$f" ] || fail "expected vendored font missing: internal/console/assets/fonts/$f"
done
for f in "$FONTS_DIR"/*.woff2; do
  base="$(basename "$f")"
  known=no
  for e in "${EXPECTED[@]}"; do
    [ "$base" = "$e" ] && known=yes
  done
  [ "$known" = yes ] || fail "unexpected woff2 in fonts dir: $base — update scripts/check-font-licenses.sh, internal/console/assets/VENDOR.md and scripts/notices-header.txt"
done

command -v python3 >/dev/null 2>&1 || fail "python3 not found on PATH (needed to parse woff2 name tables)"

VENV="${TMPDIR:-/tmp}/bintrail-font-license-venv"
if [ ! -x "$VENV/bin/python3" ]; then
  python3 -m venv "$VENV"
fi
# fonttools parses the name table; brotli decompresses woff2 table data.
"$VENV/bin/pip" install --quiet 'fonttools>=4.47,<5' 'brotli>=1.0,<2'

"$VENV/bin/python3" - "$FONTS_DIR" "${EXPECTED[@]}" <<'EOF'
import sys
from fontTools.ttLib import TTFont

fonts_dir, files = sys.argv[1], sys.argv[2:]
failed = False
for name in files:
    nt = TTFont(f"{fonts_dir}/{name}")["name"]
    copyright_ = (nt.getDebugName(0) or "").strip()
    license_desc = (nt.getDebugName(13) or "").strip()
    license_url = (nt.getDebugName(14) or "").strip()
    problems = []
    if not copyright_:
        problems.append("name ID 0 (copyright notice) missing/empty")
    if "SIL Open Font License" not in license_desc:
        problems.append("name ID 13 (license description) missing or not OFL")
    if not license_url:
        problems.append("name ID 14 (license info URL) missing/empty")
    if problems:
        failed = True
        print(f"ERROR: {name}: " + "; ".join(problems), file=sys.stderr)
    else:
        print(f"ok: {name}: name IDs 0/13/14 present ({license_url})")
if failed:
    print(
        "The vendored woff2 files must carry their OFL-1.1 name records "
        "(IDs 0, 13, 14). Re-subset per internal/console/assets/VENDOR.md — "
        "pyftsubset's DEFAULT --name-IDs drops the license fields.",
        file=sys.stderr,
    )
    sys.exit(1)
EOF
