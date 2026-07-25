#!/usr/bin/env bash
# validate-mcpb.sh — sanity-check one or more .mcpb bundles.
#
# For each bundle: unzip it, parse manifest.json, verify the required manifest
# fields, and check the declared entry-point binary exists in the archive and
# is executable. Runs in the release pipeline (called from build-mcpb.sh, so a
# malformed bundle fails the release) and standalone via `make validate-mcpb`.
#
# Requires: unzip, python3 (both present on GitHub ubuntu runners and macOS).
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <bundle.mcpb> [...]" >&2
    exit 2
fi

fail=0
for BUNDLE in "$@"; do
    if [ ! -f "$BUNDLE" ]; then
        echo "validate-mcpb: FAIL $BUNDLE: file not found" >&2
        fail=1
        continue
    fi

    WORK="$(mktemp -d)"
    if ! unzip -q "$BUNDLE" -d "$WORK"; then
        echo "validate-mcpb: FAIL $BUNDLE: not a valid zip archive" >&2
        rm -rf "$WORK"
        fail=1
        continue
    fi

    if ERR=$(python3 - "$WORK" "$BUNDLE" 2>&1 <<'PYEOF'
import json, os, stat, sys

work, bundle = sys.argv[1], sys.argv[2]
manifest_path = os.path.join(work, "manifest.json")
if not os.path.isfile(manifest_path):
    sys.exit("manifest.json missing from bundle root")

with open(manifest_path) as f:
    m = json.load(f)

for field in ("manifest_version", "name", "version", "description", "author", "server"):
    if field not in m:
        sys.exit(f"manifest.json missing required field: {field}")

server = m["server"]
if server.get("type") != "binary":
    sys.exit(f"server.type = {server.get('type')!r}, want 'binary'")

entry = server.get("entry_point")
if not entry:
    sys.exit("server.entry_point missing")
entry_path = os.path.join(work, entry)
if not os.path.isfile(entry_path):
    sys.exit(f"entry point binary not in bundle: {entry}")
if not entry.endswith(".exe") and not os.stat(entry_path).st_mode & stat.S_IXUSR:
    sys.exit(f"entry point binary not executable: {entry}")

cfg = server.get("mcp_config", {})
args = cfg.get("args", [])
for required_arg in ("--connect", "${user_config.console_url}", "--token", "${user_config.token}"):
    if required_arg not in args:
        sys.exit(f"mcp_config.args missing {required_arg!r}: {args}")

# The declared tool list must at least be well-formed. Whether it MATCHES the
# server's registered tools is asserted by TestMCPBManifestToolsMatchServer in
# cmd/bintrail-mcp (a Go unit test): the bundled binary is the stdio<->HTTP
# BRIDGE, so enumerating tools from here would mean standing up a live server
# and index inside the release pipeline.
tools = m.get("tools")
if not isinstance(tools, list) or not tools:
    sys.exit("manifest.json must declare a non-empty tools list")
for i, t in enumerate(tools):
    if not isinstance(t, dict) or not t.get("name") or not t.get("description"):
        sys.exit(f"tools[{i}] must have a non-empty name and description: {t!r}")

uc = m.get("user_config", {})
for key in ("console_url", "token"):
    if key not in uc:
        sys.exit(f"user_config missing {key!r}")
    if not uc[key].get("required"):
        sys.exit(f"user_config.{key} must be required")
if not uc["token"].get("sensitive"):
    sys.exit("user_config.token must be sensitive")

if "__MCPB_" in json.dumps(m):
    sys.exit("unsubstituted template placeholder left in manifest")
PYEOF
    ); then
        echo "validate-mcpb: OK   $BUNDLE"
    else
        echo "validate-mcpb: FAIL $BUNDLE: $ERR" >&2
        fail=1
    fi
    rm -rf "$WORK"
done
exit $fail
