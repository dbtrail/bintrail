#!/usr/bin/env python3
"""Fetch bintrail repo distribution stats from the GitHub API.

Reports release download counts (per version / platform / format), repo
metadata (stars, forks, watchers), and traffic (clones/views, needs a
token with push access). download_count is a CUMULATIVE counter, so the
script keeps local snapshots under ~/.local/state/bintrail-repo-stats/
and reports the delta since the previous run.

Stdlib only. Usage: repo_stats.py [--json] [--no-snapshot]
"""

import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPO = "dbtrail/dbtrail"
API = f"https://api.github.com/repos/{REPO}"
STATE_DIR = Path.home() / ".local" / "state" / "bintrail-repo-stats" / "snapshots"

# Checksums, signatures and SBOMs are fetched by tooling, not humans —
# excluded from the "artifact downloads" headline (raw counts still kept).
NON_ARTIFACT = re.compile(r"(checksums\.txt|\.sbom\.json$|\.sig$|\.pem$)")
# GoReleaser asset naming: bintrail_<version>_<os>_<arch>.<ext>
ASSET_RE = re.compile(r"^bintrail_(?P<ver>[^_]+)_(?P<os>[^_]+)_(?P<arch>[^.]+)\.(?P<ext>.+)$")


def token():
    for var in ("GITHUB_TOKEN", "GH_TOKEN"):
        if os.environ.get(var):
            return os.environ[var]
    try:
        out = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, timeout=5)
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def fetch(url, tok=None):
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    if tok:
        req.add_header("Authorization", f"Bearer {tok}")
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.load(resp)


def fetch_releases():
    releases, page = [], 1
    while True:
        batch = fetch(f"{API}/releases?per_page=100&page={page}")
        if not batch:
            return releases
        releases.extend(batch)
        page += 1


def collect():
    repo = fetch(API)
    stats = {
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "stars": repo["stargazers_count"],
        "forks": repo["forks_count"],
        "watchers": repo["subscribers_count"],  # watchers_count is an alias for stars
        "open_issues_and_prs": repo["open_issues_count"],
        "assets": {},  # "tag/asset_name" -> download_count
        "releases": [],
    }
    for rel in fetch_releases():
        stats["releases"].append({"tag": rel["tag_name"], "published_at": rel["published_at"]})
        for a in rel.get("assets", []):
            stats["assets"][f"{rel['tag_name']}/{a['name']}"] = a["download_count"]

    tok = token()
    if tok:
        try:
            clones = fetch(f"{API}/traffic/clones", tok)
            views = fetch(f"{API}/traffic/views", tok)
            stats["traffic_14d"] = {
                "clones": clones["count"], "unique_cloners": clones["uniques"],
                "views": views["count"], "unique_visitors": views["uniques"],
            }
        except urllib.error.HTTPError:
            pass  # token without push access — traffic needs it
    return stats


def load_previous():
    if not STATE_DIR.is_dir():
        return None
    snaps = sorted(STATE_DIR.glob("*.json"))
    if not snaps:
        return None
    with open(snaps[-1]) as f:
        return json.load(f)


def save_snapshot(stats):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    name = stats["fetched_at"].replace(":", "-") + ".json"
    with open(STATE_DIR / name, "w") as f:
        json.dump(stats, f, indent=1)


def aggregate(assets):
    """Sum artifact downloads by version, os/arch and format."""
    by_version, by_platform, by_format, total = {}, {}, {}, 0
    for key, count in assets.items():
        tag, name = key.split("/", 1)
        if NON_ARTIFACT.search(name):
            continue
        total += count
        by_version[tag] = by_version.get(tag, 0) + count
        m = ASSET_RE.match(name)
        if m:
            plat = f"{m['os']}/{m['arch']}"
            by_platform[plat] = by_platform.get(plat, 0) + count
            by_format[m["ext"]] = by_format.get(m["ext"], 0) + count
    return total, by_version, by_platform, by_format


def fmt_delta(cur, prev):
    if prev is None:
        return ""
    d = cur - prev
    return f" ({'+' if d >= 0 else ''}{d})"


def report(stats, prev):
    total, by_ver, by_plat, by_fmt = aggregate(stats["assets"])
    p_total = p_ver = None
    since = ""
    if prev:
        p_total, p_ver, _, _ = aggregate(prev.get("assets", {}))
        since = f" since {prev['fetched_at'][:16].replace('T', ' ')} UTC"

    lines = [f"# {REPO} stats — {stats['fetched_at'][:16].replace('T', ' ')} UTC", ""]
    lines.append(
        f"**Repo**: {stats['stars']} stars{fmt_delta(stats['stars'], prev and prev.get('stars'))}"
        f" · {stats['forks']} forks · {stats['watchers']} watchers"
        f" · {stats['open_issues_and_prs']} open issues+PRs"
    )
    lines.append(f"**Artifact downloads (all releases)**: {total}{fmt_delta(total, p_total)}{' ' + since if prev else ''}")
    lines.append("_(tar.gz/deb/rpm only — checksums, signatures and SBOMs excluded)_")

    lines.append("\n## By version (top 10 by downloads)")
    for tag, n in sorted(by_ver.items(), key=lambda kv: -kv[1])[:10]:
        lines.append(f"- {tag}: {n}{fmt_delta(n, p_ver.get(tag) if p_ver else None)}")

    if by_plat:
        lines.append("\n## By platform")
        for plat, n in sorted(by_plat.items(), key=lambda kv: -kv[1]):
            lines.append(f"- {plat}: {n}")
    if by_fmt:
        lines.append("\n## By format")
        for ext, n in sorted(by_fmt.items(), key=lambda kv: -kv[1]):
            lines.append(f"- {ext}: {n}")

    if "traffic_14d" in stats:
        t = stats["traffic_14d"]
        lines.append("\n## Traffic (last 14 days)")
        lines.append(f"- Clones: {t['clones']} ({t['unique_cloners']} unique)")
        lines.append(f"- Views: {t['views']} ({t['unique_visitors']} unique)")
    else:
        lines.append("\n_Traffic (clones/views) skipped — needs a GitHub token with push access._")

    lines.append("\n_GHCR docker pulls are not measurable (no public API) — pending Scarf Gateway (telemetry design Phase 0)._")
    if not prev:
        lines.append("_First snapshot saved — deltas will appear from the next run._")
    return "\n".join(lines)


def main():
    args = sys.argv[1:]
    try:
        stats = collect()
    except (urllib.error.URLError, OSError) as e:
        print(f"error: GitHub API unreachable: {e}", file=sys.stderr)
        return 1
    prev = load_previous()
    if "--no-snapshot" not in args:
        save_snapshot(stats)
    if "--json" in args:
        json.dump(stats, sys.stdout, indent=1)
    else:
        print(report(stats, prev))
    return 0


if __name__ == "__main__":
    sys.exit(main())
