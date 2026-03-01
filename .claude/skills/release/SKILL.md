---
name: release
description: Prepares and publishes a new bintrail release — updates CHANGELOG, commits, tags, and pushes. GoReleaser then runs automatically via GitHub Actions.
---

Guide the user through releasing a new version of bintrail. If no version is given as an argument, ask for it (e.g. `0.2.0` — no `v` prefix needed here).

Steps:

1. Read `CHANGELOG.md` and verify there is an `[Unreleased]` section with at least one entry. If it is empty, ask the user to add entries before continuing.

2. Determine today's date (YYYY-MM-DD format).

3. Update `CHANGELOG.md`:
   - Rename `## [Unreleased]` to `## [{{version}}] - {{today}}`
   - Add a new empty `## [Unreleased]` section above it

4. Run `go vet ./...` — abort and report any issues before touching git.

5. Run `go test ./...` — abort if any tests fail.

6. Stage and commit:
   ```
   git add CHANGELOG.md
   git commit -m "Bump version to {{version}}, update changelog"
   ```

7. Create an annotated tag:
   ```
   git tag -a v{{version}} -m "Release v{{version}}"
   ```

8. Show the user a summary of what was done (commit hash, tag, changed files) and ask for explicit confirmation before pushing.

9. On confirmation, push both:
   ```
   git push origin main
   git push origin v{{version}}
   ```

10. Tell the user that GitHub Actions will now run the release workflow (`.github/workflows/release.yaml`) and provide the Actions URL.

Notes:
- GoReleaser handles binary builds, archives, checksums, and the GitHub release.
- Do not bump any Go source version constants — the Makefile reads the version from git tags via `git describe`.
