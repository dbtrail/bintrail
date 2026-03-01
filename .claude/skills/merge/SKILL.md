---
name: merge
description: Squash-merge a PR, pull main, and clean up the worktree and branches. Run this after the PR is ready to merge. Usage: /merge <PR-number>
---

Squash-merge PR #$ARGUMENTS and clean up the worktree and branches.

Follow these steps **in order**. Do not skip steps.

1. **Capture context before anything else** — all subsequent git commands depend on this:
   ```bash
   git worktree list --porcelain
   ```
   - The **first** `worktree` line is the main repo path. Save it as `<main-repo>`.
   - The **current** worktree path is the one whose `HEAD` matches the current branch. Save it as `<worktree-path>`.
   - The current branch name is on the `branch` line (strip the `refs/heads/` prefix). Save it as `<branch>`.
   - If the session is NOT inside a worktree (i.e. only one entry), set `<worktree-path>` to empty.

2. **Confirm PR is mergeable**: Use `gh pr view $ARGUMENTS --json state,mergeable,headRefName` to check:
   - `state` must be `"OPEN"` — abort with a clear message if already merged or closed.
   - `headRefName` must match `<branch>` — abort if mismatched (wrong PR for this worktree).

3. **Merge the PR**: Use `gh pr merge $ARGUMENTS --squash --auto` OR the `mcp__github__merge_pull_request` tool with `merge_method: "squash"`. Either works — prefer `gh` for simplicity. If merge fails (e.g. checks still running), report the error and stop — do not proceed with cleanup.

4. **Pull main into local main**:
   ```bash
   git -C <main-repo> pull origin main
   ```
   This fetches the squash commit. If this fails (e.g. network error), report but continue — the local main can be synced later.

5. **Delete the remote branch**:
   ```bash
   git -C <main-repo> push origin --delete <branch>
   ```
   If this fails with "remote ref does not exist", the branch was already deleted — that's fine, continue.

6. **Delete the local branch** (must happen before worktree removal — removing the worktree destroys the session's cwd, which bricks all subsequent Bash commands):
   ```bash
   git -C <main-repo> branch -d <branch>
   ```
   If `-d` refuses because git doesn't recognize the branch as merged (can happen with squash merges), use `-D` instead.

7. **Remove the worktree** (only if `<worktree-path>` is non-empty and exists). `cd` into `<main-repo>` first in the same command so the Bash tool's persistent CWD lands on a directory that still exists — without this, the shell bricks after the directory is deleted:
   ```bash
   cd <main-repo> && git worktree remove <worktree-path> --force
   ```
   Use `--force` because the branch is already merged.

8. **Report**: Print a summary:
   - PR #$ARGUMENTS merged (squash)
   - Local main updated
   - Remote branch `<branch>` deleted
   - Worktree `<worktree-path>` removed
   - Local branch `<branch>` deleted
