---
name: merge
description: Squash-merge a PR, pull main, and clean up the worktree and branches. Run this after the PR is ready to merge. Usage: /merge <PR-number>
---

Squash-merge PR #$ARGUMENTS and clean up the worktree and branches.

Follow these steps **in order**. Do not skip steps.

1. **Capture context and escape the worktree** — all subsequent steps depend on this:
   ```bash
   git worktree list --porcelain
   ```
   - The **first** `worktree` line is the main repo path. Save it as `<main-repo>`.
   - The **current** worktree path is the one whose `HEAD` matches the current branch. Save it as `<worktree-path>`.
   - The current branch name is on the `branch` line (strip the `refs/heads/` prefix). Save it as `<branch>`.
   - If the session is NOT inside a worktree (i.e. only one entry), set `<worktree-path>` to empty.

   **CRITICAL — immediately change the Bash tool's CWD to the main repo**:
   ```bash
   cd <main-repo>
   ```
   This MUST be a standalone Bash call (not combined with other commands) so that the Bash tool persists `<main-repo>` as the CWD for all future calls. If you skip this, later steps will fail after the worktree directory is removed — the Bash tool cannot start a shell in a non-existent directory, so even `cd` inside a command won't help.

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

6. **Remove the worktree and delete the local branch** — the CWD is already `<main-repo>` (set in step 1), so these commands run safely:

   If `<worktree-path>` is non-empty:
   ```bash
   git worktree prune 2>/dev/null; git worktree remove <worktree-path> --force 2>/dev/null; git branch -D <branch>
   ```
   Use `;` (not `&&`) so each command runs regardless of the previous one's exit code — `prune` and `remove` may warn about already-deleted paths, which is fine.

   If `<worktree-path>` is empty (merge was run from main repo, not a worktree):
   ```bash
   git branch -D <branch>
   ```

7. **Report**: Print a summary:
   - PR #$ARGUMENTS merged (squash)
   - Local main updated
   - Remote branch `<branch>` deleted
   - Worktree `<worktree-path>` removed
   - Local branch `<branch>` deleted

   If the `/exit` prompt appears saying "You have N commits on `<branch>`" — that is **expected with squash merges**. Git does not track that individual commits were squash-merged into main; it sees them as unmerged on the feature branch. The changes are already on main as one squash commit. It is always safe to choose **Remove worktree**.
