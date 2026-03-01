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

6. **Remove the worktree and delete the local branch in one command** — these two steps must be combined because:
   - Git refuses to delete a branch that is still checked out in a worktree.
   - Removing the worktree destroys the session's CWD, bricking any subsequent Bash commands.
   - Solving both: `cd` to `<main-repo>` first, remove the worktree, then delete the branch — all in a single shell command so the CWD lands on the main repo before the worktree directory disappears.

   If `<worktree-path>` is non-empty and still registered (check with `git -C <main-repo> worktree list`):
   ```bash
   cd <main-repo> && git worktree remove <worktree-path> --force && git branch -D <branch>
   ```

   If git says the worktree path "does not exist" (stale metadata), prune first:
   ```bash
   cd <main-repo> && git worktree prune && git branch -D <branch>
   ```

   If `<worktree-path>` is empty (merge was run from main repo, not a worktree):
   ```bash
   git -C <main-repo> branch -d <branch>
   ```
   Use `-D` if `-d` refuses (squash merges are not recognized as merged by git).

8. **Report**: Print a summary:
   - PR #$ARGUMENTS merged (squash)
   - Local main updated
   - Remote branch `<branch>` deleted
   - Worktree `<worktree-path>` removed
   - Local branch `<branch>` deleted
