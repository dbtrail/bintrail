---
name: issue
description: Implement a GitHub issue end-to-end for the bintrail project. Creates an isolated worktree+branch, implements the fix, runs tests, commits, pushes, and opens a PR that auto-closes the issue on merge.
---

Implement GitHub issue #$ARGUMENTS for the bintrail project.

Follow these steps in order:

1. **Create worktree**: Use the `EnterWorktree` tool with `name: "issue-$ARGUMENTS"`. This creates an isolated branch `issue-$ARGUMENTS` based on `main` and switches the session into it. All subsequent file changes happen in that worktree — `main` is untouched.

2. **Read the issue**: `gh issue view $ARGUMENTS` — understand the full request before touching any code.

3. **Explore before writing**: Use Glob and Grep to find relevant files. Read them. Never modify code you haven't read. Check CLAUDE.md for architecture guidance.

4. **Implement**: Make the minimal changes needed. Follow existing patterns (flag naming conventions, validation function placement, test structure). Do not add features beyond what the issue asks.

5. **Test**: Run `go test ./... -count=1`. All tests must pass before committing. Fix any failures.

6. **Commit**: Stage only the files you changed. Commit message must include `closes #$ARGUMENTS`. Use the heredoc form:
   ```
   git commit -m "$(cat <<'EOF'
   <summary line>

   <details if needed>

   closes #$ARGUMENTS

   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
   EOF
   )"
   ```

7. **Push**: `git push -u origin issue-$ARGUMENTS`

8. **Create PR**: Open a pull request targeting `main`. The `closes #$ARGUMENTS` in the PR body auto-closes the issue when the PR is merged:
   ```
   gh pr create --title "<summary>" --body "$(cat <<'EOF'
   closes #$ARGUMENTS

   ## Summary
   - <bullet points>

   ## Test plan
   - [ ] Unit tests pass (`go test ./... -count=1`)

   🤖 Generated with [Claude Code](https://claude.com/claude-code)
   EOF
   )"
   ```

9. **Return PR URL**: Print the PR link so the user can review and merge. Once the PR is approved, run `/merge $ARGUMENTS` to squash-merge it, pull main, and clean up the worktree and branches.
