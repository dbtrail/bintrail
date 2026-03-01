---
name: issue
description: Implement a GitHub issue end-to-end for the bintrail project. Creates an isolated worktree+branch, implements the fix, runs tests, commits, pushes, and opens a PR that auto-closes the issue on merge.
---

Implement GitHub issue #$ARGUMENTS for the bintrail project.

Follow these steps in order:

1. **Create worktree**: Use the `EnterWorktree` tool with `name: "issue-$ARGUMENTS"`. This creates an isolated branch `issue-$ARGUMENTS` based on `main` and switches the session into it. All subsequent file changes happen in that worktree — `main` is untouched.

2. **Read the issue**: Use `mcp__github__get_issue` with `owner: "nethalo"`, `repo: "bintrail"`, `issue_number: $ARGUMENTS` — understand the full request before touching any code.

3. **Explore before writing**: Use Glob and Grep to find relevant files. Read them. Never modify code you haven't read. Check CLAUDE.md for architecture guidance.

4. **Present plan and wait for approval**: Before writing any code, summarize your implementation plan in a clear, concise list:
   - Which files you will change and why
   - What new functions/types you will add
   - Any non-obvious design decisions
   Then **stop and wait** for the user to approve or redirect. Do not proceed to step 5 until the user explicitly says to go ahead (e.g. "looks good", "go ahead", "yes"). If the user gives feedback, revise the plan and present it again.

5. **Implement**: Make the minimal changes needed. Follow existing patterns (flag naming conventions, validation function placement, test structure). Do not add features beyond what the issue asks.

6. **Test**: Run `go test ./... -count=1`. All tests must pass before committing. Fix any failures.

7. **Commit**: Stage only the files you changed. Commit message must include `closes #$ARGUMENTS`. Use the heredoc form:
   ```
   git commit -m "$(cat <<'EOF'
   <summary line>

   <details if needed>

   closes #$ARGUMENTS

   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
   EOF
   )"
   ```

8. **Push**: `git push -u origin issue-$ARGUMENTS`

9. **Create PR**: Open a pull request targeting `main`. The `closes #$ARGUMENTS` in the PR body auto-closes the issue when the PR is merged:
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

10. **Review**: Run `/code-review` to check the implementation for critical issues, bugs, and code quality problems. Fix any critical or important issues found before proceeding.

11. **Return PR URL**: Print the PR link so the user can review and merge. Once the PR is approved, run `/merge $ARGUMENTS` to squash-merge it, pull main, and clean up the worktree and branches.
