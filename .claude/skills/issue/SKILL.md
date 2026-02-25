---
name: issue
description: Implement a GitHub issue end-to-end for the bintrail project. Reads the issue, explores relevant code, implements the fix or feature, runs unit tests, commits with "closes #N", pushes, and closes the issue.
---

Implement GitHub issue #$ARGUMENTS for the bintrail project.

Follow these steps in order:

1. **Read the issue**: `gh issue view $ARGUMENTS` — understand the full request before touching any code.

2. **Explore before writing**: Use Glob and Grep to find relevant files. Read them. Never modify code you haven't read. Check CLAUDE.md for architecture guidance.

3. **Implement**: Make the minimal changes needed. Follow existing patterns (flag naming conventions, validation function placement, test structure). Do not add features beyond what the issue asks.

4. **Test**: Run `go test ./... -count=1`. All tests must pass before committing. Fix any failures.

5. **Commit**: Stage only the files you changed. Commit message must include `closes #$ARGUMENTS`. Use the heredoc form:
   ```
   git commit -m "$(cat <<'EOF'
   <summary line>

   <details if needed>

   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
   EOF
   )"
   ```

6. **Push**: `git push`

7. **Close the issue**: `gh issue close $ARGUMENTS --comment "<one sentence summary of what was implemented>"`
