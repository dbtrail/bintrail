---
name: ci-check
description: Run the full local CI suite (vet, fmt, build, unit tests) before pushing. Stops at the first failure and reports what to fix.
disable-model-invocation: true
---

Run these steps in order. Stop immediately if any step fails and report the output.

1. **vet**: `go vet ./...`
2. **fmt**: `gofmt -l .` — if any files are printed, fail with: "Unformatted files (run gofmt -w <file>): <list>"
3. **build**: `go build ./...`
4. **unit tests**: `go test ./... -count=1`

If all steps pass, print:

```
✓ vet
✓ fmt
✓ build
✓ tests
CI check passed — safe to push.
```

If a step fails, print which step failed, its output, and stop. Do not run subsequent steps.
