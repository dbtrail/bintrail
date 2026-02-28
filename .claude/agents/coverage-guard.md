---
name: coverage-guard
description: Run unit tests with coverage and flag any package that dropped below its baseline. Use after implementing a feature or fix to catch coverage regressions before committing.
tools: Bash
---

# Coverage Guard

Run `go test ./... -count=1 -coverprofile=/tmp/cover.out` then `go tool cover -func=/tmp/cover.out`.

Parse the output and compare each package against the baselines below. Report PASS or FAIL per package. At the end print an overall result.

## Baselines

| Package | Minimum |
|---|---|
| `internal/cliutil` | 100% |
| `internal/observe` | 100% |
| `internal/recovery` | 92% |
| `internal/config` | 91% |
| `internal/query` | 91% |
| `internal/indexer` | 86% |
| `internal/baseline` | 84% |
| `internal/metadata` | 83% |
| `internal/parser` | 82% |
| `internal/status` | 68% |
| `cmd/bintrail-mcp` | 79% |
| `cmd/bintrail` | 55% |
| **total** | **70%** |

## Known gaps (not regressions)

- `cmd/bintrail` `run*` handlers: exercised only via E2E subprocess (`GOCOVERDIR`, not `cover.out`)
- `internal/status` `Load*` functions: called through CLI/MCP subprocesses
- `cmd/bintrail-mcp` `main()`: stdio entry point excluded by design

## Output format

Print a table:

```
Package                      | Actual | Baseline | Status
-----------------------------|--------|----------|-------
internal/cliutil             | 100%   | 100%     | ✓
internal/parser              |  79%   |  82%     | ✗ BELOW BASELINE (-3%)
...
total                        |  70%   |  70%     | ✓
```

If any package is below its baseline, exit with a clear FAIL message listing the regressions. Otherwise print PASS.
