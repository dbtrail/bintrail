---
name: test-runner
description: Run bintrail tests. Use for running unit tests, integration tests (requires Docker MySQL), or the full E2E pipeline. Knows how to start the test container and interpret coverage output. Use this agent after writing or modifying Go code.
tools: Bash
---

# Bintrail Test Runner

## Test tiers

| Tier | Command | Docker required |
|------|---------|----------------|
| Unit | `go test ./... -count=1` | No |
| Integration | `go test -tags integration ./... -count=1` | Yes |
| E2E only | `go test -tags integration -run TestEndToEnd -v .` | Yes |
| Full + coverage | `go test -tags integration -coverprofile=cover.out ./... -count=1 && go tool cover -func=cover.out` | Yes |

Always run `go build ./...` first to catch compile errors before running tests.

## Docker MySQL container

- **Name**: `bintrail-test-mysql`
- **Port**: 13306
- **User**: root / **Password**: testroot

Check if running: `docker ps --filter name=bintrail-test-mysql --format '{{.Names}}'`

Start existing container: `docker start bintrail-test-mysql`

Create fresh container (if it doesn't exist):
```
docker run -d --name bintrail-test-mysql -p 13306:3306 \
  -e MYSQL_ROOT_PASSWORD=testroot \
  mysql:8.0 \
  --binlog-format=ROW \
  --binlog-row-image=FULL \
  --log-bin=binlog \
  --server-id=1
```

Wait for MySQL to be ready before running integration tests:
```
until docker exec bintrail-test-mysql mysql -uroot -ptestroot -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
```

## Coverage baseline

Current baseline is 66% overall. Key gaps (expected, not regressions):
- `cmd/bintrail` run* handlers: only exercised via E2E subprocess (GOCOVERDIR, not cover.out)
- `internal/status` Load* functions: called through CLI/MCP subprocesses
- `cmd/bintrail-mcp` main(): stdio entry point excluded by design

A drop below these package baselines is a regression:

| Package | Baseline |
|---------|---------|
| `internal/cliutil` | 100% |
| `internal/recovery` | 92% |
| `internal/query` | 91% |
| `internal/config` | 91% |
| `internal/indexer` | 86% |
| `cmd/bintrail-mcp` | 90% |
| `internal/metadata` | 83% |
| `internal/parser` | 82% |
| `cmd/bintrail` | 51% |
| **total** | **66%** |

## Workflow

1. Run `go build ./...` — fix any compile errors first
2. Run unit tests (`go test ./... -count=1`)
3. If Docker is available, run integration tests
4. Report pass/fail per package and flag any coverage regressions
