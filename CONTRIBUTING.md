# Contributing to Bintrail

Thank you for your interest in contributing. This document covers how to get set up, the conventions used in the codebase, and the pull request process.

## Prerequisites

- Go 1.22 or later (the codebase uses `range N`, `min()`, `slices`, and `strings.SplitSeq`)
- A MySQL 8.0+ instance for integration testing (unit tests run without a DB)
- `git`

## Getting started

```sh
git clone https://github.com/nethalo/bintrail
cd bintrail
go mod download
go build ./...
go test ./...
```

All unit tests pass without a running database. If you want to test against a real MySQL instance, see [Integration testing](#integration-testing) below.

## Project layout

```
cmd/bintrail/      One file per command. Shared CLI helpers live in query.go.
internal/          Core packages. Each package has a _test.go alongside it.
migrations/        Reference DDL — tables are created by `bintrail init`, not this file.
```

Read `CLAUDE.md` for a detailed map of the architecture, key patterns, and gotchas discovered during development.

## Making changes

### Adding a new command

1. Create `cmd/bintrail/<command>.go` in `package main`.
2. Declare a `var <cmd>Cmd = &cobra.Command{...}` and register it in `init()` with `rootCmd.AddCommand(...)`.
3. Name flag variables with a short prefix matching the command (e.g. `rotRetain` for `rotate`, `stIndexDSN` for `status`).
4. Reuse `parseEventType` and `parseQueryTime` from `query.go` if the command accepts time or event-type filters — they are in the same package.
5. Use `mysql.ParseDSN(dsn)` to extract the database name from a DSN; don't use `SELECT DATABASE()`.

### Coding conventions

- Use `any` instead of `interface{}`.
- Use `min()`, `max()` built-ins instead of manual comparisons.
- Use `range N` instead of `for i := 0; i < N; i++`.
- Use `slices.Reverse`, `slices.Sort`, etc. from the standard library.
- Use `strings.SplitSeq` (Go 1.24) where applicable.
- Keep command files self-contained. Avoid creating shared packages for logic that is only used in one place.

### Working with the database

- **Never insert `pk_hash` explicitly** — it is a generated stored column (`SHA2(pk_values, 256)`).
- **PK lookups** must use both `pk_hash = SHA2(?, 256)` (for the index scan) and `pk_values = ?` (as a hash collision guard).
- **Partitions**: the catch-all `p_future VALUES LESS THAN MAXVALUE` must always exist. When adding new partitions use `REORGANIZE PARTITION p_future INTO (... new partitions ..., PARTITION p_future VALUES LESS THAN MAXVALUE)`.
- **`schema_snapshots`**: `snapshot_id` is a group identifier (shared by all rows of one snapshot), not the auto-increment row PK (`id`). `NewResolver(db, 0)` loads the latest snapshot.

### JSON and type handling

After a JSON round-trip (`json.Unmarshal` into `map[string]any`), all numbers are `float64`. Use the integer-detection pattern from `formatValue` in `recovery.go` when formatting values for SQL:

```go
if val == math.Trunc(val) && math.Abs(val) < 1e15 {
    return strconv.FormatInt(int64(val), 10)
}
```

MySQL JSON column values returned by go-mysql are raw JSON bytes (`[]byte`). Promote them to `json.RawMessage` before marshalling to avoid base64 encoding — see `marshalRow` in `indexer/indexer.go`.

## Writing tests

- Unit tests should not require a live database. Use in-memory data structures and test the pure helper functions.
- Recovery tests use `newGen()` (`New(nil, nil)`) — nil DB and nil resolver triggers the all-columns WHERE fallback, which is safe for unit tests.
- Do not hardcode UNIX timestamps for specific future dates. Compute them at test time:
  ```go
  ts := time.Date(2026, 2, 20, 0, 0, 0, 0, time.UTC).Unix()
  ```
- Use the `assertSQL(t, stmt, want)` / `assertContains(t, s, want)` helpers where they exist in the same test file.
- Test files in `cmd/bintrail/` are `package main` and have direct access to all unexported helpers — use this for testing command-level helpers without exporting them.

Run the full suite before opening a PR:

```sh
go test ./...
go vet ./...
```

## Integration testing

Integration tests require a MySQL instance with:

```sql
SET GLOBAL binlog_format       = ROW;
SET GLOBAL binlog_row_image    = FULL;
```

The recommended approach is a local Docker container:

```sh
docker run -d --name mysql-bintrail \
  -e MYSQL_ROOT_PASSWORD=root \
  -p 3306:3306 \
  mysql:8.0 \
  --binlog-format=ROW \
  --binlog-row-image=FULL \
  --log-bin=mysql-bin \
  --server-id=1

# Initialise the index
bintrail init --index-dsn "root:root@tcp(127.0.0.1:3306)/binlog_index"
```

## Pull request checklist

- [ ] `go test ./...` passes
- [ ] `go vet ./...` is clean
- [ ] New behaviour is covered by tests
- [ ] `CLAUDE.md` is updated if you introduced a new pattern, gotcha, or architectural decision
- [ ] Commit messages are clear and describe *why*, not just *what*

## Commit style

Short imperative subject line, 72 chars max. Body is optional but encouraged for non-trivial changes:

```
Add changed-column filter to query engine

JSON_CONTAINS is used instead of a string search so the filter matches
exact column names and not substrings (e.g. "status" must not match
"order_status").
```

## License

By contributing you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
