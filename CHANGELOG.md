# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **`bintrail up --console` can now serve the Time-travel (point-in-time reconstruct) surface** (#379). `up` gained `--baseline-dir` / `--baseline-s3` (env: `BINTRAIL_CONSOLE_BASELINE_DIR` / `BINTRAIL_CONSOLE_BASELINE_S3`, flag > env > default), threaded into the embedded console's configuration exactly as the standalone `bintrail console` does — so a single `bintrail up --console --baseline-dir <dir>` process streams live *and* answers baseline-gated reconstruct queries from the browser, instead of requiring a second standalone-console process on another port. The gating is reused verbatim (`baselineConfigured` in `internal/console/server.go`, enforced at the endpoint); `up` defines no `--profile`/`--no-archive`, so the gate reduces to baseline presence. The read-only, token-auth, and result-cap invariants are unchanged.

### Fixed
- **Strict-mode (`AllowGaps=false`) cross-source fetches now abort when *any* archive source fails to load, not only when every source fails** (#377). `query.FetchMerged` previously hard-failed only when *all* archive sources errored; with two or more sources (one per `bintrail_id` in `archive_state`), a single broken source was demoted to an `slog.Warn` and its deltas silently dropped — undetectable downstream, because the planner validates `archive_state` coverage *before* the fetch, so the affected hours still read as "covered". Affected strict-mode callers — `bintrail reconstruct` (single-row and full-table), the `bintrail shim` virtual schemas (default `--allow-gaps=false`), and the console's `GET /api/reconstruct` — could fold an incomplete delta set and report success. Any archive-source failure under strict mode is now a hard error naming the failed source. Permissive callers (`bintrail recover`, console events/recover, and `bintrail query`'s own warn-and-continue archive loop) keep their best-effort behavior unchanged.

### Changed
- **License changed from Business Source License 1.1 to the Apache License, Version 2.0.** `LICENSE` now carries the verbatim Apache-2.0 text and a new `NOTICE` file records the copyright attribution (`(c) 2025 Daniel Guzman Burgos`), as is conventional under Apache-2.0. The README and CONTRIBUTING license sections and the `.goreleaser.yaml` package/image `license` metadata (`nfpms`, OCI `org.opencontainers.image.licenses` labels) are updated to `Apache-2.0`. This drops the BUSL Additional Use Grant restrictions and the four-year Change Date — bintrail is now permissively licensed for any use, including commercial. The CLA already reserved the maintainer's right to relicense, so existing contributions are covered.

## [0.7.13] - 2026-06-01

### Added
- **Full-table `_snapshot` time-travel queries now reconstruct the complete table at AS OF** (#362). A no-WHERE `SELECT * FROM _snapshot.<table> AS OF '<ts>'` through `bintrail shim` previously returned only rows with binlog activity in the retained window, silently omitting rows that existed at that instant but were never touched. It now merges the `bintrail baseline` snapshot at-or-before AS OF with the post-snapshot binlog deltas across the whole table — never-touched baseline rows pass through, rows updated/inserted after the baseline take their latest image, and rows deleted after it drop out — reusing the same merge engine as the offline `bintrail reconstruct`. When a baseline merge isn't possible (no baseline source configured, an unsupported or unresolvable primary key, or no baseline at-or-before AS OF) it degrades to the binlog-only `_flashback` behaviour instead of failing, emitting a `Warn` so the degradation is visible. Buffered output stays bounded by the existing full-table row cap (`ER_TOO_BIG_SELECT`).

### Fixed
- **Temporal primary-key baseline lookups no longer silently miss on non-UTC hosts** (#359). Single-row `_snapshot` queries and `bintrail reconstruct --pk` bind the PK value as a string against the typed baseline Parquet column; for `DATETIME`/`TIMESTAMP` PKs (stored UTC-anchored, read back by DuckDB as `TIMESTAMP WITH TIME ZONE`) the string→timestamp cast used the host OS timezone, so on a non-UTC host the row silently failed to match and the lookup fell back to binlog-only. The DuckDB session is now pinned to UTC, making the match deterministic on any host, and `DATETIME`/`TIMESTAMP`/`DATE` PKs are now admitted by the single-row baseline lookup. Also corrects `docs/time-travel-sql.md`, which overclaimed that the no-WHERE form returns "every row that existed at that instant" for both virtual schemas (#356).

## [0.7.12] - 2026-05-29

### Added
- Two new commands collapse the onboarding wall from five steps to two: **`bintrail doctor`** and **`bintrail up`**. `doctor` runs preflight checks against the source MySQL (connection, `log_bin`, `binlog_format=ROW`, `binlog_row_image=FULL`, binlog retention, `REPLICATION SLAVE`/`CLIENT` grants, FK-cascade warning, schema visibility) plus optional index-DB write access when `--index-dsn` is given, and reports each as PASS/FAIL/WARN/SKIP with **copy-pasteable remediation** (the GRANT, `SET GLOBAL`, or my.cnf snippet that fixes it) instead of a one-line error. Exit code is 0 only when every required check passes; warnings never fail the run, so it is safe in CI (`--format json` for machine consumption). `up` chains preflight + `init` (idempotent table creation) + `stream` (auto-snapshots the schema if none exists, resumes from the last checkpoint) behind a single invocation; `--skip-doctor` bypasses the preflight. When `--server-id` is omitted it is derived deterministically from a SHA-256 of the source `host:user:dbname`, so the same DSN yields the same ID every run for clean restart/resume. The underlying `init`/`snapshot`/`index`/`stream` commands remain available unchanged.
- Release pipeline now publishes a **multi-arch Docker image** (`linux/amd64` + `linux/arm64`) to `ghcr.io/dbtrail/bintrail` on every `v*` tag, removing the Go-toolchain dependency for the most common install path. The `.goreleaser.yaml` gained `dockers`/`docker_manifests` (one image per arch stitched into a `:{version}` and `:latest` manifest), `nfpms` (deb + rpm assets attached to the GitHub release, no external repo required), `sboms` (syft SBOM per archive), and `signs`/`docker_signs` (cosign keyless signing of checksums and image manifests via GitHub OIDC). The published image uses a dedicated `Dockerfile.goreleaser` that copies the binaries GoReleaser already cross-compiled, rather than rebuilding from source like the manual-build root `Dockerfile` — the two coexist so `docker build .` keeps working. The release workflow adds `packages: write` + `id-token: write` permissions, QEMU/Buildx setup (the arm64 image's `RUN` layer executes under emulation on the amd64 runner), GHCR login, and the cosign/syft installers, and pins `goreleaser-action` to `~> v2` instead of `latest`. Homebrew tap publishing is staged but **deferred**: it needs a `dbtrail/homebrew-tap` repo plus a `HOMEBREW_TAP_GITHUB_TOKEN` PAT secret (the default `GITHUB_TOKEN` cannot push to a second repo) — the ready-to-enable `brews` block is committed commented-out in `.goreleaser.yaml`. Prereleases are gated so an RC tag is safe to test with: `release.prerelease: auto` marks `-rc*` tags as GitHub prereleases, and `skip_push: auto` on the `:latest` manifest keeps a prerelease from claiming the tag anonymous users pull (version-specific tags still push, so the RC fully exercises the pipeline). The full pipeline was validated end-to-end by the `v0.7.12-rc1` prerelease (multi-arch build, cosign signing, and GHCR push all succeeded; `:latest` correctly stayed put). Anonymous `docker pull` additionally requires a one-time flip of the GHCR package to public (see `docs/docker.md`).

## [0.7.11] - 2026-05-24

### Added
- `bintrail query` accepts `--order ASC|DESC` (default `ASC`) to control the sort direction applied **before** `--limit`, so `--order DESC --limit N` returns the **N newest** events instead of the N oldest re-sorted in display order. Pre-fix the SQL emitted `ORDER BY event_timestamp, event_id` (no direction = ASC) for every caller; the dbtrail SaaS agent applied an in-memory sort after the fact, which fooled the table renderer but returned the wrong page identity — `--order DESC --limit 100` against a table with 1M events would silently return events 1..100 sorted descending, never the latest 100. The fix is threaded through every code path that calls `ORDER BY` in the query pipeline so DuckDB archive reads, MySQL live queries, and the cross-source merge all honor the requested direction: `internal/query/query.go:buildQuery` (live MySQL — both the simple path and the LimitPerPK ROW_NUMBER variant), three `internal/parquetquery/parquetquery.go` builders (glob, file-list, single-file backwards-compat), and `internal/query/merge.go:MergeResults`/`MergeAndTrim` (cross-source dedup + final sort). Sort keys both follow the requested direction (`event_timestamp <dir>, event_id <dir>`) so the ordering is total and deterministic across timestamp collisions — the original outer ORDER BY emitted `event_timestamp ASC, event_id ASC` implicitly; reversing only one key would have silently picked wrong tie-break winners and broken the `LimitPerPK` reverse-walk precondition. The inner ROW_NUMBER `ORDER BY event_timestamp DESC, event_id DESC` clauses (live MySQL ROW_NUMBER and DuckDB QUALIFY) stay fixed at DESC regardless of caller direction because their semantic is "latest N events per PK", not "first N in requested direction". `MergeAndTrim` sorts ASC internally for the per-PK trim (preserving `LimitPerPK`'s precondition) then re-sorts in the caller's direction before applying the global cap so DESC + Limit slices from the correct end of the list. Validated by a new integration test asserting that ASC + Limit=N and DESC + Limit=N against a > 2N-row dataset return STRICTLY DISJOINT event_id sets — the disjoint-row-set assertion is what would have caught the bug originally. Unit tests pin every SQL-builder branch (live MySQL outer + inner, all three Parquet builders, MergeResults/MergeAndTrim direction handling); `OrderDirection` is exported so the SaaS layer normalises with the same rule. Companion to nethalo/dbtrail#1511 — the SaaS agent now forwards `--order=` to the CLI instead of post-sorting in memory (#1511).

### Fixed
- `bintrail agent` BYOS-mode `recover` commands over the WebSocket bridge now honour the `gtid` field in the SaaS-forwarded payload. `RecoverRequest` had no `GTID` field, so JSON unmarshal silently discarded `{"gtid": "<target>"}` and the handler fell back to time-only filtering — producing reversal SQL for unrelated events at the same timestamp. On a tool that emits production SQL, returning `success: true` with the wrong scope is a critical correctness/safety regression. `RecoverRequest.GTID` now carries the `json:"gtid,omitempty"` tag the SaaS sends; `DefaultHandler.HandleRecover` propagates it into `query.Options{GTID:...}`, which the three data sources (in-memory buffer, MySQL index DB, parquet archives) already honour — this change just routes the value through. Zero-value `TimeStart`/`TimeEnd` are now skipped instead of passed verbatim, matching the SaaS-side change that drops the 24h-default-window clamp when only `gtid` is supplied (a gtid can reference an event arbitrarily far in the past — clamping would silently drop it). A fail-loud guard at the top of `HandleRecover` rejects requests where both GTID and time window are empty: without it the previous code would build `query.Options{Limit: 1000}` with no predicates and happily return reversal SQL for the last 1000 events in the index — the exact silent-fallback shape that surfaced the bug originally. Regression pinned by `TestHandleRecover_filterByGTID` (three events at the same timestamp, only one carrying the requested GTID — reversal SQL references only that event's PK), `TestHandleRecover_gtidNoMatch` (non-existent GTID → no recovery statements, no fallback to full-table scope), `TestHandleRecover_gtidNoMatch_largeFallback` (1500 events in buffer above the handler's 1000-row cap, broad time window, non-matching GTID — verifies the filter wasn't bypassed at scale), `TestRecoverRequestJSON_gtid` (pins `"gtid"` lowercase wire tag so a future typo fails loud), and `TestRecoverRequestJSON_backwardCompat` (older SaaS callers without the key still decode). Go's `encoding/json` ignores unknown JSON keys, so this CLI change ships safely before the SaaS PR — newer agents reading older SaaS payloads see `GTID == ""` and behave as before; older agents reading newer SaaS payloads silently drop `gtid`, which is the bug being fixed and why `/deploy-agent` must roll this CLI to BYOS hosts before the SaaS-side PR can take effect. Refs `nethalo/dbtrail#1512` (#345).

## [0.7.10] - 2026-05-24

### Added
- `bintrail stream` now auto-discovers the source's current binlog position on first run when neither `--start-file`/`--start-pos` nor `--start-gtid` is provided AND no `stream_state` checkpoint exists. Mirrors the behaviour `bintrail agent` BYOS mode has had since `config.CurrentBinlogPosition` landed — removes the wrapper-script burden every fresh stream install was reinventing. Implementation is a new `resolveStartWithAutoDiscover` wrapper around the existing `resolveStart` that invokes the discovery callback only on the (saved=nil ∧ no-flags ∧ callback-present) tuple, so all previously-tested paths (`--start-file`/`--start-gtid` explicit, saved checkpoint resume, mutually-exclusive flag rejection, invalid GTID) remain unchanged. Test pinning covers both the `--start-file` and `--start-gtid` bypass branches so a future refactor cannot asymmetrically drop the GTID guard and silently overwrite an operator's declared base GTID (#312).
- `bintrail shim` accepts three time-travel query shapes that operators reach for by muscle memory from Oracle / SQL Server training, removing the demo footguns surfaced during interactive Percona Live sessions: (a) **column projection lists** — `SELECT id, name FROM _flashback.t AS OF '<ts>' WHERE id=1` now returns just the listed columns in the listed order; missing columns surface as NULL (matching MySQL's behaviour after `ALTER TABLE DROP COLUMN`); bare identifiers only — backticks, `schema.column`, and aliases fall through to the existing malformed-query error. (b) **`AS OF TIMESTAMP '<ts>'`** — the Oracle/SQL Server keyword form is now accepted alongside the plain `AS OF '<ts>'` shape. (c) **`SHOW [FULL] TABLES FROM _flashback|_diff|_snapshot`** — lists tables from the latest schema snapshot, sorted, with the `Tables_in_<virtual>` column header matching real MySQL's `Tables_in_<dbname>` convention. Pre-snapshot installs return an empty resultset (same UX as `SHOW TABLES` against a fresh DB) rather than an error. ProxySQL routing requires a new `rule_id 990005` emitted by `bintrail proxysql-config`; without it the query routes to real MySQL and gets `ER_BAD_DB`. Critical regression caught in post-merge review: the PK-filtered `runPointInTime` path silently bypassed user-supplied column lists because `imageToResult → orderColumns` is designed for `SELECT *` and drops missing-from-image keys + appends image-only keys alphabetically; new `imageToResultVerbatim` helper used when `q.Columns != nil` honours the user's projection exactly. Out of scope: `USE _flashback` (COM_INIT_DB routes by connection-default-hostgroup, not query rules), column lists in the hint-comment form, and `SHOW DATABASES` listing the three virtual schemas (#313, #314, #315).
- `bintrail proxysql-config --validate` is an opt-in pre-flight that connects to `BINTRAIL_SOURCE_DSN`, probes `mysql.user` for each tenant declared in `shim.yaml`, and warns when the backend's authentication plugin differs from `--backend-auth-plugin`. Warn-only by design: validation never blocks SQL generation, and probe failures (permission denied, connect refused, etc.) warn-and-continue so the operator can still produce setup SQL offline. Three semantic bugs caught in post-merge review and fixed before release: (a) the initial implementation probed the DSN's connection user (typically the bintrail admin/indexer account) instead of the tenant user ProxySQL re-handshakes with — checking the wrong identity entirely and leaving the risk `--validate` was meant to mitigate unmitigated in the standard topology; (b) filtered by the server's hostname against `mysql.user.host`, which is the CLIENT host pattern (where the user may connect FROM), not the server's hostname — a category error that made the `host = ?` branch effectively dead code; (c) `LIMIT 1` + no `ORDER BY` collapsed legitimate split-plugin grants (same user with `@'localhost'=native` and `@'%'=caching_sha2`) to one non-deterministic row, silently hiding the conflict. Final query iterates tenants and runs `SELECT host, plugin FROM mysql.user WHERE user = ?` (no host filter), collecting all matching rows into a map so the orchestrator warns on mismatch, split-plugin grants, and user-not-found (a separate diagnostic from permission-denied because the SQL we're about to write references a user that won't exist on the backend). Help text for `caching_sha2_password` also tightened to document the TLS / primed-cache precondition #310 left implicit (#327).
- `bintrail agent` now sends `X-Bintrail-Server-UUID` on every `POST /v1/events` metadata flush, mirroring the WS-dial reconcile header shipped in #317. Companion to `dbtrail/dbtrail#1504`: the SaaS ingest endpoint resolves the server by UUID before falling back to `bintrail_id`, so a dashboard-pre-registered row (`bintrail_id` NULL) is found on first ingest instead of triggering a duplicate `byos-<server-id>`. Without this agent change the SaaS fix is dormant. Empty `--server-uuid` preserves legacy behaviour — the header is omitted entirely (not sent with empty value) so an older backend isn't confused. Regression-guarded by `TestMetadataClientOmitsServerUUIDHeaderWhenEmpty` (#341).

### Fixed
- `bintrail shim` emits a periodic `INFO`-level summary of denied monitor-user probes — restores the observability #322 demoted to `DEBUG` without bringing the per-probe log flood back. Production runs at `--log-level info`, so credential probes spoofing the monitor username and ProxySQL misconfigurations (rotated `mysql-monitor_password`) produced zero shim-side signal between #322 and this fix. A mutex-guarded aggregator (count + bounded distinct-remotes set, cap 16, with `first_seen`/`last_seen` brackets) feeds a 5-minute ticker that emits one line per non-zero interval and resets. The `distinct_remotes` field is load-bearing: it tells "one IP hammering 3000 times" from "200 IPs probing once" — the difference between a misconfigured ProxySQL and a credential-stuffing burst. Two regressions caught in post-merge review and corrected: (a) the ticker goroutine was started fire-and-forget; Go does not wait for unjoined goroutines before process exit, so counts accumulated since the last tick were silently lost on every clean shutdown — fixed with `sync.WaitGroup` tracked from `runShim` and `wg.Wait()` after `serveLoop` returns; (b) the original `atomic.Int64` aggregator had no access to the connection so it couldn't capture the remote — fixed by changing `classifyHandshakeErr` to return `(level, msg, isMonitor)` and having `handleConn` call `monitorDeniedBump(remote)` (#326).
- `bintrail agent --server-uuid` is now canonicalized before forwarding to the `X-Bintrail-Server-UUID` header and WS-dial reconcile fields. `uuid.Parse` accepts uppercase, mixed-case, braced `{...}`, and `urn:uuid:...` forms, so two operators registering the same logical server from different copy-paste sources would send divergent header values to the SaaS — surfacing as a duplicate `byos-<server-id>` record alongside the pre-registered one with no operator-visible cause (the SaaS reconciles by exact-string match today). `validateServerUUID` now returns `(canonical string, error)` and `runAgent` assigns the canonical form (lowercase, hyphenated, no braces, no `urn:` prefix) back to `agtServerUUID`. Empty input remains empty for back-compat. The SaaS should still normalize on receipt as defense-in-depth, but normalizing in the agent — closer to the operator's input — is the right primary fix (#329).
- `config.CurrentBinlogPosition` now returns a domain-specific error naming `log_bin` and the remediation query (`SHOW VARIABLES LIKE 'log_bin'`) when binary logging is disabled on the source. Both `SHOW BINARY LOG STATUS` (8.4+) and `SHOW MASTER STATUS` (<8.4) return an empty resultset rather than a syntax error when `log_bin=OFF`, so `QueryRow.Scan` returns `sql.ErrNoRows` on both branches — the previous wrapped error surfaced as `"no rows in result set (fallback: no rows in result set)"`, zero hint at the cause and identical operator-hostile symptom to the crash-loop #320 was meant to retire. Affects every caller of the shared helper: `bintrail agent` BYOS-mode startup AND `bintrail stream` after its first-run auto-discovery (this release, #312). Initial implementation gated the domain-error branch behind `&&` (both queries returning `ErrNoRows`); production never hits the symmetric shape — on any real MySQL/Percona with `log_bin=OFF` it's asymmetric (5.7/8.0/Percona <8.4: `SHOW BINARY LOG STATUS` → 1064, `SHOW MASTER STATUS` → `ErrNoRows`; 8.4+: the opposite), so the gate was changed to `||`. The sqlmock test had passed because mocks can return any combination; a table-driven regression test now covers both real-world shapes plus the degenerate both-empty sanity case (#325).
- `bintrail agent`'s WebSocket reconnect loop now fails fast on synchronous 4xx HTTP-upgrade errors (bad/missing/revoked API key, malformed headers, future `unknown_server_uuid` rejections, etc.) instead of spinning until `--max-reconnect-attempts` exhausts. The dial path previously discarded the `*http.Response`, so `isTemporary` only saw the raw handshake error and classified it as transient. `dial()` now captures the response, wraps a 4xx in a new `*PermanentDialError` carrying `StatusCode` + underlying error, and `isTemporary` returns false for it. 5xx and pre-HTTP transport errors stay transient. `resp.Body` is closed defensively even though `coder/websocket` replaces it with an `io.NopCloser` on the error path — cheap insurance against a future lib change re-introducing a real `ReadCloser`. Partially addresses #328: item 1 (mapping a specific `unknown_server_uuid` close-reason string in `ClassifyClose`) awaits SaaS-side spec — leaving the issue open for the follow-up (#328).

### Changed
- `bintrail agent --server-uuid` help text and the cross-reference from `--server-id` refreshed to match the SaaS-side hardening in `nethalo/dbtrail#1490`. The previous `WARNING` text claimed UUIDs are "silently ignored" — no longer accurate: unmatched UUIDs are logged server-side and the SaaS refuses to bind. Same cluster: operators with a pre-registered UUID from the dashboard intuitively try `--server-id` and hit `strconv.ParseUint` rejection (a 2026-05-24 demo had three EC2 agents go into restart-loop after a sed-swap of `--server-id 202` → UUID), so `--server-id`'s help now cross-references `--server-uuid` for discoverability. Minimal-scope cross-ref — does NOT unify the flags. The `slog.Info` startup hint in `runAgent` also refreshed to drop the stale "no duplicate `byos-<server-id>` record was created" framing — that auto-create path no longer runs under the hardened SaaS (#336, #337).
- User-facing docs synced with the last 72h of merged behaviour changes. (a) `SHOW MASTER STATUS` references in `guide.md` and `streaming-101.md` replaced with `SHOW BINARY LOG STATUS` (MySQL 8.4 removed `SHOW MASTER STATUS`; #308 shipped `config.CurrentBinlogPosition` with the new form first). (b) The "first run requires `--start-file`/`--start-gtid`" prose in `guide.md`'s setup walkthrough and `streaming-101.md` Step 7 updated to reflect the auto-discovery shipped in #312 — the first run is now flag-free; `streaming.md`'s RDS-binlog-readiness section references the new error string the auto-discover path emits. (c) `time-travel-sql.md`'s "four statement shapes" section now mentions the three demo-footgun features that shipped in this release: column lists in `SELECT` (#313), `AS OF TIMESTAMP` keyword (#314), and `SHOW TABLES FROM` virtual schemas (#315). Also notes the `WHERE`-must-be-PK invariant from #296 since interactive users will hit it. No new docs or sections — only updates where existing prose was factually contradicted by what shipped (#344).

## [0.7.9] - 2026-05-23

### Fixed
- Follow-up to the 0.7.8 NULL-`binlog_file` fix. Deploy verification of 0.7.8 on a real tenant surfaced the same bug class one column over: the Explorer crashed with `Scan error on column index 2, name "start_pos": converting NULL to uint64 is unsupported`. Production indexes carry NULL across MANY columns declared NOT NULL simultaneously — not just `binlog_file` — so 0.7.8's targeted fix was incomplete. This release extends the defensive Scan to every NOT NULL column except `event_id` (AUTO_INCREMENT, cannot return NULL on read) in all three Scan sites: `internal/query/query.go:scanRows` (MySQL Fetch), `internal/archive/archive.go:ArchivePartition` (rotate Parquet writer, with `!Valid` propagated into the `nulls[]` slice so true NULL is preserved), and `internal/parquetquery/parquetquery.go:scanRows` (DuckDB Parquet reader). Columns newly defended: `start_pos`, `end_pos`, `event_timestamp`, `schema_name`, `table_name`, `event_type`, `pk_values`, `schema_version`. `ResultRow` public types stay plain (NULL → zero value, accepted asymmetry with the already-pointer `GTID`/`ConnectionID`). Mechanically extending NULL → zero-value mapping across more columns would convert three downstream sites from fail-loud (Scan error) to fail-silent (wrong answer), so this release also guards them: `internal/reconstruct/reconstruct.go:applyEvent` adds an explicit `case 0` that preserves state AND emits a `slog.Warn` naming the affected row (was previously a silent `default`-branch no-op that would have produced wrong PITR state); `internal/query/merge.go:LimitPerPK` gives empty-PK drift rows a per-row bucket (`\x00drift:<event_id>` — `\x00` cannot appear in real PK strings) so they no longer collapse onto bucket `""` and silently drop beyond the cap or collide with legitimate empty-PK rows; `internal/parquetquery/parquetquery.go:canTerminateEarly` filters out zero-timestamp rows before computing cutoff so a drift-row-heavy early Parquet file no longer silently terminates the multi-file scan and drops all later real data. Validated end-to-end: the new `TestFetch_allNullableNotNullColumns` integration test ALTERs every defended column to allow NULL, inserts a row with NULL in all of them, asserts Fetch returns it without crashing; verified to fail with the EXACT production error message from the 0.7.8 deploy when the `start_pos` defense is reverted. Each downstream guard has its own targeted regression test (`TestApplyEvent_driftRowPreservesState`, `TestLimitPerPK_driftRowsBucketIndependently`, `TestCanTerminateEarly` new subtests). Follow-up worth tracking separately: investigate the producer side — defensive scanning unblocks consumers but doesn't address how partial rows reach a NOT NULL schema in the first place (#318).

## [0.7.8] - 2026-05-23

### Fixed
- `bintrail query`, `bintrail rotate` (via the agent), and historical-partition reads via DuckDB no longer crash with `sql: Scan error on column index 1, name "binlog_file": converting NULL to string is unsupported` when the index contains a row whose `binlog_file` is NULL. Surfaced as a P1 in the dbtrail SaaS Explorer (`/app/explorer` → "Run Query" returned the raw scan error for any tenant whose index had a single NULL-binlog_file row — e.g., customer indexes created before the `binlog_file NOT NULL` migration, or rows inserted by external pipelines). The same root cause lived at three independent Scan sites: `internal/query/query.go:scanRows` (MySQL Fetch — the visible crash), `internal/archive/archive.go:ArchivePartition` (rotate-path Parquet writer — strictly worse blast radius since a failed rotate blocks partition pruning and grows the index unbounded), and `internal/parquetquery/parquetquery.go:scanRows` (DuckDB-backed reads of archived partitions — was latent until the archive fix turned it into a live crash by correctly preserving NULL through to Parquet). All three now scan `binlog_file` into a `sql.NullString` and assign `.String` to `ResultRow.BinlogFile`, matching the `gtid`/`connection_id` pattern already used in the same Scans. `internal/archive/archive.go` additionally propagates `!binlogFile.Valid` into the `nulls[1]` slot so the Parquet writer emits a true NULL instead of smuggling an empty string. `cmd/bintrail/reconstruct.go`'s baseline-vs-first-event gap check used `first.BinlogFile > bmeta.BinlogFile`; with the scan fixes, `first.BinlogFile == ""` would silently evaluate as "no gap" — added a Warn-and-skip guard mirroring the adjacent `bmeta.BinlogFile == ""` branch. Side-fix in `internal/testutil/testutil.go`: `InitIndexTables` was missing the `connection_id` column (added to migrations + `init.go` but never propagated), so every integration test in `internal/query/` was already broken with `Unknown column 'connection_id' in 'field list'` before this work. The schema declares `binlog_file NOT NULL` — defensive scanning is appropriate regardless. Validated end-to-end: `internal/parquetquery/parquetquery_archive_integration_test.go` drifts MySQL, runs `ArchivePartition`, and reads back via DuckDB `parquetquery.Fetch` — one test covers all three layers, verified to fail with the exact #318 error message when any of the three fixes is reverted (#318).

### Fixed
- `bintrail shim` `_flashback` / `_snapshot` / `_diff` (and the `/*+ DBTRAIL_AT */` hint-comment form) now reject queries whose `WHERE` column does not match the table's declared primary key with `ER_PARSE_ERROR` (1064) instead of silently returning the wrong row. The parser captured the column name into `PKColumn` but only used it to dispatch (point-lookup vs full-table); the literal value was joined against `binlog_events.pk_values` regardless of column name, so `WHERE customer_id=1` against a table PK'd on `id` returned the row with `id=1` — a schema-valid resultset for a question the user never asked. `validatePKColumn()` now runs once in `HandleQuery` after `Parse`, reusing the existing `resolverCache` (same TTL, same sticky-fallback, same `Debug`/`Warn` log split as `columnOrderFor`). Reject branches name both the expected and supplied column; composite PKs and PK-less tables get distinct 1064s. Permissive when no snapshot is loaded, the resolver load fails, or the table is missing from the snapshot — preserves the `columnOrderFor` graceful-degradation contract so a broken snapshot lookup never turns a working query into a parse error. Validated end-to-end on RDS MySQL 8.0.46 + ProxySQL 2.6.6: 5 non-PK reject paths across all 4 query shapes, 2 regression paths (correct PK, no-WHERE full-table) (#296).
- `docs/time-travel-sql.md`: post-walkthrough fixes from the same E2E run. (a) `bintrail proxysql-config` emits four routing rules (`990001-990004`) since #294 added the `DBTRAIL_AT` hint rule, not three as Step 3 and the Troubleshooting recipe claimed. (b) Step 2 Ubuntu install block now uses the `signed-by=/etc/apt/keyrings/proxysql.gpg` keyring pattern instead of the deprecated `apt-key add -` — required on Ubuntu 22.04+ and removed in 24.04. (c) `bintrail init-shim` and `bintrail proxysql-config` no longer prefixed with `sudo` (they don't need root and root-owned 0600 generated files broke the next-step `mysql … < proxysql-setup.sql` redirect when the operator's shell was unprivileged); a new Prerequisites bullet tells operators to `chown` their `/etc/bintrail/` to the operator user at install time. `CLAUDE.md`'s `proxysql-config` row updated to match (#297, #298, #299).
- `docs/streaming.md`: four RDS-specific gotchas surfaced during the same walkthrough that operators hit silently otherwise. (a) AWS RDS for MySQL only enables binary logging when `backup-retention-period >= 1`; with `0` (a reasonable choice for a demo) `bintrail stream` / `bintrail index` fail with `ERROR 1381 (HY000): You are not using binary logging` while `SHOW VARIABLES` still happily reports the custom parameter-group `binlog_format=ROW`. (b) `bintrail stream` registers as a binlog client via `COM_REGISTER_SLAVE` and is rejected by RDS read-replicas (`read_only=1`) with `ER_OPTION_PREVENTS_STATEMENT` (1290) — `--source-dsn` must point at the primary, not a replica. (c) After raising backup retention from `0` to `1`, `@@log_bin` flips to `1` within ~2 minutes but `SHOW BINARY LOGS` stays empty until RDS completes its first automated snapshot (another ~30s-1min); a `bintrail stream` launched into that window exits with `no start position specified` and looks like the binlog enablement didn't work. (d) RDS caps `binlog retention hours` at 720 (30 days); values above the ceiling are rejected with `ERROR 1644 (45000)` — longer historical reach is the index's job (with `bintrail baseline` for replay anchors before the index window), not the RDS binlog buffer (#300, #304, #305, #306).

## [0.7.6] - 2026-05-22

### Added
- **(security, breaking change)** `bintrail-mcp-gateway` requires a per-tenant `auth_secret` on the OAuth authorize page so guessing a tenant ID alone is no longer sufficient to obtain an access token. **Strict mode**: a tenant without an `AuthSecretHash` cannot authorize — `POST /oauth/authorize` returns 401 whether the secret is wrong, missing, or unset (single uniform reject, no wire-level oracle). `POST /admin/tenants` requires `auth_secret` in the body (400 otherwise) so an admin fat-finger never lands an unmigratable tenant. The secret is bcrypt-hashed (`bcrypt.DefaultCost`) before persistence; the cleartext is never echoed on responses or in logs, and the hash itself is excluded from JSON responses (`json:"-"`) so a stolen admin token doesn't leak the hash inventory. The authorize HTML form has a required "Tenant Secret" password input alongside the existing "Tenant ID" field. PUT preserves an unspecified `auth_secret` (no silent clear); rotation = PUT with a non-empty value. `tenant_id` is constrained to `^[A-Za-z0-9_-]{1,64}$` at create time to defend against log-injection. **Operator migration**: every active tenant must have an `auth_secret` set before this PR is rolled out — discover unset tenants via `slog.Warn("authorize rejected: tenant secret missing or incorrect", tenant_id=…, legacy_no_secret_configured=true)` on every authorize attempt against an unmigrated tenant. The earlier "gradual rollout" design (302+code for legacy tenants) shipped briefly in this PR's first commit but was reversed after the multi-agent review because it preserved the very tenant-ID-only-auth vulnerability #132 was filed to close, AND added a wire-level oracle attackers could use to enumerate and authorize as legacy tenants. New direct dependency: `golang.org/x/crypto/bcrypt` (#132).

### Fixed
- `bintrail shim` accepts the optimizer-hint comment form documented on dbtrail.com: `SELECT /*+ DBTRAIL_AT='<ts>' */ * FROM [<schema>.]<table> [WHERE <col> = <value>]`. Previously the shim's parser only recognised explicit virtual-schema FROM clauses (`_flashback.<t>`, `_snapshot.<t>`, `_diff.<t>`), so the docs-advertised hint form hit `ER_NOT_SUPPORTED_YET` (1235) — the very routing ProxySQL was configured for refused the query. The shim now detects the hint at the start of `Parse`, rewrites it to a `TypeFlashback` point-lookup against the original table name, and runs through the existing flashback pipeline. Friendly for ORMs (Hibernate, Sequelize, etc.) that can't easily rewrite the FROM clause in app code. Malformed hints (bad timestamp, missing FROM, etc.) return a non-`ErrNotTimeTravel` error so `HandleQuery` emits `ER_PARSE_ERROR` (1064), preserving the user-input-vs-server-fault distinction. Detection is gated by a cheap probe regex so non-hint queries pay only one extra token check (#288).
- `bintrail agent` now refuses to start in BYOS mode (`--source-dsn` + `--server-id` set) when no flush sink is configured. Previously the metadata/payload flush sinks were only initialized when `--s3-bucket` was set, so a customer following the public install docs without that flag would see a healthy-looking WebSocket connection while the in-memory buffer accumulated events with no durable destination and dropped everything on restart — the SaaS Explorer / recover / who-changed flows all returned empty with no operator signal. The agent now hard-fails at startup with an error naming both `--s3-bucket` and `BINTRAIL_S3_BUCKET`, surfacing the misconfiguration at the system boundary rather than as silent zero-data drift (#289).
- `bintrail shim` point-lookup `_flashback` / `_snapshot` now returns an empty resultset when the latest event at-or-before AS OF is a DELETE — matching the full-table reconstruction path (#276) and the Oracle `AS OF` semantic the docs already advertise (`docs/time-travel-sql.md`). Previously the PK-filtered path resurrected the row by returning the DELETE's `row_before` — a behaviour intentionally shipped in 0.7.5 ("point-lookup path is unchanged") but inconsistent with the no-`WHERE` path, the docs, and operator expectations. The forensic "what did this row look like right before deletion?" question is still answerable via `_diff`, which returns the full per-PK history including the DELETE's `row_before`. `docs/time-travel-sql.md`'s "Time-travel query returns empty" section now enumerates the DELETE cause and points operators at `_diff` for the distinction. The convergence is pinned by `TestSelectImage.delete_returns_nil` + `TestExtractFullTableImages` (#287).

## [0.7.5] - 2026-05-09

### Added
- `bintrail shim` reconstructs the full row state of a table at AS OF when the query omits `WHERE`: `SELECT * FROM _flashback.<table> AS OF '<ts>'` (and the same against `_snapshot.<table>`) returns every row that existed at the requested instant. DELETE events are correctly suppressed — rows that didn't exist at AS OF don't appear in the resultset (matches Oracle's `AS OF` semantic). The PK-filtered point-lookup path is unchanged. Buffered with a 100,000-row cap; overflow surfaces as `ER_TOO_BIG_SELECT` (1104) so monitoring can distinguish it from a real shim crash. Streaming the resultset (no cap) is deferred until an operator reports it as a bottleneck. JOINs, aggregations, and non-PK WHERE filters are intentionally out of scope — pipe the resultset to `duckdb`, `pandas`, or any downstream SQL tool (#276).
- `bintrail shim` accepts `--auth-method` (and `BINTRAIL_AUTH_METHOD`) to advertise `caching_sha2_password` or `sha256_password` instead of the historical `mysql_native_password` default. Useful on MySQL 8.4+ instances where `mysql_native_password` is disabled by policy. The default is unchanged so existing deployments see no behaviour difference. Implementation generates an in-memory self-signed RSA-2048 keypair once per process (the `cacheShaPassword sync.Map` and tlsConfig live on a single shared `*server.Server`, so SHA2 caching actually caches and a typo in the flag fails the daemon at startup, not silently per-connection). Requires ProxySQL 2.7+ between the application and the shim — the LTS 2.6 line isn't verified to negotiate SHA2 against backends. See `docs/time-travel-sql.md` Step 4 for the systemd recipe (#274).

### Fixed
- `bintrail shim` returns typed MySQL wire codes for client-input rejections so ORMs and monitoring can distinguish user input errors from server faults: `ER_PARSE_ERROR` (1064) for malformed time-travel queries (recognised virtual schema, bad shape / bad AS OF / missing `USE <db>`), `ER_NOT_SUPPORTED_YET` (1235) for non-time-travel queries routed to the shim. Real internal failures (DB timeout, archive S3 outage, build-resultset bug) keep returning `ER_UNKNOWN_ERROR` (1105) so the catch-all "the server is broken" signal is preserved (#277).
- `bintrail shim` returns `ER_NO_PARTITION_FOR_GIVEN_VALUE` (1526) for coverage-gap errors instead of the catch-all 1105, so an operator monitoring 1105 spikes can tell "the user asked for an AS OF outside index retention" apart from "the shim crashed". Wrap is localised in `runPointInTime` / `runDiff` via `errors.Is(err, *query.GapError)`; everything else still returns 1105 (#283).

## [0.7.4] - 2026-05-05

### Fixed
- `bintrail shim` no longer triggers ProxySQL to SHUNN its hostgroup when `monitor` (or any other unknown user) probes the listener. `TenantAuth.GetCredential` now returns `server.ErrAccessDenied` for unknown users, which `(*Conn).handshake` translates to `ER_ACCESS_DENIED_ERROR` (1045) on the wire. Previously the library raised `ER_NO_SUCH_USER` (1449) — ProxySQL classifies that as "backend broken" and SHUNNs the backend, making time-travel queries appear to silently return empty for ~N seconds at a time after every monitor probe. The shim's `handleConn` also classifies handshake errors via a new `classifyHandshakeErr` helper that uses `errors.Is` / `errors.As` against typed sentinels (`io.EOF`, `mysql.ErrBadConn`, `*mysql.MyError` with code 1045) instead of substring matches — so ProxySQL TCP probes drop to `Debug` and auth failures to `Info`, keeping the steady-state shim log clean (#262).
- `bintrail shim` now accepts fully qualified time-travel queries like `SELECT * FROM _flashback.orders AS OF '...' WHERE id = 1` without requiring a prior `USE <db>`. The shim derives each tenant's source schema from `source_dsn` (via `mysqldriver.ParseDSN`) and pre-seeds `Handler.db` after a successful handshake; an explicit later `USE` from the client still wins. A misconfigured tenant (unparseable or path-less DSN) falls back to the previous "issue USE first" behaviour with a per-tenant warning, and `runShim` emits a startup-time summary log listing the affected users so operators can spot configurations where most tenants will need to issue `USE` manually. New exported `shim.LoadTenantConfigs` returns the validated tenant slice (including `SourceDSN`); `shim.LoadTenants` is preserved as a thin wrapper so existing callers and tests are unchanged (#263).

## [0.7.3] - 2026-05-04

### Fixed
- `bintrail shim` no longer falsely classifies the current hour as a coverage gap. The shim was passing the user query's schema (e.g. `e2e_source`) as `DBName` to `query.FetchMerged`, which feeds the planner. The planner reads `information_schema.PARTITIONS WHERE TABLE_SCHEMA = ?`; with the wrong schema it returned zero partitions and treated every queried hour as rotated-out. Under `AllowGaps=false` (the default since 0.7.2) `_diff` queries aborted with a coverage-gap error; `_flashback` and `_snapshot` silently returned zero rows. The shim now plumbs the index DB name from the index DSN through a new `shim.Config.IndexDBName` field. As a hardening bonus, `bintrail shim` refuses to start if the index DSN cannot be parsed or omits the database — both conditions previously degraded silently. New integration tests (`-tags integration`) catch real `information_schema.PARTITIONS` shape drift the existing sqlmock tests cannot (#259, #260, #261).

## [0.7.2] - 2026-05-04

### Fixed
- `bintrail shim` now actually authenticates ProxySQL-forwarded connections. The previous TenantAuth implementation returned an empty cleartext from `GetCredential`, so the `mysql_native_password` handshake only succeeded when the client sent an empty password — every real ProxySQL → shim connection in 0.7.0 / 0.7.1 was rejected with `Access denied`. The shim now stores the cleartext password (read from a new `mysql_password` field in `shim.yaml`) and validates the client's scrambled response against it, the same way any MySQL server does (#254).
- `bintrail shim` virtual-schema queries no longer crash with `ArchiveFetcher is required when NoArchive is false`. `runPointInTime` and `runDiff` now wire `parquetquery.Fetch` as the archive fetcher — the same fetcher `bintrail query` and `bintrail recover` use — so S3 archive auto-discovery works out of the box without `--no-archive` (#255).
- `bintrail shim` no longer silently returns partial results when an archive source fails or the planner detects a coverage gap. The shim previously hard-coded `AllowGaps=true`, inheriting `bintrail recover`'s warn-and-continue behaviour — but on the wire-protocol path the connecting MySQL client has no stderr channel, so transient S3 throttling / IAM rotation / DuckDB load failures produced a successful-looking resultset missing rows the customer could not detect. The default is now strict: archive failures and planner-detected gaps abort the customer's query with a visible MySQL protocol error. Operators who explicitly want the previous warn-and-continue behaviour pass `--allow-gaps`. The library default in `internal/shim/handler.go`'s `NewHandler` is also flipped for consistency, and the new default is triple-pinned by tests so a future regression cannot silently revert it (#257, #258).

### Changed (breaking, but only relative to the non-functional 0.7.0 / 0.7.1 shim auth path)
- `shim.yaml`'s tenant block now requires `mysql_password` (cleartext) instead of `mysql_pass_sha1` (the SHA1 hex). `bintrail proxysql-config` recomputes the SHA1 from the cleartext at SQL-generation time, so operators no longer need to run a manual SHA1 recipe; the shim itself needs the cleartext to validate ProxySQL-forwarded auth (see #254). Existing `shim.yaml` files using only `mysql_pass_sha1` now error at startup with a clear migration message pointing at the new field. The legacy field is parsed (not rejected) by the strict YAML decoder so the error message can be specific.

## [0.7.1] - 2026-05-03

### Fixed
- `bintrail shim` no longer silently truncates `_diff` responses at 1000 rows. The arbitrary cap dropped a partial audit history with no signal to the customer for any PK that exceeded it within the requested window. A `_diff` query is already PK-scoped and time-windowed, so the cap was protecting against a non-issue; customers who hit a real problem can narrow the `BETWEEN` range. The `Options.Limit` doc comment in `internal/query/query.go` is also corrected (`0 → no limit`, not the stale `0 → default 100`) so the contract `_diff` now relies on is documented (#245, #249).
- `bintrail shim`'s accept loop now uses exponential backoff (100ms → 5s, doubling) instead of a fixed 100ms `time.Sleep`. A wedged listener (fd exhaustion, transient kernel state) previously emitted ~10 error log lines per second indefinitely while the process appeared alive; the new shape cuts steady-state log volume by ~50× without introducing a magic threshold. The sleep is also now a cancellable `select`, so a SIGTERM during a long backoff returns immediately instead of waiting up to 5s (#247, #251).

### Changed
- The image-selection branch of `internal/shim/handler.go` `runPointInTime` is extracted as a pure `selectImage([]query.ResultRow) map[string]any` helper. Behaviour is observably identical to the prior switch — `RowAfter` wins when present, falls back to `RowBefore` for DELETE events, returns nil for empty input or both-images-empty — but the priority rule is now testable without sqlmock or a real MySQL. Six table-driven cases lock the contract, including the `len() > 0` vs `!= nil` boundary that would silently regress DELETE handling on an empty non-nil RowAfter (#248, #250).

## [0.7.0] - 2026-05-03

### Added
- New `bintrail shim` subcommand: an in-process MySQL-protocol server that answers BYOS time-travel SQL queries (`_flashback.*`, `_diff.*`, `_snapshot.*` virtual schemas) by translating them into the existing `query.FetchMerged` engine. The shim sits behind ProxySQL on `127.0.0.1:3308` by default; ProxySQL is the password gate, the shim validates only that the connecting username appears in `shim.yaml`. Recognised statement shapes: `SELECT * FROM _flashback.<table> AS OF '<ts>' WHERE <col> = <value>`, the same shape against `_snapshot`, and `SELECT * FROM _diff.<table> BETWEEN '<t1>' AND '<t2>' WHERE <col> = <value>`. Time-range queries auto-discover S3 archives via `archive_state` so rotated-out hours resolve transparently.

### Changed
- The BYOS time-travel SQL story is now self-contained in the `bintrail` binary — there is no separate `dbtrail-shim` binary to download. `bintrail init-shim`'s output drops the `agent_url` and `agent_token` fields (no longer needed); `BINTRAIL_API_KEY` is no longer a precondition for scaffolding shim.yaml. The default `--listen` becomes `127.0.0.1:3308` so the shim is not reachable from the network unless the operator explicitly opens it. `docs/byos-time-travel-sql.md` rewritten to walk the customer through `bintrail shim` end-to-end. The systemd unit ships at `deploy/bintrail-shim.service` (renamed from `dbtrail-shim.service`).

### Removed
- The "Attach dbtrail-shim binaries to release" step in `.github/workflows/release.yaml`. That step pointed at a bucket that does not exist (the placeholder name in issue #236 was taken literally) and failed with HTTP 403 on every release. The shim now ships as a subcommand of the `bintrail` binary itself, so no cross-repo S3 pull-through is needed.

## [0.6.0] - 2026-05-02

### Added
- GitHub releases now include `dbtrail-shim` binaries (`dbtrail-shim-linux-amd64`, `dbtrail-shim-linux-arm64`, `dbtrail-shim-darwin-amd64`, `dbtrail-shim-darwin-arm64`) alongside the existing bintrail archives. The shim enables BYOS time-travel SQL via ProxySQL routing; binaries are pulled from the canonical SaaS build pipeline and re-published as release assets, so customers can `curl -LO https://github.com/dbtrail/bintrail/releases/latest/download/dbtrail-shim-linux-amd64` (#236).
- `bintrail init-shim` scaffolds a `shim.yaml` for the dbtrail-shim binary from the customer's existing `.bintrail.env`. Reads `BINTRAIL_SOURCE_DSN`, `BINTRAIL_SERVER_ID`, and `BINTRAIL_API_KEY` (validating all three are set and free of newlines, listing any that are missing in the error). Emits a deterministic YAML file with single-quoted scalars (safe against `:`, `#`, `'`, leading whitespace) and `mysql_user` / `mysql_pass_sha1` as TODO comments for the customer to fill in. Mirrors the patterns from `bintrail config init` and `bintrail generate-key`: refuse-overwrite, 0o600 perms, `--out -` for stdout, byte-identical re-runs (#237).
- `bintrail proxysql-config` reads `BINTRAIL_SOURCE_DSN` and a customer-edited `shim.yaml` and emits a deterministic, idempotent SQL script the customer applies to the ProxySQL admin port. The SQL configures `mysql_servers` (hostgroup 990 = real MySQL, 991 = shim), one `mysql_users` row per tenant in shim.yaml, and three `mysql_query_rules` (990001-990003) routing `\b_flashback\.`, `\b_diff\.`, `\b_snapshot\.` to the shim hostgroup. All numeric IDs sit in the 990* range to avoid colliding with operator-managed ProxySQL config; the DELETEs are scoped strictly to that range (mysql_users by `default_hostgroup = 990`) so a username collision with an operator user in another hostgroup surfaces as a loud PRIMARY KEY violation rather than silent destruction. The script wraps DML in `BEGIN`/`COMMIT` so a partial failure rolls back the whole change set; LOAD/SAVE run after COMMIT. Customer-supplied values flow through a single-quote-doubling `sqlQuote` helper, and shim.yaml is parsed with `yaml.UnmarshalStrict` so a typo like `mysql_user_name:` surfaces as "field not found" instead of "mysql_user is empty" (#238).
- New `docs/byos-time-travel-sql.md` walks a BYOS customer end-to-end from zero to a working `SELECT * FROM _flashback.t AS OF '...' WHERE id = N` on a fresh Ubuntu 22.04 / Amazon Linux 2023 host: shim binary download, `init-shim`, ProxySQL install, `proxysql-config`, systemd unit, application connection switch, sample flashback query, and troubleshooting (auth denied, missing query rules, shim port unreachable, agent unreachable, operator users in hostgroup 990). A copy of the systemd unit ships at `deploy/dbtrail-shim.service` (#239).

## [0.5.8] - 2026-04-15

### Added
- `bintrail query` accepts `--include-snapshot` + `--baseline <path-or-s3-url>` to merge a mydumper baseline Parquet as a third source alongside the live MySQL index and S3 archive. Baseline rows are emitted as synthetic `SNAPSHOT` events (new `parser.EventSnapshot = 6`) with the baseline's `snapshot_timestamp` metadata as their `event_timestamp`, so they flow through the existing `MergeAndTrim` pipeline and slot into sorted output before any subsequent binlog event for the same PK. Filter semantics: `--column-eq` hits the typed Parquet column via DuckDB `CAST(… AS VARCHAR) = ?` (index-friendly equality, no `JSON_EXTRACT`); `--event-type ≠ SNAPSHOT`, `--gtid`, `--changed-column`, `--flag` all exclude the snapshot source with a visible `slog.Info` reason; `--since`/`--until` compare against the baseline's recorded creation timestamp. `--include-snapshot` rejects combinations that would silently produce wrong data (`--pk`/`--pks`: snapshot rows have no `pk_values` in this release; `--profile`: RBAC rules are not applied to snapshot rows) and requires `--baseline` + `--schema` + `--table`. `--event-type SNAPSHOT` without `--include-snapshot` is rejected (previously silently returned zero rows). Unblocks dbtrail SaaS Phase 2 FK-aware cascade victim reconstruction for rows that existed before streaming began (#234).

## [0.5.7] - 2026-04-15

### Added
- `bintrail query` and `bintrail recover` now accept `--column-eq column=value`, a repeatable filter that matches events where a column inside `row_after` **or** `row_before` equals the given value. The OR across both sides covers DELETEs (value in `row_before`) and INSERTs (value in `row_after`) symmetrically. Repeating the flag composes AND. A column-name allowlist (`[A-Za-z0-9_]`) keeps the interpolated JSON path safe. The literal unquoted `NULL` sentinel matches rows where the column is explicitly JSON null (via `JSON_TYPE = 'NULL'`). The same filter is mirrored into the DuckDB archive path so merged live + archive queries stay consistent. MCP `query` and `recover` tools accept a matching `column_eq: [string]` parameter (#229).
- `bintrail query` and `bintrail recover` now accept `--pks` (comma-separated or repeatable) and `--limit-per-pk N` for batched multi-PK lookups. On the archive path this collapses N sequential DuckDB scans into one pass with `WHERE pk_values IN (...)` and `QUALIFY ROW_NUMBER() OVER (PARTITION BY pk_values ORDER BY event_timestamp DESC, event_id DESC) <= N`; on the MySQL side a ROW_NUMBER subquery enforces the per-PK cap server-side. The primary workload is dbtrail SaaS's "find latest DELETE per PK" auto-detection pass, which now completes in a single invocation instead of N shell-outs re-opening MySQL and re-scanning the same parquet files each time. `bintrail query --pks=a,b,c --format=json` emits a grouped shape `{"results": [{"pk": "X", "events": [...]}, ...]}` preserving input order, with empty groups for PKs that had no matches so callers can correlate inputs to outputs without a second lookup. `--pk` keeps its existing SHA2-indexed fast path. `--pk` and `--pks` are mutually exclusive; `--limit-per-pk` requires one of them; empty or whitespace-only PKs in `--pks` are rejected with a clear error, and duplicates are deduplicated with first-occurrence-wins ordering. Cross-source merge applies the per-PK cap before the global `--limit` trim via a new shared `query.MergeAndTrim` helper, so a large `--limit-per-pk` cannot starve later PKs under the ASC-sorted global cutoff (#231).

## [0.5.6] - 2026-04-14

### Fixed
- BYOS agent now detects two classes of silent source-identity misattribution. (1) The source `@@server_uuid` is re-read every 60 seconds and compared against the UUID captured at startup; if the source MySQL restarts with a regenerated `auto.cnf`, fails over behind a VIP, or resolves to a different instance, the agent exits with an actionable error instead of continuing to stamp the stale UUID on every `MetadataRecord`. Transient DB errors on the identity tick are tolerated with a warning. (2) `byos.EnsurePartitionKey` writes a `.bintrail-partition-key` marker to the customer S3 prefix on first run and validates it on every subsequent run; a mismatch (e.g. upgrading across the `--index-dsn` UUID-vs-numeric partition-key cutover) hard-fails with a message explaining the cutover and the operator's remediation options. An additional startup warning fires in the BYOS+S3-without-index-dsn path so the chosen partition key appears in the banner (#196, #198, #228).

### Changed
- S3 archive downloads for `query`/`recover` are now a single-pass prefetch pipeline instead of a per-batch download-then-query loop. Up to 2 files are prefetched in parallel while DuckDB queries the current one; queries remain strictly sequential (one DuckDB query at a time), so peak per-query RAM is unchanged, and peak temp files on disk drops from 4 to 3. Early termination is now checked after every file rather than at batch boundaries, so wide time-range queries with `--limit` stop as soon as the collected results cannot be displaced by any later file. S3 download errors that race with consumer cancellation are now logged at Debug for context errors and Warn for real failures (403, DNS, throttling) so production problems surface (#225, #227).

## [0.5.5] - 2026-04-13

### Fixed
- `bintrail baseline` now emits valid 0-row Parquet files for empty tables (tables with a schema DDL but no data rows in the mydumper dump). Previously, empty tables were silently skipped, causing `bintrail reconstruct` to fail with "no baseline snapshot found" for any table that was legitimately empty at dump time. Views are still correctly skipped via a `CREATE TABLE` vs `CREATE VIEW` heuristic on the schema SQL file (#226).
- `bintrail reconstruct` now handles missing baselines gracefully as defense-in-depth: when no baseline Parquet exists for a table, it emits a warning and treats the table as empty instead of aborting the entire reconstruct run. This covers pre-fix baselines and tables created after the last baseline snapshot (#226).

## [0.5.4] - 2026-04-13

### Changed
- S3 archive queries (`query --archive-s3`, `recover --archive-s3`) now use prefix-scoped S3 listing, batched concurrent downloads (4 at a time), chronological file ordering, and early termination when `--limit` is satisfied. A 24-hour query that previously took 18s+ now completes in under 5s; queries with small limits terminate after downloading only the first few files. The SaaS backend can remove its 144-chunk splitting logic entirely (#225).

## [0.5.3] - 2026-04-13

### Fixed
- `bintrail reconstruct` S3 baseline discovery now works with DuckDB v1.4.4, which changed `glob()` from a scalar function to a table function. The previous `SELECT unnest(glob('s3://...'))` syntax fails with a Binder Error; replaced with `SELECT * FROM glob('s3://...')` which works across DuckDB versions (#223).

## [0.5.2] - 2026-04-12

### Fixed
- `bintrail baseline` now recognizes mydumper 0.10.0's unchunked data-file naming (`<db>.<table>.sql`) in addition to the chunked format (`<db>.<table>.<chunk>.sql`) used by mydumper >= 0.11.0. Ubuntu 24.04's apt package ships mydumper 0.10.0 which produces single data files without a numeric chunk suffix; `DiscoverTables` previously required the chunk number and silently skipped these files, producing zero Parquet output. Together with #219 (mydumper version probing in `bintrail dump`), this unblocks the entire `dump → baseline → reconstruct --output-format mydumper` pipeline on Ubuntu 24.04 (#221).

## [0.5.1] - 2026-04-12

### Fixed
- `bintrail dump` no longer passes `--sync-thread-lock-mode` and `--trx-tables` to mydumper versions older than 0.11.0. These flags were introduced in mydumper 0.11.0 but were hardcoded in `buildMydumperArgs`, so `bintrail dump` failed immediately on Ubuntu 24.04's apt-installed mydumper 0.10.0 with `Unknown option --sync-thread-lock-mode`. The fix probes the local mydumper binary's version via `mydumper --version` and conditionally includes the flags only when the version is >= 0.11.0. Docker-mode invocations (`--mydumper-image`) always include the flags since the official Docker image ships a recent version. When the version cannot be determined, the flags are omitted conservatively with a `slog.Warn`. This unblocks the entire `dump → baseline → reconstruct --output-format mydumper` pipeline on Ubuntu 24.04 (#219).

## [0.5.0] - 2026-04-11

### Added
- `bintrail reconstruct --output-format mydumper` reconstructs entire tables at a target point in time and emits a mydumper-compatible dump directory (schema file, chunked INSERT files, and a `metadata` file with baseline binlog position). The output is restorable with plain `mysql < *.sql` or `myloader` with no further binlog replay needed — closes the gap between single-row investigation and full point-in-time recovery. The algorithm is merge-on-read: stream the baseline Parquet row-by-row via DuckDB, look up each row's PK in an in-memory change map built from the merged MySQL + archive event set, and emit SQL directly. No MySQL restore cycle, no InnoDB buffer warmup. Requires baselines written by this version (embeds the raw `CREATE TABLE` text as a new `bintrail.create_table_sql` Parquet metadata key); older baselines are rejected with a clear re-baseline message. New flags: `--output-format mydumper`, `--output-dir`, `--tables schema.table,...`, `--chunk-size` (default `256MB`), `--parallelism` (default `runtime.NumCPU()`). Strict `--allow-gaps=false` semantics inherited from single-row reconstruct — a coverage gap between baseline and target aborts unless the user explicitly accepts incomplete data. Supported PK types: integer (int/smallint/tinyint/mediumint/bigint), string (char/varchar/text/enum/set), DATETIME, TIMESTAMP, DATE, YEAR. Any PK column outside the allow-list (DECIMAL, BINARY, BLOB, BIT, JSON, spatial types, and any unknown type) causes reconstruct to hard-fail at start with a clear error message naming the column; #214 tracks expanding the supported set. UPDATE events that mutate the primary key itself are not handled — the change map is keyed by the before-image PK, so the after-image row may be dropped. Re-snapshot the baseline after PK-reshaping schema changes (#187).
- Full-table reconstruct now supports **DECIMAL and NUMERIC primary key columns**. Previously these were rejected at `ReconstructTable` entry because the #212 canonicalizer allow-list only covered integer, string, datetime, date, and year types. Investigation for #214 showed DECIMAL is the one type in the originally-listed set that can be added as a pure canonicalizer change without touching on-disk formats: go-mysql v1.13.0's `decodeDecimal` returns a pre-formatted Go string when `useDecimal` is false (the bintrail default — never set in `cmd/bintrail/stream.go` or `agent.go`), and the baseline writer stores DECIMAL as `parquet.String()`, which DuckDB reads back as a Go string. Both sides land on byte-identical strings for every representable value including zeros (`"0.00"` not `".00"` — `decodeDecimal` at `replication/row_event.go:1565-1567` explicitly writes `"0"` for zero-leading integer parts), so the canonicalizer branch is a type-check + pass-through. BINARY, VARBINARY, BLOB variants, BIT, and JSON remain deferred with explicit hard errors: each has a real representation mismatch between the indexer's `%v`-of-`[]byte` format and `baseline.parseSQLValue`, which does not decode MySQL hex literals (`0x...`), bit-string literals (`b'...'`), or convert JSON between raw-bytes and string forms. Fixing any of those requires a non-additive change to either `parser.BuildPKValues` or `internal/baseline/reader_sql.go`, tracked as separate follow-up issues. New integration test `TestRunReconstruct_fullTableRoundTrip_decimalPK` covers the full round-trip end-to-end (zero, positive, negative, INSERT-via-event); new regression test `TestRunReconstruct_rejectsRemainingUnsupportedPKTypes` is a table-driven pin against accidental allow-list expansion without a matching representation fix (#214).
- `bintrail baseline` now embeds the raw mydumper `<db>.<table>-schema.sql` contents in the Parquet file as `bintrail.create_table_sql`. Full-table reconstruct (#187) reads this back to emit a faithful `CREATE TABLE` in its schema file without re-synthesising from Parquet column types (which would lose indexes, foreign keys, charsets, engine, etc.).
- `recovery.FormatSQLValue`, `recovery.EscapeString`, `recovery.QuoteName` exported from `internal/recovery` so the full-table mydumper writer reuses the exact MySQL literal formatting used by reversal SQL. `FormatSQLValue` extended to handle `int64`/`int32`/`int`/`uint64`/`uint32`/`float32`/`time.Time`/`[]byte` (DuckDB scan types) in addition to the JSON-round-tripped types it already handled.
- `baseline.ReadParquetMetadataAny` reads Parquet metadata from either local paths or `s3://` URLs (using DuckDB's `parquet_kv_metadata` via the `httpfs` extension), so full-table reconstruct works against S3-resident baselines without adding a direct AWS SDK dependency.
- Benchmark harness for the full-table reconstruct merge loop at `internal/reconstruct/fulltable_bench_test.go`. Parameterised on baseline size × change rate (100k/1k, 1M/10k, 10M/100k at 1% change rate), reports `rows/sec` and `hit-ratio` as custom metrics via `b.ReportMetric`, and asserts the end-state counters on the first iteration so a regression in the merge loop surfaces as a benchmark failure instead of a silent throughput drop. This is the preparatory measurement work for #207 (row-group-level PK-range pruning). Run via `go test -bench=BenchmarkMergeBaselineIntoWriter -benchmem -run=^$ -count=3 ./internal/reconstruct/`. The 10M tier is gated behind `-short` because the synthetic fixture build alone is ~30s. **Initial numbers on Apple M1** (zstd-compressed 500k-row-group Parquet, single-column INT PK, evenly-spaced change events): 100k/1k ≈ 98ms / 1.02M rows/sec, 1M/10k ≈ 785ms / 1.27M rows/sec, 10M/100k ≈ 8.56s / 1.17M rows/sec. Throughput is linear across three orders of magnitude — the merge loop is not allocation-bound, so the speedup headroom #207 would unlock is bounded by Amdahl on a workload that already sustains >1M rows/sec. Extrapolated to a 50GB / ~500M-row baseline: roughly 7 minutes wall clock. This number goes in the #207 thread for the pruning decision (#207).

### Fixed
- `bintrail query` now **always surfaces archive fetch failures on stderr** and aborts immediately on context cancellation. The archive loop previously wrapped every `parquetquery.Fetch` error in a `slog.Warn` and continued, so expired AWS credentials, S3 `AccessDenied`, DuckDB `memory_limit` OOM, corrupted Parquet files, and `Ctrl-C` all produced partial/empty results with exit 0 and no visible signal at the default text log level. 0.4.8's #203 fix addressed the specific Binder Error trigger (pre-0.4.4 parquets missing the `connection_id` column) at the `parquetquery` layer, but left the surrounding silent-swallow anti-pattern untouched — any future archive failure mode would have reproduced the same six-days-of-empty-results outage. The fix extracts the entire archive fetch loop into `queryArchiveSources(ctx, sources, opts, fetch, stderr)` which (a) prints `Warning: archive query failed for <src>: <err>` to stderr regardless of log level via a `lineBreakReplacer` that collapses every line-terminator character (`\r\n`, `\r`, `\n`, `\v`, `\f`) to ` | ` so one failure = one stderr line, (b) still emits the structured `slog.Warn` with the raw (unsanitized) error for log-format=json consumers and full-fidelity debugging, and (c) runs a dual cancellation check — `ctx.Err()` AND `errors.Is(err, context.Canceled/DeadlineExceeded)` — so Ctrl-C short-circuits the query immediately instead of iterating every remaining source printing warnings. Non-cancellation errors keep the per-source "log and continue" semantics operators rely on — one broken archive still does not kill the whole query, only ones that are visibly broken. The helper takes `query.ArchiveFetcher` (the same named type `FetchMerged` uses) so signature drift with the shared pipeline becomes a compile error. Scoped to `cmd/bintrail/query.go` to preserve the existing (already-tested) `recover`, `reconstruct`, agent, and MCP-server paths untouched; the same silent-swallow still lives in `internal/query/fetchmerged.go`, `cmd/bintrail-mcp/main.go`, and `internal/agent/handler.go` as tracked follow-ups (#203).
- Full-table reconstruct now correctly handles **DATETIME/TIMESTAMP primary keys at every declared fractional precision** (0 through 6). The initial #187 implementation used `fmt.Sprintf("%v", ...)` to hash PK values on both sides of the merge, but the bintrail indexer stores DATETIME pk_values as go-mysql-formatted strings with the column's declared precision (`"14:30:45"` for DATETIME(0), `"14:30:45.123456"` for DATETIME(6)) while DuckDB's `parquet_scan` returns the same column as a `time.Time`. The resulting `%v` strings diverged and every event for a DATETIME-PK table silently missed the baseline, producing a dump with duplicate PK rows. **Schema change**: `schema_snapshots` gains a `column_type` column (e.g. `"datetime(6)"`) populated by `TakeSnapshot` from `information_schema.COLUMNS.COLUMN_TYPE`. `indexer.EnsureSchema` adds it idempotently on startup so existing installations upgrade transparently; `ReconstructTables` and the MCP server's `recoverTool` also run the migration eagerly because those code paths previously didn't. The new `canonicalizePKValue` helper in `internal/reconstruct` reads the declared precision from `ColumnType` and formats DuckDB values to match go-mysql's `formatDatetime` output exactly. Pre-#212 snapshots (empty ColumnType) fall back to a best-effort heuristic that works for DATETIME(0); full-table reconstruct emits an `slog.Warn` at the start of the run whenever a DATETIME/TIMESTAMP PK column has an empty ColumnType, instructing the operator to re-run `bintrail snapshot`. The canonicalizer **hard-fails** on nil PK values, missing PK columns, unknown DuckDB scan types, and any PK column type outside the supported allow-list (integers, strings, enum/set, datetime/timestamp/date, year) — all conditions that previously could silently corrupt the output. Any PK column outside the allow-list (including DECIMAL, BINARY, VARBINARY, BLOB, BIT, JSON, and spatial types) is rejected at `ReconstructTable` entry; #214 tracks extending support to those types. `MissingPKColumnError` is now a typed error exposing the offending column name via `errors.As`. `ReconstructTables` aggregates multi-table failures via `errors.Join` so every per-table error surfaces in the CLI exit wrap. 4 new integration tests pin the contract end-to-end: `TestRunReconstruct_fullTableRoundTrip_datetimePK` (DATETIME(0)), `TestRunReconstruct_fullTableRoundTrip_datetime6PK` (DATETIME(6) with mixed whole-second and microsecond values — the empirical validation of the precision-aware fix), `TestRunReconstruct_fullTableRoundTrip_varcharPK`, plus the pre-existing #187 INT PK round trip. A dedicated canonicalizer unit test suite covers every supported type, precision 0-6, fallback heuristic, and every hard-fail path (#212).
- `bintrail reconstruct --pk ...` now fetches events from both live MySQL partitions **and** Parquet archives, closing a latent correctness bug where single-row reconstruction silently missed events that had been rotated out of MySQL and archived. The previous code called `engine.Fetch` directly — bypassing the query planner, archive auto-discovery, and `MergeResults` pipeline already used by `bintrail recover`. Factored out a shared `query.FetchMerged` helper so `recover` and `reconstruct` share one pipeline, and the upcoming full-table reconstruct (#187) can reuse it. New `--no-archive` and `--allow-gaps` flags on `reconstruct` mirror `recover`'s surface area; `--allow-gaps` defaults to `false` because a silently incomplete row state is worse than a clear error for a recovery tool. Strict mode (`AllowGaps=false`) now aborts when the query planner fails, when no DBName is available for gap detection, or when every archive source fails — previously these conditions silently degraded to partial data. The query planner now runs regardless of `--no-archive` so users retain gap visibility even when opting out of archive queries (#209).

## [0.4.9] - 2026-04-10

### Added
- BYOS buffer now supports size-based eviction via `--buffer-max-events` and `--buffer-max-bytes` flags (e.g. `--buffer-max-bytes 256MB`). Previously the in-memory buffer only evicted by age (`--buffer-retain`), so a write burst within the retention window could grow RAM unbounded. When a cap is exceeded, the oldest events are evicted FIFO with a `slog.Warn` for operator visibility. New heartbeat fields `buffer_bytes` and `size_evictions` report buffer pressure to dbtrail. Both caps default to 0 (unlimited) for backward compatibility (#194).

## [0.4.8] - 2026-04-10

### Fixed
- Archive queries against pre-v0.4.4 parquet files no longer silently return 0 events. Older parquets lack the `connection_id` column added in 0.4.4; DuckDB threw a `Binder Error` when the per-file query SELECTed that column from a single file (where `union_by_name=true` is a no-op). The error was swallowed by the caller and the query returned empty. Fix: probe the parquet schema before building the SELECT and substitute `NULL::INT32 AS connection_id` when the column is absent. Applied to both the S3 per-file download path and the local glob path (#203).

## [0.4.7] - 2026-04-09

### Added
- `bintrail agent` now exits with distinct process codes when the dbtrail backend rejects the WebSocket with a permanent close: **64** for auth/config failures (`missing_credentials`, `invalid_key`, `wrong_tenant_mode`) and **65** for `rate_limited`. systemd units should add `RestartPreventExitStatus=64 65` to stop respawning on permanent failures — previously every fatal close exited 1 and the supervisor kept respawning a doomed agent. Unknown reason strings on a fatal close code fall through as a transient exit (safe to respawn) so backend contract drift never silently pins the agent into a fatal loop. The permanent-error log line now includes `close_code` / `close_reason` structured fields for grep/alerting. Recognizes both canonical short forms (`invalid_key`) and legacy human strings (`Invalid API key`) for back-compat with older dbtrail versions (#201, #202).

## [0.4.6] - 2026-04-08

### Fixed
- `bintrail rotate --add-future N` is now declarative: it maintains *at least* N future hourly partitions beyond the current hour (top-up only) instead of adding N new partitions per invocation. In daemon mode the old behaviour leaked `+N` partitions per cycle — a demo tenant running `--add-future=1 --interval=1h` accumulated 413 partitions over 17 days and blew up `SELECT DISTINCT` on `binlog_events` from sub-second to 23s. Semantics now match the documentation in `docs/deployment.md` (#199).

## [0.4.5] - 2026-04-07

### Removed
- BYOS agent no longer requires `--index-dsn` when `--s3-bucket` is configured (#197). The requirement was added in 0.4.1 to guarantee stable S3 partition keys via a locally-persisted `bintrail_id`, but it forced customers to provision a dedicated MySQL (a footgun the customer-facing setup docs never mentioned). With the source identity propagation shipped in 0.4.4 (#195), the dbtrail SaaS side now resolves a stable `bintrail_id` server-side from the `@@server_uuid` + host/port/user fields carried on every metadata record (architecture §22.11, nethalo/dbtrail#1179). The local customer agent falls back to `--server-id` for the S3 partition key and WebSocket heartbeat label, which are customer-local identifiers intentionally decoupled from the SaaS-resolved `bintrail_id`.

## [0.4.4] - 2026-04-07

### Added
- BYOS agent now propagates source server identity (`server_uuid`, `source_host`, `source_port`, `source_user`) on every metadata record sent to dbtrail — lets the SaaS side register the source server and resolve a stable `bintrail_id`, closing the identity-model gap between hosted and BYOS modes (#195)

### Changed
- BYOS agent fails loud at startup if source identity capture (`@@server_uuid` query or `--source-dsn` parse) fails, instead of silently emitting metadata with an empty `server_uuid` for the process lifetime (#195)

## [0.4.3] - 2026-04-06

### Added
- `bintrail stream --gap-timeout` flag (default 30s) configures the timeout for gap-detection queries (`SHOW BINARY LOGS`, `@@gtid_purged`, `@@gtid_executed`); raise this on managed MySQL instances with many binlog files where the default 10s was too tight (#190)
- `bintrail agent --max-reconnect-attempts` flag (default 10) bounds the WebSocket reconnect loop so the agent exits non-zero after consecutive failures, letting a process supervisor (e.g. systemd `Restart=on-failure`) respawn it (#191)

### Changed
- `bintrail stream` gap-detection query timeout default raised from 10s to 30s — the query only runs once per resume so a higher ceiling has no ongoing cost (#190)

### Fixed
- `bintrail agent` no longer stays "active" in systemd when its WebSocket connection dies but cannot reconnect — the new retry budget surfaces the failure as a process exit so systemd can respawn the agent and the dashboard sees a fresh, healthy connection (#191)

## [0.4.2] - 2026-04-05

### Fixed
- Release binaries now target glibc 2.17 (via Zig linker) — fixes `GLIBC_2.38 not found` on Amazon Linux 2023, RHEL 9, and other distros with glibc < 2.39

## [0.4.1] - 2026-04-03

### Fixed
- BYOS MetadataClient now sends `bintrail_id` (stable UUID) instead of numeric `@@server_id` in metadata records and WebSocket heartbeats — prevents misidentification when multiple MySQL servers share the default `server_id = 1`
- BYOS+S3 mode now requires `--index-dsn` to ensure stable `bintrail_id` resolution for S3 partitioning — prevents orphaned partitions that cannot be correlated with future runs

## [0.4.0] - 2026-04-01

### Added
- `bintrail agent` command — opens an outbound WebSocket to dbtrail for remote query, recovery, and forensics commands; no inbound ports required
- BYOS (Bring Your Own Storage) mode — parsed binlog events are split into metadata (sent to dbtrail API, zero row data) and payload (written as Parquet to customer S3, never leaves customer infrastructure)
- Abstract storage backend interface (`internal/storage`) with S3 implementation for BYOS payload writes
- In-memory event buffer for BYOS mode — keeps recent events in memory for fast local access while S3 remains authoritative
- BYOS flush pipeline with configurable interval (`--flush-interval`, default 5s) and retry with exponential backoff; flush health reported in agent heartbeat
- Agent pre-flight validation (`--validate` flag) — checks MySQL connectivity, replication privileges, S3 access, schema snapshot, dbtrail API auth, and WebSocket channel in one pass
- `connection_id` column in `binlog_events` — captures MySQL `pseudo_thread_id` from binlog `QueryEvent.SlaveProxyID`; included in all query output formats, archives, buffer, and BYOS metadata
- Auto-migration via `indexer.EnsureSchema()` for `connection_id` column on existing installations (instant DDL in MySQL 8.0+)
- systemd service unit (`deploy/bintrail-agent.service`) for bare metal agent installs

### Changed
- Module path moved from `github.com/nethalo/bintrail` to `github.com/dbtrail/bintrail`
- License changed from Apache 2.0 to Business Source License 1.1

## [0.3.2] - 2026-03-15

### Added
- Binlog gap detection on `bintrail stream` restart — position mode checks `SHOW BINARY LOGS`, GTID mode compares checkpoint against `@@gtid_purged` and `@@gtid_executed`
- Automatic gap filling for fillable gaps; unfillable gaps auto-advance to the earliest available position with a warning logged and checkpoint updated to prevent crash loops
- `--no-gap-fill` flag for `bintrail stream` to refuse starting when a gap is detected

## [0.3.1] - 2026-03-13

### Added
- Capture foreign key constraints in schema snapshots — `bintrail snapshot` now queries `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` joined with `REFERENTIAL_CONSTRAINTS` and stores FK relationships in a new `fk_constraints` table, using the same `snapshot_id` as `schema_snapshots`
- New `fk_constraints` table created by `bintrail init` — no additional MySQL grants required
- Graceful upgrade path: existing installations that upgrade without re-running `bintrail init` get a warning instead of a failed snapshot

## [0.3.0] - 2026-03-12

### Added
- `list_schema_changes` MCP tool — queries the `schema_changes` table with filters (`schema`, `table`, `ddl_type`, `since`, `until`, `limit`), making DDL audit data queryable via MCP
- TRUNCATE DDL support — `parseDDL()` now detects `TRUNCATE [TABLE]` statements and records them in `schema_changes`
- Composite index `idx_schema_table` on `schema_changes` for efficient per-table lookups

### Changed
- Rotate now archives and drops one partition at a time instead of archiving all partitions first and then dropping in a single bulk `ALTER TABLE` — reduces disk space pressure during large rotations and ensures each partition is freed immediately after archiving

## [0.2.16] - 2026-03-10

### Added
- Truncation warning when query or recover results hit the limit — CLI prints to stderr, MCP tools append to the response text so the LLM knows to narrow the time range or increase the limit

## [0.2.15] - 2026-03-09

### Changed
- Bump DuckDB threads from 1 to 2 — archive Parquet files have 3-4 row groups (500K rows each) and DuckDB can only parallelize across row groups; with 6GB container memory there is enough headroom for two threads (125MB each)

## [0.2.14] - 2026-03-09

### Changed
- Raise DuckDB memory limit from 1GB to 4GB — 190MB compressed Parquet files decompress to well over 1GB; 2GB containers still OOM-killed during scans

### Fixed
- Apply ORDER BY + LIMIT per archive file instead of scanning all rows — DuckDB's top-N optimization keeps only the LIMIT rows in memory during sort, then MergeResults merge-sorts the per-file results for the correct global top-K. Previously a single hour with 1.75M events was fully materialized in Go before applying the limit, causing both excessive memory usage and 2+ minute query times

## [0.2.13] - 2026-03-09

### Changed
- Raise DuckDB memory limit from 1GB to 4GB — 190MB compressed Parquet files decompress to well over 1GB; 2GB containers still OOM-killed during scans

## [0.2.12] - 2026-03-09

### Changed
- Raise DuckDB memory limit from 256MB to 1GB for 2GB container environments — 190MB compressed Parquet files need more than 256MB to decompress and scan; the previous limit caused OOM kills even with single-file sequential processing

## [0.2.11] - 2026-03-09

### Fixed
- Remove ORDER BY from per-file S3 archive queries — forces DuckDB to buffer the entire result set for sorting, spiking memory. Sorting now happens in Go via `MergeResults` after all files are collected, letting DuckDB stream rows with minimal memory

## [0.2.10] - 2026-03-09

### Changed
- Download S3 Parquet files to local temp via AWS SDK before querying with DuckDB — eliminates httpfs extension which held entire S3 files in memory (outside `memory_limit` tracking), causing OOM kills even with conservative limits. Local reads use OS page cache (mmap), keeping memory usage predictable and low

## [0.2.9] - 2026-03-09

### Fixed
- Query S3 Parquet files one at a time instead of all at once — each file can require hundreds of MB when decompressed; sequential processing lets DuckDB release memory between files, fitting comfortably in container memory limits
- Reduce DuckDB to 1 thread (125MB baseline per thread per DuckDB docs) to maximize memory available for data

## [0.2.8] - 2026-03-09

### Fixed
- Pre-filter S3 archive files by Hive partition time range (`event_date`/`event_hour`) before passing to DuckDB — a 1-hour query now reads 1-2 files instead of all 10, dramatically reducing memory usage and S3 transfer
- Bump DuckDB `memory_limit` to 512MB (was 256MB) as a safety net for larger archive scans

## [0.2.7] - 2026-03-09

### Fixed
- Tune DuckDB for container environments — limit to 2 threads and disable `preserve_insertion_order` to reduce peak memory when querying S3 Parquet archives; prevents OOM kills (`exit=137`) and DuckDB out-of-memory errors on memory-constrained containers

## [0.2.6] - 2026-03-09

### Fixed
- Cap DuckDB memory at 256MB to prevent OOM kills in memory-constrained containers — DuckDB defaults to 80% of system RAM, which exhausts memory when reading S3 Parquet files; with the limit it spills to disk instead

## [0.2.5] - 2026-03-09

### Fixed
- Auto-detect S3 bucket region via `GetBucketLocation` — when `AWS_DEFAULT_REGION` differs from the bucket's actual region, `ListObjectsV2` and DuckDB `parquet_scan` both failed with 301 PermanentRedirect
- Set DuckDB `temp_directory` to OS temp dir — DuckDB creates a `.tmp` scratch directory in the CWD, which fails in containers where the working directory is read-only

## [0.2.4] - 2026-03-09

### Fixed
- Bypass DuckDB S3 glob expansion entirely — use AWS SDK `ListObjectsV2` to enumerate `.parquet` files, then pass explicit paths to `parquet_scan()`. DuckDB's glob fails on S3 paths containing `=` signs (Hive partition keys like `event_date=2026-03-09/`), silently returning zero results even with valid credentials and correct single-level globs

## [0.2.3] - 2026-03-09

### Fixed
- Load DuckDB `aws` extension for S3 credential resolution — without it, DuckDB attempts anonymous S3 access which silently returns zero results instead of using `AWS_ACCESS_KEY_ID` / `AWS_SESSION_TOKEN` from the environment

## [0.2.2] - 2026-03-09

### Fixed
- Use explicit single-level S3 globs (`/*/*/*.parquet`) instead of unsupported `**` recursive glob — DuckDB's httpfs extension does not support recursive globs on S3, causing "No files found" errors on archive queries

## [0.2.1] - 2026-03-09

### Fixed
- Enable `hive_partitioning` in `parquet_scan` for S3 archive queries — DuckDB's glob resolution failed on S3 paths containing `=` signs (Hive-partitioned directories like `event_date=2026-03-09/`)

## [0.2.0] - 2026-03-06

### Added
- MCP gateway with OAuth 2.1 for Claude Connector support
- Tenant provisioning admin API and backend health monitoring for MCP gateway
- Rate limiting and request logging for MCP gateway
- Auto-snapshot on DDL detection with restore coverage tracking
- `--reset` flag for `bintrail stream` to force new start position
- Seamless mode switching between position and GTID in `bintrail stream`
- Idempotent stream startup by preferring saved checkpoint over flags
- S3 upload retry with `--retry` flag for `baseline` and `rotate` commands
- Standalone `bintrail upload` command for S3 uploads
- At-rest encryption for mydumper dump files (`dump --encrypt`)
- Docker support: Dockerfile, docker-compose template, and mydumper Docker fallback
- `event_hour` Hive partition level in archive path
- `archive_state` table to track archived Parquet files
- Archive and S3 stats in `bintrail status` output
- `--format json` for all commands
- `--sync-thread-lock-mode` and `--trx-tables` flags for mydumper
- Stream state and `bintrail-id` in status output
- `/health` endpoint for `bintrail-mcp` HTTP server
- Debug logging for status command

### Fixed
- Always emit `TO_SECONDS` partition pruning hints for `since`/`until` queries
- Include `archive_state` data in status restore coverage
- Add missing Archives section to MCP status tool
- Emit GTID tracking events to prevent gaps in accumulated GTID set
- Place `--outputdir` last in mydumper args for Docker wrapper compatibility
- Skip shell script wrappers when resolving mydumper on PATH
- Remove `Truncate(time.Hour)` from rotate cutoff so hourly partitions drop correctly
- Parse mydumper 0.16+ metadata format
- Replace `UTC_TIMESTAMP()` with `CURRENT_TIMESTAMP` in `archive_state` DDL
- Show S3 upload progress in rotate output
- Create partitions from current hour forward
- Suppress usage output on command errors
- Normalize shortened UUIDs in GTID sets from RDS
- Allow gateway's own issuer origin in origin middleware

## [0.1.1] - 2026-03-01

### Fixed
- Nil dereference in `parser.New()` when resolver is nil (crash risk)
- MCP HTTP server now shuts down gracefully on SIGINT/SIGTERM
- `proxy.py` SSE stream parsing crash on non-UTF-8 bytes
- Unchecked `os.MkdirAll` error in E2E test

### Changed
- Unknown compression codecs now return an error instead of silently falling back to no compression; new `ValidateCodec()` function validates early in CLI layers
- `config.Connect()` injects a 10-second default TCP connect timeout when the DSN does not specify one
- `proxy.py` is now fully self-contained (inlined `log.py`); `log.py` removed
- Partial file cleanup errors in baseline are now logged

## [0.1.0] - 2026-03-01

### Added
- MySQL ROW-format binlog parser and indexer (`bintrail index`)
- Live replication indexing (`bintrail stream`) with Prometheus metrics
- Query engine with table/json/csv output (`bintrail query`)
- Reversal SQL generator (`bintrail recover`)
- Schema snapshot management (`bintrail snapshot`)
- Hourly partition rotation with retention policy (`bintrail rotate`)
- Partition archiving to Parquet (`rotate --archive-dir`, `--archive-s3`)
- DuckDB-backed Parquet archive querying (`query --archive-dir`, `--archive-s3`)
- mydumper integration (`bintrail dump`) and Parquet baseline converter (`bintrail baseline`)
- MCP server with stdio and HTTP transport modes (`bintrail-mcp`)
- RBAC foundation with table flags, profiles, and access rules (`bintrail flag`, `bintrail profile`, `bintrail access`)
- TLS/SSL support for stream mode
- Server identity system (`--bintrail-id`)
- GoReleaser-based release process with version injection
