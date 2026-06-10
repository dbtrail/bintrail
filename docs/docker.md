# Docker Deployment

dbtrail ships a multi-stage Dockerfile that produces a minimal image containing both `bintrail` and `bintrail-mcp` binaries.

## Building the image

```bash
docker build -t bintrail .
```

Inject version metadata at build time:

```bash
docker build \
  --build-arg VERSION=$(git describe --tags --always) \
  --build-arg COMMIT=$(git rev-parse --short HEAD) \
  --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
  -t bintrail .
```

### Multi-architecture builds

Build for both `amd64` and `arm64` using Docker Buildx:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t your-registry/bintrail:latest \
  --push .
```

> **Note:** CGO cross-compilation for `arm64` requires the `aarch64-linux-gnu-gcc` toolchain. Docker Buildx handles this automatically with QEMU emulation, though native compilation is faster.

## Pre-built images (GHCR)

Each tagged release publishes a multi-arch image (`linux/amd64` + `linux/arm64`) to GitHub Container Registry, so you don't need a Go toolchain or a local build:

```bash
docker pull ghcr.io/dbtrail/bintrail:latest          # core CLI + MCP server
docker pull ghcr.io/dbtrail/bintrail-console:latest  # web console (serve/watch)
docker pull ghcr.io/dbtrail/bintrail:v0.7.12         # a specific version
```

`bintrail-console` is its own image (and GHCR package — it needs the same
one-time public-visibility flip described below): a single binary with
entrypoint `bintrail-console`, used by the Compose quickstart to run `watch`.
The cosign verification below applies to it equally
(`cosign verify ghcr.io/dbtrail/bintrail-console:latest …`).

The images and the release checksums are signed with [cosign](https://github.com/sigstore/cosign) (keyless, via GitHub OIDC). Verify an image before running it:

```bash
cosign verify ghcr.io/dbtrail/bintrail:latest \
  --certificate-identity-regexp "https://github.com/dbtrail/dbtrail/.*" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

An SBOM is attached to each release archive on the GitHub Releases page.

> **Just evaluating?** `ghcr.io/dbtrail/bintrail-demo` is a zero-setup,
> single-container demo (MySQL + dbtrail + ProxySQL + traffic generator,
> evaluation-only) — see [demo.md](demo.md). It is a separate GHCR
> package and needs the same one-time public-visibility flip described below.

> **Maintainer note (one-time):** the first release creates the GHCR package as **private**. Anonymous `docker pull` only works once the package is made public: in the org's *Packages → bintrail → Package settings*, set visibility to **Public** and link it to this repository. Until then the `pull` commands above require `docker login ghcr.io`.

## Running with docker run

### One-off commands

```bash
# Initialize the index database
docker run --rm bintrail init \
  --index-dsn "root:password@tcp(mysql-host:3306)/bintrail_index"

# Take a schema snapshot
docker run --rm bintrail snapshot \
  --source-dsn "bintrail:password@tcp(source-host:3306)/" \
  --index-dsn  "root:password@tcp(mysql-host:3306)/bintrail_index" \
  --schemas    "myapp"

# Query indexed events
docker run --rm bintrail query \
  --index-dsn "root:password@tcp(mysql-host:3306)/bintrail_index" \
  --schema myapp --table users --limit 10
```

### Long-running stream

```bash
docker run -d \
  --name bintrail-stream \
  --restart always \
  bintrail stream \
    --index-dsn  "root:password@tcp(mysql-host:3306)/bintrail_index" \
    --source-dsn "bintrail:password@tcp(source-host:3306)/" \
    --server-id  1234 \
    --schemas    "myapp" \
    --metrics-addr ":9090" \
    --log-format json
```

### Running the MCP server

The image includes `bintrail-mcp` for use with Claude Code or Claude Desktop:

```bash
docker run -d \
  --name bintrail-mcp \
  -p 8080:8080 \
  -e BINTRAIL_INDEX_DSN="root:password@tcp(mysql-host:3306)/bintrail_index" \
  --entrypoint bintrail-mcp \
  bintrail --http :8080
```

## Docker Compose

The `docker-compose.yml` at the repository root is the zero-friction setup:
an index MySQL (persisted in a named volume) plus `bintrail-console watch` —
preflight checks, index tables, automatic schema snapshot, the live binlog
stream, **and the web console**, in one `up -d`. It pulls the published
`ghcr.io/dbtrail/bintrail-console` image; no Go toolchain or local build
needed.

### Quick start

No clone, no config — the compose file is self-contained and the servers
to watch are added from the console UI afterwards:

```bash
curl -fsSLO https://raw.githubusercontent.com/dbtrail/dbtrail/main/docker-compose.yml
docker compose up -d
docker compose logs -f bintrail
```

Optional knobs go in a `.env` next to the file: `SOURCE_DSN` to start
streaming one source immediately at boot, `CONSOLE_TOKEN` to pin the access
token across restarts, `INDEX_DSN` to bring your own index MySQL. (From a
source checkout, `cp .env.example .env` gives you the annotated template.)

The logs print the console URL, access token included:

```
Console is running — open it and add the MySQL servers to watch:

    http://127.0.0.1:8090/?token=ab12cd34…
```

Notes:

- `SOURCE_DSN` is the MySQL you want to watch. The user needs
  `REPLICATION SLAVE`, `REPLICATION CLIENT`, and `SELECT`. A MySQL on the
  same machine is reachable from inside Docker as `host.docker.internal`.
- The console is published on the **host loopback only**
  (`127.0.0.1:8090`). To reach it from another machine, change the port
  mapping to `"8090:8090"` and pin a stable `CONSOLE_TOKEN` in `.env`
  (without a pinned token a fresh one is generated per boot and printed in
  the logs).
- `bintrail-console watch` is idempotent: restarts resume the stream from
  its saved checkpoint. The preflight (`doctor`) failing prints
  copy-pasteable remediation in the logs and the container retries.
- Saved console connections (the Servers menu) persist in the
  `bintrail-state` volume.
- `BINTRAIL_TAG` in `.env` pins the image version (default `latest`);
  building from a source checkout instead is a comment-toggle in the
  compose file (`build:` with `dockerfile: Dockerfile.bintrail-console`).

### The bundled index MySQL 8.4

The bundled index is **MySQL 8.4 LTS**, pinned to an exact minor tag. The
container holds the binary; the data lives in a separate `bintrail-index-data`
volume — bumping a minor version is "swap the container, keep the volume"
(the PMM pattern). dbtrail **ships** this MySQL but does not **operate** it:
disk, backups, and upgrades are yours (for a managed, operated index, see
[dbtrail.com](https://dbtrail.com)). Support boundary: [SUPPORT.md](../SUPPORT.md).

**Credentials** — no static default password. On the first `up`, the
one-shot `index-init` service generates a random password into the
`bintrail-index-secret` volume (or takes one from `INDEX_MYSQL_ROOT_PASSWORD`
in `.env` if you set it *before* the first boot). Both the index MySQL and
dbtrail read it from there. The password is baked into the datadir at init,
so `bintrail-index-data` and `bintrail-index-secret` are a pair: back them up
together, and changing the password later means resetting both volumes.

**Troubleshooting** — if `docker compose up` never prints the console URL,
the index MySQL likely isn't healthy yet (the `bintrail` service waits for it
via `depends_on`, so its own log stays empty until then). Check the index
directly: `docker compose logs index-init index-mysql`. A "password" or
"healthcheck" error there usually means the `bintrail-index-secret` volume was
reset out of sync with `bintrail-index-data` — reset both together.

**Upgrading from a pre-8.4 bundled index** — the old eval index used a
`mysql:8.0` container on the `index-mysql-data` volume. The new compose uses a
**new** `bintrail-index-data` volume on 8.4 and leaves the old one untouched,
so by default dbtrail simply re-indexes into the fresh 8.4 volume from the
source's binlogs (the bundled index was always "volume loss = re-index").

> ⚠️ **The 8.4 datadir is non-downgradable.** A MySQL 8.4 server started on an
> 8.0 datadir runs an in-place upgrade automatically and **irreversibly** —
> you cannot go back to 8.0 afterward. That is exactly why the new volume name
> is used instead of reusing `index-mysql-data`: your old 8.0 data stays
> recoverable. To carry the old data forward deliberately, `mysqldump` from the
> old `index-mysql-data` volume and reload into the new one (a logical
> dump/restore, not an in-place datadir upgrade), or point `INDEX_DSN` at a BYO
> index instead.

**Upgrading a compose stack from before the console split** — older
`docker-compose.yml` files ran `bintrail up --console` from the
`ghcr.io/dbtrail/bintrail` image. That flag no longer exists (the combined
daemon is now `bintrail-console watch`, in its own image), so a
`docker compose pull && up -d` on the OLD file crash-loops with
`unknown flag: --console`. The fix is to re-download `docker-compose.yml`
(the curl in the Quick start) — image and command changed, but your `.env`
and all data volumes (`bintrail-index-data`, `bintrail-index-secret`,
`bintrail-state`, including saved console servers) carry over unchanged.

### Connecting to an external index MySQL (bring your own)

To run your own index instead of the bundled 8.4, set `INDEX_DSN` in `.env` to a MySQL 8.0+ you operate and
remove the bundled index from the compose file: delete the `index-init` and
`index-mysql` services, the `bintrail-index-data` / `bintrail-index-secret`
volumes, the `bintrail-index-secret` mount and the `depends_on: index-mysql`
on the `bintrail` service. dbtrail installs only its schema on your server;
its sizing, backups, and upgrades are yours — see
[Capacity Planning](./capacity.md), [deployment.md](./deployment.md), and
[SUPPORT.md](../SUPPORT.md). (The BYO contract floor stays MySQL **8.0+** —
only the *bundled* index is 8.4.)

## Environment variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `SOURCE_DSN` | compose (optional) | DSN for a source MySQL to start watching at boot (empty = add servers from the console UI) |
| `INDEX_DSN` | compose (optional) | Bring-your-own index MySQL (default: the bundled container) |
| `SCHEMAS` | compose (optional) | Comma-separated schemas to track (empty = all user schemas) |
| `CONSOLE_TOKEN` | compose (optional) | Pin the console access token (default: generated per boot) |
| `INDEX_MYSQL_ROOT_PASSWORD` | compose (optional) | Pin the bundled index root password (set *before* first boot; default: randomly generated into the `bintrail-index-secret` volume) |
| `BINTRAIL_TAG` | compose (optional) | Image tag to run (default `latest`) |
| `BINTRAIL_INDEX_DSN` | bintrail-mcp | Index DSN for the MCP server |

(`SERVER_ID` is no longer needed — `bintrail-console watch` derives a stable
one from the source DSN.)

## Image details

- **Base**: `debian:bookworm-slim` (glibc required by DuckDB)
- **Binaries**: `/usr/local/bin/bintrail`, `/usr/local/bin/bintrail-mcp`
- **Entrypoint**: `bintrail` (pass subcommands as arguments)
- **No shell scripts or init systems** — the container runs a single binary

The `ghcr.io/dbtrail/bintrail-console` image follows the same contract with
one binary: entrypoint `bintrail-console`, uid 999 pinned (the compose secret
volume is chowned to it), `/var/lib/bintrail` pre-created for the server
registry. Build it from source with
`docker build -f Dockerfile.bintrail-console -t bintrail-console .`

### Why not Alpine?

dbtrail depends on DuckDB (`duckdb-go`) for querying Parquet archives. DuckDB's Go bindings include pre-compiled C libraries linked against glibc. Alpine uses musl libc, which is binary-incompatible and would cause runtime failures.

## Full demo

For a complete demo stack with traffic generation, Prometheus, and Grafana dashboards, see `demo/compose.yml` and `demo/README.md`.
