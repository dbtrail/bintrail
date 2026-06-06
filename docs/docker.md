# Docker Deployment

Bintrail ships a multi-stage Dockerfile that produces a minimal image containing both `bintrail` and `bintrail-mcp` binaries.

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
docker pull ghcr.io/dbtrail/bintrail:latest        # latest release
docker pull ghcr.io/dbtrail/bintrail:v0.7.12       # a specific version
```

The images and the release checksums are signed with [cosign](https://github.com/sigstore/cosign) (keyless, via GitHub OIDC). Verify an image before running it:

```bash
cosign verify ghcr.io/dbtrail/bintrail:latest \
  --certificate-identity-regexp "https://github.com/dbtrail/bintrail/.*" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

An SBOM is attached to each release archive on the GitHub Releases page.

> **Just evaluating?** `ghcr.io/dbtrail/bintrail-appliance` is a zero-setup,
> single-container demo (MySQL + bintrail + ProxySQL + traffic generator,
> evaluation-only) — see [appliance.md](appliance.md). It is a separate GHCR
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
an index MySQL (persisted in a named volume) plus `bintrail up --console` —
preflight checks, index tables, automatic schema snapshot, the live binlog
stream, **and the web console**, in one `up -d`. It pulls the published
`ghcr.io/dbtrail/bintrail` image; no Go toolchain or local build needed.

### Quick start

No clone needed — the compose file is self-contained:

```bash
curl -fsSLO https://raw.githubusercontent.com/dbtrail/bintrail/main/docker-compose.yml
echo 'SOURCE_DSN=USER:PASSWORD@tcp(YOUR_MYSQL_HOST:3306)/' > .env
docker compose up -d
docker compose logs -f bintrail
```

(From a source checkout, `cp .env.example .env` gives you the annotated
template with every optional knob instead.)

The logs print the console URL, access token included:

```
Console (read-only) is running. Open:

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
- `bintrail up` is idempotent: restarts resume the stream from its saved
  checkpoint. The preflight (`doctor`) failing prints copy-pasteable
  remediation in the logs and the container retries.
- Saved console connections (the Servers menu) persist in the
  `bintrail-state` volume.
- `BINTRAIL_TAG` in `.env` pins the image version (default `latest`);
  building from a source checkout instead is a comment-toggle in the
  compose file (`build: .`).

### Connecting to an external index MySQL

If you already have a MySQL instance for the index, set `INDEX_DSN` in
`.env` to point at it and remove the `index-mysql` service from the compose
file.

## Environment variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `SOURCE_DSN` | compose (required) | DSN for the source MySQL database to watch |
| `INDEX_DSN` | compose (optional) | Bring-your-own index MySQL (default: the bundled container) |
| `SCHEMAS` | compose (optional) | Comma-separated schemas to track (empty = all user schemas) |
| `CONSOLE_TOKEN` | compose (optional) | Pin the console access token (default: generated per boot) |
| `INDEX_MYSQL_ROOT_PASSWORD` | compose (optional) | Root password for the bundled index MySQL |
| `BINTRAIL_TAG` | compose (optional) | Image tag to run (default `latest`) |
| `BINTRAIL_INDEX_DSN` | bintrail-mcp | Index DSN for the MCP server |

(`SERVER_ID` is no longer needed — `bintrail up` derives a stable one from the
source DSN.)

## Image details

- **Base**: `debian:bookworm-slim` (glibc required by DuckDB)
- **Binaries**: `/usr/local/bin/bintrail`, `/usr/local/bin/bintrail-mcp`
- **Entrypoint**: `bintrail` (pass subcommands as arguments)
- **No shell scripts or init systems** — the container runs a single binary

### Why not Alpine?

Bintrail depends on DuckDB (`duckdb-go`) for querying Parquet archives. DuckDB's Go bindings include pre-compiled C libraries linked against glibc. Alpine uses musl libc, which is binary-incompatible and would cause runtime failures.

## Full demo

For a complete demo stack with traffic generation, Prometheus, and Grafana dashboards, see `demo/compose.yml` and `demo/README.md`.
