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
  -e INDEX_DSN="root:password@tcp(mysql-host:3306)/bintrail_index" \
  bintrail stream \
    --index-dsn  "$INDEX_DSN" \
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

A production-oriented `docker-compose.yml` is provided at the repository root. It starts an index MySQL instance and runs `bintrail stream`.

### Quick start

1. Create a `.env` file:

```bash
INDEX_MYSQL_ROOT_PASSWORD=your-secure-password
SOURCE_DSN=bintrail:password@tcp(your-source-mysql:3306)/
SCHEMAS=myapp
SERVER_ID=1234
```

2. Start the stack:

```bash
docker compose up -d
```

3. Check status:

```bash
docker compose logs -f bintrail
```

### Using a pre-built image

To use a pre-built image instead of building locally, replace the `build: .` line in `docker-compose.yml`:

```yaml
services:
  bintrail:
    image: your-registry/bintrail:v1.2.3
    # build: .  # comment out or remove
```

### Connecting to an external index MySQL

If you already have a MySQL instance for the index, remove the `index-mysql` service from compose and update `INDEX_DSN` in your `.env` to point to your existing instance.

## Environment variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `INDEX_DSN` | all commands | DSN for the index MySQL database |
| `SOURCE_DSN` | snapshot, stream | DSN for the source MySQL database |
| `SCHEMAS` | snapshot, stream | Comma-separated schemas to track |
| `SERVER_ID` | stream | Unique replication server ID |
| `BINTRAIL_INDEX_DSN` | bintrail-mcp | Index DSN for the MCP server |

## Image details

- **Base**: `debian:bookworm-slim` (glibc required by DuckDB)
- **Binaries**: `/usr/local/bin/bintrail`, `/usr/local/bin/bintrail-mcp`
- **Entrypoint**: `bintrail` (pass subcommands as arguments)
- **No shell scripts or init systems** — the container runs a single binary

### Why not Alpine?

Bintrail depends on DuckDB (`duckdb-go`) for querying Parquet archives. DuckDB's Go bindings include pre-compiled C libraries linked against glibc. Alpine uses musl libc, which is binary-incompatible and would cause runtime failures.

## Full demo

For a complete demo stack with traffic generation, Prometheus, and Grafana dashboards, see `demo/compose.yml` and `demo/README.md`.
