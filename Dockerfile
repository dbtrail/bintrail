# ── Stage 1: build ─────────────────────────────────────────
# Debian-based image required — DuckDB's pre-compiled static libs need glibc.
FROM golang:1.24.7-bookworm AS builder

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /src

# Cache dependency downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 go build \
    -ldflags="-s -w -X main.Version=${VERSION} -X main.CommitSHA=${COMMIT} -X main.BuildDate=${BUILD_DATE}" \
    -o /bintrail ./cmd/bintrail

RUN CGO_ENABLED=1 go build \
    -ldflags="-s -w -X main.mcpVersion=${VERSION}" \
    -o /bintrail-mcp ./cmd/bintrail-mcp

# ── Stage 2: runtime ───────────────────────────────────────
# Debian slim for glibc compatibility with DuckDB.
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --system --no-create-home bintrail

COPY --from=builder /bintrail /usr/local/bin/bintrail
COPY --from=builder /bintrail-mcp /usr/local/bin/bintrail-mcp

USER bintrail
ENTRYPOINT ["bintrail"]
