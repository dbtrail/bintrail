# ── Stage 1: build ─────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Cache dependency downloads separately from source
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bintrail ./cmd/bintrail

# ── Stage 2: runtime ───────────────────────────────────────
FROM alpine:3.20

# ca-certificates for TLS; mysql-client for health probe in entrypoint
RUN apk add --no-cache ca-certificates mysql-client bash

COPY --from=builder /bintrail /usr/local/bin/bintrail
COPY demo/bintrail/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
