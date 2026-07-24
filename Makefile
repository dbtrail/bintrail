BINARY_NAME=bintrail
MCP_BINARY=bintrail-mcp
GATEWAY_BINARY=mcp-gateway
CONSOLE_BINARY=bintrail-console
PG_BINARY=bintrail-pg
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
BUILD_DATE=$(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

BINTRAIL_LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.CommitSHA=$(COMMIT) -X main.BuildDate=$(BUILD_DATE)"
MCP_LDFLAGS=-ldflags "-X main.mcpVersion=$(VERSION)"
GATEWAY_LDFLAGS=-ldflags "-X main.gatewayVersion=$(VERSION)"
# bintrail-console and bintrail-pg both reuse BINTRAIL_LDFLAGS: they inject the
# same main.Version/CommitSHA/BuildDate vars as the core binary.

.PHONY: all build build-mcp build-gateway build-console build-pg clean test console-e2e lint install build-all tidy deps notices check-notices mcpb validate-mcpb

all: build build-mcp build-gateway build-console build-pg

build:
	go build $(BINTRAIL_LDFLAGS) -o $(BINARY_NAME) ./cmd/bintrail

build-mcp:
	go build $(MCP_LDFLAGS) -o $(MCP_BINARY) ./cmd/bintrail-mcp

build-gateway:
	go build $(GATEWAY_LDFLAGS) -o $(GATEWAY_BINARY) ./cmd/mcp-gateway

# bintrail-console links DuckDB (console → query → parquetquery), so it
# requires CGO_ENABLED=1 like the core bintrail binary.
build-console:
	go build $(BINTRAIL_LDFLAGS) -o $(CONSOLE_BINARY) ./cmd/bintrail-console

# bintrail-pg links jackc/pgx + pglogrepl (PostgreSQL capture) plus DuckDB via
# the shared read plane (query/reconstruct → parquetquery), so it requires
# CGO_ENABLED=1 like the core bintrail and bintrail-console binaries.
build-pg:
	go build $(BINTRAIL_LDFLAGS) -o $(PG_BINARY) ./cmd/bintrail-pg

install:
	go install $(BINTRAIL_LDFLAGS) ./cmd/bintrail
	go install $(MCP_LDFLAGS) ./cmd/bintrail-mcp
	go install $(GATEWAY_LDFLAGS) ./cmd/mcp-gateway
	go install $(BINTRAIL_LDFLAGS) ./cmd/bintrail-console
	go install $(BINTRAIL_LDFLAGS) ./cmd/bintrail-pg

clean:
	rm -f $(BINARY_NAME) $(MCP_BINARY) $(GATEWAY_BINARY) $(CONSOLE_BINARY) $(PG_BINARY)
	go clean

test:
	go test ./... -count=1

# Headless-Chrome regression guard for the console SPA (assets the Go suite
# never renders). Needs Docker (bintrail-test-mysql) + Node. PW_CHANNEL=chrome
# uses system Chrome instead of the playwright-managed chromium.
console-e2e:
	bash test/console-e2e/run.sh

lint:
	golangci-lint run ./...

# Cross-compilation — requires CGO_ENABLED=1 (DuckDB uses pre-compiled C libraries).
# linux/arm64: requires aarch64-linux-gnu-gcc (apt install gcc-aarch64-linux-gnu)
# darwin targets: must be built on macOS (native toolchain handles both amd64/arm64)
build-all:
	GOOS=linux   GOARCH=amd64 CGO_ENABLED=1 go build $(BINTRAIL_LDFLAGS) -o dist/$(BINARY_NAME)-linux-amd64 ./cmd/bintrail
	GOOS=linux   GOARCH=arm64 CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build $(BINTRAIL_LDFLAGS) -o dist/$(BINARY_NAME)-linux-arm64 ./cmd/bintrail
	GOOS=darwin  GOARCH=amd64 CGO_ENABLED=1 go build $(BINTRAIL_LDFLAGS) -o dist/$(BINARY_NAME)-darwin-amd64 ./cmd/bintrail
	GOOS=darwin  GOARCH=arm64 CGO_ENABLED=1 go build $(BINTRAIL_LDFLAGS) -o dist/$(BINARY_NAME)-darwin-arm64 ./cmd/bintrail
	GOOS=linux   GOARCH=amd64 CGO_ENABLED=1 go build $(MCP_LDFLAGS) -o dist/$(MCP_BINARY)-linux-amd64 ./cmd/bintrail-mcp
	GOOS=linux   GOARCH=arm64 CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build $(MCP_LDFLAGS) -o dist/$(MCP_BINARY)-linux-arm64 ./cmd/bintrail-mcp
	GOOS=darwin  GOARCH=amd64 CGO_ENABLED=1 go build $(MCP_LDFLAGS) -o dist/$(MCP_BINARY)-darwin-amd64 ./cmd/bintrail-mcp
	GOOS=darwin  GOARCH=arm64 CGO_ENABLED=1 go build $(MCP_LDFLAGS) -o dist/$(MCP_BINARY)-darwin-arm64 ./cmd/bintrail-mcp
	GOOS=linux   GOARCH=amd64 CGO_ENABLED=1 go build $(BINTRAIL_LDFLAGS) -o dist/$(CONSOLE_BINARY)-linux-amd64 ./cmd/bintrail-console
	GOOS=linux   GOARCH=arm64 CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build $(BINTRAIL_LDFLAGS) -o dist/$(CONSOLE_BINARY)-linux-arm64 ./cmd/bintrail-console
	GOOS=darwin  GOARCH=amd64 CGO_ENABLED=1 go build $(BINTRAIL_LDFLAGS) -o dist/$(CONSOLE_BINARY)-darwin-amd64 ./cmd/bintrail-console
	GOOS=darwin  GOARCH=arm64 CGO_ENABLED=1 go build $(BINTRAIL_LDFLAGS) -o dist/$(CONSOLE_BINARY)-darwin-arm64 ./cmd/bintrail-console
	GOOS=linux   GOARCH=amd64 CGO_ENABLED=1 go build $(BINTRAIL_LDFLAGS) -o dist/$(PG_BINARY)-linux-amd64 ./cmd/bintrail-pg
	GOOS=linux   GOARCH=arm64 CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build $(BINTRAIL_LDFLAGS) -o dist/$(PG_BINARY)-linux-arm64 ./cmd/bintrail-pg
	GOOS=darwin  GOARCH=amd64 CGO_ENABLED=1 go build $(BINTRAIL_LDFLAGS) -o dist/$(PG_BINARY)-darwin-amd64 ./cmd/bintrail-pg
	GOOS=darwin  GOARCH=arm64 CGO_ENABLED=1 go build $(BINTRAIL_LDFLAGS) -o dist/$(PG_BINARY)-darwin-arm64 ./cmd/bintrail-pg
	GOOS=linux   GOARCH=amd64 CGO_ENABLED=0 go build $(GATEWAY_LDFLAGS) -o dist/$(GATEWAY_BINARY)-linux-amd64 ./cmd/mcp-gateway
	GOOS=linux   GOARCH=arm64 CGO_ENABLED=0 go build $(GATEWAY_LDFLAGS) -o dist/$(GATEWAY_BINARY)-linux-arm64 ./cmd/mcp-gateway
	GOOS=darwin  GOARCH=amd64 CGO_ENABLED=0 go build $(GATEWAY_LDFLAGS) -o dist/$(GATEWAY_BINARY)-darwin-amd64 ./cmd/mcp-gateway
	GOOS=darwin  GOARCH=arm64 CGO_ENABLED=0 go build $(GATEWAY_LDFLAGS) -o dist/$(GATEWAY_BINARY)-darwin-arm64 ./cmd/mcp-gateway

# Build + validate an .mcpb MCP Bundle (the Claude Desktop one-click install
# format) for the HOST platform, e.g. dist/mcpb/dbtrail-darwin-arm64.mcpb on
# an Apple Silicon Mac. Release bundles are produced per release-matrix
# platform by GoReleaser (scripts/build-mcpb.sh runs as a bintrail-mcp
# post-build hook and validates each bundle).
mcpb: build-mcp
	bash scripts/build-mcpb.sh $(MCP_BINARY) $(shell go env GOOS) $(shell go env GOARCH) $(VERSION)

# Re-validate already-built bundles (unzip + manifest + entry-binary checks).
validate-mcpb:
	bash scripts/validate-mcpb.sh dist/mcpb/*.mcpb

tidy:
	go mod tidy

deps:
	go mod download

# Regenerate THIRD-PARTY-NOTICES (license-compliance artifact bundled in every
# release channel). Runs go-licenses over the three published binary mains;
# requires CGO_ENABLED=1 (DuckDB) and go-licenses on PATH:
#   go install github.com/google/go-licenses@latest
notices:
	bash scripts/gen-notices.sh

# CI staleness guard: fails if the dependency graph changed without a matching
# `make notices` regeneration. Cheap — hashes `go list -m all`, no CGO build.
check-notices:
	bash scripts/check-notices.sh
