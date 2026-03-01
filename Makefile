BINARY_NAME=bintrail
MCP_BINARY=bintrail-mcp
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
BUILD_DATE=$(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

BINTRAIL_LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.CommitSHA=$(COMMIT) -X main.BuildDate=$(BUILD_DATE)"
MCP_LDFLAGS=-ldflags "-X main.mcpVersion=$(VERSION)"

.PHONY: all build build-mcp clean test lint install build-all tidy deps

all: build build-mcp

build:
	go build $(BINTRAIL_LDFLAGS) -o $(BINARY_NAME) ./cmd/bintrail

build-mcp:
	go build $(MCP_LDFLAGS) -o $(MCP_BINARY) ./cmd/bintrail-mcp

install:
	go install $(BINTRAIL_LDFLAGS) ./cmd/bintrail
	go install $(MCP_LDFLAGS) ./cmd/bintrail-mcp

clean:
	rm -f $(BINARY_NAME) $(MCP_BINARY)
	go clean

test:
	go test ./... -count=1

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

tidy:
	go mod tidy

deps:
	go mod download
