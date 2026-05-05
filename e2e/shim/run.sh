#!/usr/bin/env bash
# Wrapper that runs the bintrail shim end-to-end test.
#
# The test itself owns the docker-compose lifecycle (so a developer
# can also run `SHIM_E2E=1 go test -tags shim_e2e ./e2e/shim/...`
# directly from any directory and get the same setup + teardown).
# This script just sets the gating env var and pins the working
# directory so docker-compose finds its yaml.

set -euo pipefail

cd "$(dirname "$0")"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker not found on PATH; install Docker (with the compose plugin) and retry" >&2
    exit 1
fi

SHIM_E2E=1 go test -tags shim_e2e -v -count=1 ./...
