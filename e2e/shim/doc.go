// Package shim_e2e holds the docker-compose-driven end-to-end test
// for `bintrail shim`. The test itself lives in e2e_test.go and is
// gated behind the `shim_e2e` build tag plus an explicit SHIM_E2E=1
// env var. See README.md for the run instructions.
//
// This file exists solely so `go list ./e2e/shim/...` reports a
// valid (empty) package when the build tag is not set — without it,
// IDEs and `go vet ./...` flag the directory as "no Go files".
package shim_e2e
