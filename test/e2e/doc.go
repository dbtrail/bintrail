// Package e2e holds the full-pipeline end-to-end test for the bintrail CLI
// (init → snapshot → index → query → recover → status → rotate). The test
// itself lives in e2e_test.go and is gated behind the `integration` build tag
// plus a reachable MySQL on 127.0.0.1:13306.
//
// This file exists solely so `go list ./test/e2e/...` reports a valid (empty)
// package when the build tag is not set — without it, IDEs and `go vet ./...`
// flag the directory as "no Go files". Same reason as test/shim/doc.go.
package e2e
