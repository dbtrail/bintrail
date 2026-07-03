// Package ext exposes the extension points that embedding distributions
// — builds that import cliapp and wrap the OSS core — use to inject
// behavior: an audit sink recording data-access operations, and
// overrides for feature-entitlement gates.
//
// Seams follow the same convention as the internal forensics.Enabled
// gate: package-level variables set once at process startup (before any
// command runs), called by the core at surface entry points (CLI, MCP,
// shim, console) — never inside the library layer. The OSS binary
// leaves every seam at its default: auditing is a no-op and every
// feature gate is open. Setters are not safe for concurrent use with
// command execution; call them from main() before dispatch.
package ext

import (
	"os"
	"os/user"

	"github.com/dbtrail/dbtrail/internal/forensics"
)

// SetForensicsEnabled overrides the forensics feature gate. The OSS
// default is always-on; an embedding distribution may tie it to its own
// entitlement model.
func SetForensicsEnabled(f func() bool) {
	forensics.Enabled = f
}

// ProcessActor returns the best available identity for locally-invoked
// surfaces (CLI, stdio MCP): the operating-system user, plus the RBAC
// profile when one is active. Network surfaces with real authentication
// (the shim) should record their authenticated user instead.
func ProcessActor(profile string) string {
	name := os.Getenv("USER")
	if u, err := user.Current(); err == nil && u.Username != "" {
		name = u.Username
	}
	actor := "os:" + name
	if profile != "" {
		actor += " profile:" + profile
	}
	return actor
}
