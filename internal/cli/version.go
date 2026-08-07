package cli

// buildVersion is the binary's version string, for commands that stamp it into
// generated artifacts (`views`). It lives here rather than being read from the
// root command because internal/cli cannot import cliapp — cliapp imports this
// package — so each main package pushes it in, the same way telemetry.SetVersion
// is wired.
var buildVersion string

// SetBuildVersion records the running binary's version. Unset renders as
// "(unknown version)" wherever it is displayed; it is never load-bearing.
func SetBuildVersion(v string) { buildVersion = v }
