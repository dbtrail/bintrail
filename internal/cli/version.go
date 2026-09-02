package cli

import "github.com/dbtrail/dbtrail/internal/views"

// buildVersion is the binary's version string, for commands that stamp it into
// generated artifacts (`views`). It lives here rather than being read from the
// root command because internal/cli cannot import cliapp — cliapp imports this
// package — so each main package pushes it in, the same way telemetry.SetVersion
// is wired.
var buildVersion string

// SetBuildVersion records the running binary's version. Unset renders as
// "(unknown version)" wherever it is displayed; it is never load-bearing.
// The views package's snapshot-published file (#1583) is stamped through the
// same call: its producers reach it via hooks with no caller context, so the
// version travels the same push-in road the CLI's own `views` command uses.
func SetBuildVersion(v string) {
	buildVersion = v
	views.SetProducerVersion(v)
}
