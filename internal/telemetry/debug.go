package telemetry

import (
	"fmt"
	"os"
)

// DebugEnvVar turns on a diagnostic trace to stderr.
//
// Telemetry swallows every error by design — it must never change a command's
// behaviour, output, or exit code. The cost is that a subsystem which is on by
// default can be silently inert for a bad reason (unwritable spool, a
// permanently failing endpoint, a claim nobody can clean up) with no signal to
// the operator or to us. This flag is that signal, opt-in and off by default so
// the contract still holds for everyone who does not ask for it.
const DebugEnvVar = "BINTRAIL_TELEMETRY_DEBUG"

// debugf writes one diagnostic line when DebugEnvVar is set. Deliberately the
// ONLY place telemetry may write to stderr outside the first-run notice.
func debugf(format string, args ...any) {
	if v := os.Getenv(DebugEnvVar); v == "" || v == "0" {
		return
	}
	fmt.Fprintf(os.Stderr, "telemetry: "+format+"\n", args...)
}
