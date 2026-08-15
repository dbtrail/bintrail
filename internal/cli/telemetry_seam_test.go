package cli

import (
	"testing"

	"github.com/spf13/cobra"
)

// TestStartPublishesProcessClient guards the one production hop between a
// binary's TelemetryHook and the daemon commands implemented in this package:
// Start must publish the client it resolved into the processClient seam, or
// shim's (and any future in-package daemon's) RunDaemon launch would run on a
// nil client forever — beacons silently gone, nothing failing. The wiring
// tests inject via SetClientForTest and so would never notice.
//
// It also fences the #1061 hazard this file's own comment names: a subcommand
// that grows its own PersistentPreRunE stops Start from running at all, and
// with it this publication.
func TestStartPublishesProcessClient(t *testing.T) {
	prevProcess := processClient
	t.Cleanup(func() { processClient = prevProcess })
	processClient = nil

	root := &cobra.Command{Use: "bintrail"}
	root.PersistentFlags().String("telemetry", "", "")
	child := &cobra.Command{Use: "shim"}
	root.AddCommand(child)

	var h TelemetryHook
	h.Start(child)

	if h.Client() == nil {
		t.Fatal("Start resolved no client")
	}
	if processClient != h.Client() {
		t.Fatal("Start did not publish its client to processClient — shim's daemon telemetry loop would never see it")
	}
}
