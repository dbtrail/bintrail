package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/telemetry"
	"github.com/dbtrail/dbtrail/internal/telemetry/telemetrytest"
)

// deliveryCeiling bounds how long the wiring test waits for a background
// delivery. Only a genuine hang trips it; a slow machine costs latency.
const deliveryCeiling = 30 * time.Second

// classedErr stands in for a producer's typed refusal.
type classedErr struct{}

func (classedErr) Error() string {
	return "source server has binlog_format=\"STATEMENT\"; bintrail requires ROW"
}
func (classedErr) TelemetryClass() string { return telemetry.ClassConfigInvalid }

// TestTelemetryHookRecordsTheCommandsClass closes the last seam end to end:
// a RunE that returns a Classed error must land in the delivered event as
// that class, not as unknown (#1503).
func TestTelemetryHookRecordsTheCommandsClass(t *testing.T) {
	telemetrytest.ClearReportingEnv(t)
	c, bodies := telemetrytest.CollectingClient(t)
	var h TelemetryHook
	defer h.SetClientForTest(c)()

	root := &cobra.Command{Use: "x", SilenceErrors: true, SilenceUsage: true}
	stream := &cobra.Command{
		Use:  "stream",
		RunE: func(*cobra.Command, []string) error { return classedErr{} },
	}
	root.AddCommand(stream)
	root.SetArgs([]string{"stream"})

	// Start would resolve a client from the environment; the collecting one
	// is installed by SetClientForTest, so record the span on it directly.
	// Finish only spools; the shortened daemon tick is what delivers.
	h.span = c.RecordCommand("stream")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.RunDaemon(ctx, "x")
	if err := h.Execute(root); err == nil {
		t.Fatal("Execute must return the command's error")
	}

	// The poll returns the moment the body lands; the ceiling only bounds a
	// genuine hang. It is generous on purpose: a daemon tick, a drain and a
	// POST all run in the background, and a fixed few-second budget for them
	// is what fails a loaded release runner (#1502). The collecting handler
	// lives in the shared telemetrytest helper, which is why this waits on
	// bodies() rather than on a channel of its own.
	deadline := time.Now().Add(deliveryCeiling)
	for time.Now().Before(deadline) {
		for _, b := range bodies() {
			if strings.Contains(b, `"error_class":"config_invalid"`) && strings.Contains(b, `"command":"stream"`) {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no delivered event carried error_class=config_invalid for stream within %v; bodies: %v", deliveryCeiling, bodies())
}
