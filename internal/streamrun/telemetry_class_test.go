package streamrun

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestGapRefusedErrorIsClassed: the --no-gap-fill refusal keeps the message
// the integration test pins (naming the flag) and classifies as
// binlog_not_found — the same bucket as the server's own 1236 (#1503).
func TestGapRefusedErrorIsClassed(t *testing.T) {
	err := &GapRefusedError{msg: "binlog mysql-bin.000042 purged on db.internal"}
	if !strings.HasPrefix(err.Error(), "binlog gap detected and --no-gap-fill is set: ") || !strings.Contains(err.Error(), "000042") {
		t.Errorf("message = %q", err.Error())
	}
	if got := telemetry.ClassifyError(fmt.Errorf("stream: %w", err)); got != telemetry.ClassBinlogNotFound {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassBinlogNotFound)
	}
}
