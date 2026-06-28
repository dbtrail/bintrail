package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// TestRunVerifyBaselinePair_NothingToVerify locks the CLI early-return: with
// fewer than two baselines under the source, the run prints a clear message and
// exits 0 (not a failure — the first baseline has no predecessor). It returns
// before touching the index, so nil DBs are safe here.
func TestRunVerifyBaselinePair_NothingToVerify(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)

	err := runVerifyBaselinePair(cmd, nil, nil, "", t.TempDir(), duckdbutil.Tuning{})
	if err != nil {
		t.Fatalf("want nil error (exit 0) for <2 baselines, got %v", err)
	}
	if !strings.Contains(out.String(), "nothing to verify") {
		t.Errorf("want a 'nothing to verify' message, got %q", out.String())
	}
}
