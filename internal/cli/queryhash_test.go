package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

const testQueryDigest = "3f2a1b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708"

// setQueryHashFlags sets the package-level flag vars runQuery reads and restores
// them afterwards — cobra's flag variables are process-global, so a test that
// leaked one would change what the NEXT test's runQuery does.
func setQueryHashFlags(t *testing.T, hash, profile string) {
	t.Helper()
	oldHash, oldProfile := qQueryHash, qProfile
	qQueryHash, qProfile = hash, profile
	t.Cleanup(func() { qQueryHash, qProfile = oldHash, oldProfile })
}

// newQueryHashTestCmd is the minimal cobra command runQuery needs. Kept local
// because the fuller newQueryTestCmd lives behind the integration build tag.
func newQueryHashTestCmd() *cobra.Command {
	c := &cobra.Command{}
	c.SetContext(context.Background())
	AddDuckDBTuningFlags(c)
	return c
}

func TestQueryCmd_queryHashFlagRegistered(t *testing.T) {
	f := queryCmd.Flags().Lookup("query-hash")
	if f == nil {
		t.Fatal("--query-hash is not registered on the query command")
	}
	// Registered on query ONLY: a digest names a statement SHAPE, so a reversal
	// scoped to one would undo executions the operator never named.
	if recoverCmd.Flags().Lookup("query-hash") != nil {
		t.Error("--query-hash must not exist on recover: it selects every execution of a statement shape, which is not a blast radius anyone chose")
	}
}

// TestRunQuery_refusesQueryHashWithProfile keeps the refusal at the CLI, where
// the operator gets both flag names back. The engine refuses too
// (query.ErrQueryHashUnderProfile); that copy covers library and MCP callers,
// this one covers the person typing.
func TestRunQuery_refusesQueryHashWithProfile(t *testing.T) {
	setQueryHashFlags(t, testQueryDigest, "analyst")

	err := runQuery(newQueryHashTestCmd(), nil)
	if err == nil {
		t.Fatal("query ran with both --query-hash and --profile")
	}
	if !strings.Contains(err.Error(), "--query-hash") || !strings.Contains(err.Error(), "--profile") {
		t.Errorf("error must name both flags, got: %v", err)
	}
}

// TestRunQuery_rejectsMalformedDigest: the natural mistake is pasting the
// statement instead of its hash. That must be loud — a bad digest matches no
// row on any engine, which is indistinguishable from a correct filter over a
// statement that touched nothing.
func TestRunQuery_rejectsMalformedDigest(t *testing.T) {
	setQueryHashFlags(t, "UPDATE mydb.orders SET status = 'shipped'", "")

	err := runQuery(newQueryHashTestCmd(), nil)
	if err == nil || !strings.Contains(err.Error(), "--query-hash") {
		t.Fatalf("err = %v, want a --query-hash validation error", err)
	}
}
