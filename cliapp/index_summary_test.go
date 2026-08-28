package cliapp

import (
	"errors"
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestIndexFailureSummaryKeepsTheFirstCause: the summary `bintrail index`
// exits with must still carry the first per-file failure, so a typed cause
// keeps its usage-telemetry class instead of collapsing to unknown (#1503).
func TestIndexFailureSummaryKeepsTheFirstCause(t *testing.T) {
	// A replication error is a stand-in chosen for its distinct class; file
	// mode never produces one (its causes are the parser guards, index-side
	// driver errors and unreadable files).
	first := parser.WrapReplicationError(&gomysql.MyError{Code: 1236, Message: "binlog purged"})
	err := indexFailureSummary(2, 3, first)
	if !strings.HasPrefix(err.Error(), "indexing finished with 2 of 3 file(s) failed") {
		t.Errorf("message = %q", err.Error())
	}
	var re *parser.ReplicationError
	if !errors.As(err, &re) {
		t.Fatalf("summary lost the first failure's type: %T", err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassBinlogNotFound {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassBinlogNotFound)
	}
}
