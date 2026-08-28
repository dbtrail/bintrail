package parser

import (
	"context"
	"errors"
	"fmt"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

func TestWrapReplicationError(t *testing.T) {
	if WrapReplicationError(nil) != nil {
		t.Fatal("nil must stay nil")
	}
	plain := errors.New("connection reset")
	if got := WrapReplicationError(plain); got != plain {
		t.Fatalf("a non-server error must pass through unchanged, got %T", got)
	}
	var typedNil *gomysql.MyError
	nilInChain := fmt.Errorf("sync: %w", typedNil)
	if got := WrapReplicationError(nilInChain); got != nilInChain {
		t.Fatalf("a typed-nil *MyError in the chain must pass through, got %T", got)
	}

	src := &gomysql.MyError{Code: 1236, Message: "Could not find first log file name in binary log index file", State: "HY000"}
	wrapped := WrapReplicationError(fmt.Errorf("sync: %w", src))

	var re *ReplicationError
	if !errors.As(wrapped, &re) || re.MySQLErrorNumber() != 1236 {
		t.Fatalf("wrapped = %T (%v), want *ReplicationError carrying 1236", wrapped, wrapped)
	}
	var my *gomysql.MyError
	if !errors.As(wrapped, &my) || my != src {
		t.Error("Unwrap must keep the original *gomysql.MyError reachable")
	}
	if wrapped.Error() != "sync: "+src.Error() {
		t.Errorf("Error() changed the text: %q", wrapped.Error())
	}
	if got := telemetry.ClassifyError(wrapped); got != telemetry.ClassBinlogNotFound {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassBinlogNotFound)
	}
	if got := telemetry.ClassifyError(WrapReplicationError(&gomysql.MyError{Code: 1130})); got != telemetry.ClassDBPermission {
		t.Errorf("ClassifyError(1130) = %q, want %q", got, telemetry.ClassDBPermission)
	}
}

// TestStreamParser_replicationErrorIsWrapped drives the REAL exit in Run: a
// server error packet surfacing from GetEvent leaves the parser as a
// *ReplicationError, so usage telemetry sees the number (#1503). A hand-built
// value would not catch a refactor that returns the raw error again.
func TestStreamParser_replicationErrorIsWrapped(t *testing.T) {
	sp := NewStreamParser(driftResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 4)
	if !streamer.AddErrorToStreamer(&gomysql.MyError{Code: 1236, Message: "Could not find first log file name in binary log index file"}) {
		t.Fatal("AddErrorToStreamer refused the error")
	}

	err := sp.Run(context.Background(), streamer, out)
	if err == nil {
		t.Fatal("Run must surface the server error")
	}
	var re *ReplicationError
	if !errors.As(err, &re) || re.MySQLErrorNumber() != 1236 {
		t.Fatalf("Run returned %T (%v), want *ReplicationError carrying 1236", err, err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassBinlogNotFound {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassBinlogNotFound)
	}
}
