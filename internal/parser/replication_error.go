package parser

import (
	"errors"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// ReplicationError wraps a server error packet received over the replication
// protocol (a *gomysql.MyError from StartSync or GetEvent) so packages that
// must not link go-mysql can still read the server's error NUMBER — usage
// telemetry buckets 1236 (binlog purged), 1130 (host not allowed) and the
// same permission / not-found numbers the driver path buckets, through
// telemetry.MySQLNumbered. Everything else about the error is untouched:
// Error() is the original text and Unwrap keeps errors.As on *gomysql.MyError
// working for callers that already inspect it.
type ReplicationError struct {
	code uint16
	err  error
}

func (e *ReplicationError) Error() string { return e.err.Error() }
func (e *ReplicationError) Unwrap() error { return e.err }

// MySQLErrorNumber implements telemetry.MySQLNumbered.
func (e *ReplicationError) MySQLErrorNumber() uint16 { return e.code }

// WrapReplicationError returns err wrapped in a *ReplicationError when its
// chain carries a non-nil *gomysql.MyError, and err itself otherwise
// (including nil, and a typed-nil pointer in the chain — errors.As matches
// one, and go-mysql never produces one, but a deref here would turn a
// classification helper into a crash).
// Call it where a replication error leaves the capture stack. Wrapped today:
// StreamParser.Run (GetEvent), streamrun's StartSync/StartSyncGTID, and the
// agent's startBYOSSyncer. A new syncer call site that returns the raw error
// reports its 1236 as "unknown" again.
func WrapReplicationError(err error) error {
	var my *gomysql.MyError
	if err == nil || !errors.As(err, &my) || my == nil {
		return err
	}
	return &ReplicationError{code: my.Code, err: err}
}
