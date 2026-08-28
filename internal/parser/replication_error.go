package parser

import (
	"errors"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// ReplicationError wraps a server error packet received over the replication
// protocol (a *gomysql.MyError from StartSync or GetEvent) so packages that
// must not link go-mysql can still read the server's error NUMBER — usage
// telemetry buckets 1236 (binlog purged) and 1130 (host not allowed) through
// telemetry.MySQLNumbered. Everything else about the error is untouched:
// Error() is the original text and Unwrap keeps errors.As on *gomysql.MyError
// working for callers that already inspect it.
type ReplicationError struct {
	Code uint16
	err  error
}

func (e *ReplicationError) Error() string { return e.err.Error() }
func (e *ReplicationError) Unwrap() error { return e.err }

// MySQLErrorNumber implements telemetry.MySQLNumbered.
func (e *ReplicationError) MySQLErrorNumber() uint16 { return e.Code }

// WrapReplicationError returns err wrapped in a *ReplicationError when its
// chain carries a *gomysql.MyError, and err itself otherwise (including nil).
// Call it where a replication error leaves the capture stack: StreamParser.Run
// and the StartSync/StartSyncGTID call sites.
func WrapReplicationError(err error) error {
	var my *gomysql.MyError
	if err == nil || !errors.As(err, &my) {
		return err
	}
	return &ReplicationError{Code: my.Code, err: err}
}
