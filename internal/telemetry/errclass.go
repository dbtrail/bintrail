package telemetry

import (
	"context"
	"errors"
	"io/fs"
	"net"

	"github.com/go-sql-driver/mysql"
)

// Error classes. This is the complete taxonomy — ClassifyError never returns
// anything else, and in particular never returns err.Error(): bintrail error
// strings routinely carry DSNs, hostnames, schema and table names, and file
// paths.
//
// Every class here has at least one producer in the tree. Three that never
// had one (binlog_parse, flag_invalid, network) were dropped in #1503: a
// documented class no code path can emit reads as coverage that does not
// exist, and the only way to produce them would have been to match on
// message text, which this package refuses to do.
const (
	ClassDBConnection   = "db_connection"
	ClassDBPermission   = "db_permission"
	ClassBinlogNotFound = "binlog_not_found"
	ClassSchemaMismatch = "schema_mismatch"
	ClassConfigInvalid  = "config_invalid"
	ClassStorageIO      = "storage_io"
	ClassNotFound       = "not_found"
	ClassInternal       = "internal"
	ClassUnknown        = "unknown"
)

// classes is the set ClassifyError and SetError may emit. Anything outside it
// is coerced to ClassUnknown rather than trusted onto the wire.
var classes = map[string]bool{
	ClassDBConnection:   true,
	ClassDBPermission:   true,
	ClassBinlogNotFound: true,
	ClassSchemaMismatch: true,
	ClassConfigInvalid:  true,
	ClassStorageIO:      true,
	ClassNotFound:       true,
	ClassInternal:       true,
	ClassUnknown:        true,
}

// Classed is implemented by errors that know their own telemetry class. The
// packages that produce a failure worth distinguishing implement it on their
// sentinel or typed error, which lets ClassifyError bucket them without this
// package importing any of them. That direction is forced, not chosen:
// telemetry is imported by internal/console, a read-layer package whose
// depguard (internal/event) forbids linking the capture stack, and a leaf
// package importing its own producers invites cycles. The method returns a
// class NAME, never a message; a value outside the taxonomy is coerced to
// "unknown" by normalizeClass. This package's tests deliberately do not
// import the producers either, so each producer package carries a wiring
// test that asserts the exact class against its real error.
type Classed interface {
	TelemetryClass() string
}

// MySQLNumbered is implemented by errors that carry a MySQL server error
// number from a client library this package must not link. The replication
// client (go-mysql) is the one that sees a binlog-side failure such as 1236,
// but it is a capture library, and telemetry sits under the read layer too
// (internal/event's depguard test forbids the console from linking it), so
// the capture side wraps its error (parser.ReplicationError) and this package
// reads the number through the interface. The number is all that is read.
type MySQLNumbered interface {
	MySQLErrorNumber() uint16
}

// MySQL server error numbers worth distinguishing. Only the number is read —
// the Message field of either error type can contain schema, table and user
// names.
const (
	erDBAccessDenied     = 1044
	erAccessDenied       = 1045
	erHostNotPrivileged  = 1130
	erTableAccessDenied  = 1142
	erColumnAccessDenied = 1143
	erSpecificAccess     = 1227
	erBadDB              = 1049
	erNoSuchTable        = 1146
	// ER_MASTER_FATAL_ERROR_READING_BINLOG: the source cannot serve the
	// requested position — the binlog was purged, or the GTID set names
	// transactions it no longer has. The replication client is the only path
	// that ever sees it, which is why it arrives through MySQLNumbered and not
	// as the driver's *MySQLError.
	erMasterFatalReadingBinlog = 1236
)

// classifyMySQLNumber maps a server error number to a class, shared by the
// two client libraries that can surface one. A number with no bucket is
// ClassUnknown, never a connectivity class: the server ANSWERED, so the
// failure is specific and simply not one we have a bucket for.
func classifyMySQLNumber(number uint16) string {
	switch number {
	case erAccessDenied, erDBAccessDenied, erHostNotPrivileged, erTableAccessDenied, erColumnAccessDenied, erSpecificAccess:
		return ClassDBPermission
	case erBadDB, erNoSuchTable:
		return ClassNotFound
	case erMasterFatalReadingBinlog:
		return ClassBinlogNotFound
	}
	return ClassUnknown
}

// ClassifyError maps an error to a bounded class using structural checks
// (errors.Is/errors.As) only — never string matching, which would couple the
// taxonomy to message wording and tempt someone into shipping the message.
//
// Deliberately conservative: callers that know more about the failure should
// pass a precise class to Span.SetError instead of relying on this. An honest
// "unknown" beats a confidently wrong bucket.
func ClassifyError(err error) string {
	if err == nil {
		return ""
	}

	// A producer that declared its own class wins: it knows more about the
	// failure than any structural probe below can infer.
	var classed Classed
	if errors.As(err, &classed) {
		return normalizeClass(classed.TelemetryClass())
	}

	// The query driver (go-sql-driver) and the replication client each wrap a
	// server error packet in their own type; a binlog-side failure only ever
	// arrives through the latter, which reaches here as a MySQLNumbered.
	var drvErr *mysql.MySQLError
	if errors.As(err, &drvErr) {
		return classifyMySQLNumber(drvErr.Number)
	}
	var numbered MySQLNumbered
	if errors.As(err, &numbered) {
		return classifyMySQLNumber(numbered.MySQLErrorNumber())
	}

	// A driver-level failure (bad host, refused, TLS) surfaces as a net error
	// wrapped by the driver rather than a MySQLError.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return ClassDBConnection
	}
	if errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, context.DeadlineExceeded) {
		return ClassDBConnection
	}

	if errors.Is(err, fs.ErrNotExist) {
		return ClassNotFound
	}
	if errors.Is(err, fs.ErrPermission) {
		return ClassStorageIO
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return ClassStorageIO
	}

	return ClassUnknown
}

// normalizeClass coerces an arbitrary caller-supplied class to the taxonomy,
// so a typo or a future refactor cannot smuggle free text onto the wire.
func normalizeClass(class string) string {
	if classes[class] {
		return class
	}
	return ClassUnknown
}
