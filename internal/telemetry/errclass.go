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
const (
	ClassDBConnection   = "db_connection"
	ClassDBPermission   = "db_permission"
	ClassBinlogParse    = "binlog_parse"
	ClassBinlogNotFound = "binlog_not_found"
	ClassSchemaMismatch = "schema_mismatch"
	ClassConfigInvalid  = "config_invalid"
	ClassFlagInvalid    = "flag_invalid"
	ClassStorageIO      = "storage_io"
	ClassNetwork        = "network"
	ClassNotFound       = "not_found"
	ClassInternal       = "internal"
	ClassUnknown        = "unknown"
)

// classes is the set ClassifyError and SetError may emit. Anything outside it
// is coerced to ClassUnknown rather than trusted onto the wire.
var classes = map[string]bool{
	ClassDBConnection:   true,
	ClassDBPermission:   true,
	ClassBinlogParse:    true,
	ClassBinlogNotFound: true,
	ClassSchemaMismatch: true,
	ClassConfigInvalid:  true,
	ClassFlagInvalid:    true,
	ClassStorageIO:      true,
	ClassNetwork:        true,
	ClassNotFound:       true,
	ClassInternal:       true,
	ClassUnknown:        true,
}

// MySQL server error numbers worth distinguishing. Only the number is read —
// MySQLError.Message can contain schema, table and user names.
const (
	erDBAccessDenied     = 1044
	erAccessDenied       = 1045
	erTableAccessDenied  = 1142
	erColumnAccessDenied = 1143
	erSpecificAccess     = 1227
	erBadDB              = 1049
	erNoSuchTable        = 1146
)

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

	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		switch myErr.Number {
		case erAccessDenied, erDBAccessDenied, erTableAccessDenied, erColumnAccessDenied, erSpecificAccess:
			return ClassDBPermission
		case erBadDB, erNoSuchTable:
			return ClassNotFound
		}
		// The server answered, so this is not a connectivity problem, but the
		// specific failure is not one we have a bucket for.
		return ClassUnknown
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
