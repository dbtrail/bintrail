package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/views"
)

// The hot leg of the downloadable views.sql (#1480), resolved from the server
// the console already has open.
//
// `bintrail views --include-live` does the same job in internal/cli, and this
// is deliberately not a call into it: every refusal there names --index-dsn or
// --include-live, and a JSON error the console renders in the browser must not
// send an operator looking for a command-line flag. The FACTS the two collect
// are the same three, and the reason each is collected is the same:
//
//   - host / port / database / user, from the DSN, never the password;
//   - the index's binlog_events column set, because the hot leg has no
//     union_by_name and naming a column this index lacks makes DuckDB refuse
//     the whole file with a binder error;
//   - which source the index serves, which can only be stated when exactly one
//     is registered.

// liveLegHowTo is the archives-only note's remediation for a reader who
// downloaded the file from this page. It replaces the CLI flag the generator
// would otherwise name (views.Input.LiveLegHowTo).
const liveLegHowTo = `Add a leg over the live index by ticking "Include the live index" on the Query in DuckDB card, then downloading again.`

// liveLegConfigError is a live leg this server cannot carry however it is
// asked for: no open connection, or an index DSN that names no reachable
// host, port or database. It is the caller's request meeting this server's
// configuration, so it answers 422, not the 502 an upstream fault gets.
type liveLegConfigError struct{ msg string }

func (e *liveLegConfigError) Error() string { return e.msg }

// consoleLiveTarget is the PURE half: the non-secret connection facts, out of
// the DSN this bundle already holds. No IO, so callers can ask whether the leg
// is offerable at all without touching the index.
//
// The password is dropped HERE, at the one boundary where it is in scope, so
// nothing downstream can start emitting it into a file meant to be shared.
func consoleLiveTarget(b *bundle) (*views.LiveIndex, error) {
	if b == nil || b.db == nil {
		return nil, &liveLegConfigError{msg: "the live index leg needs an open connection to this server's index, and this server has none"}
	}
	cfg, err := drivermysql.ParseDSN(b.dsn)
	if err != nil {
		return nil, &liveLegConfigError{msg: "this server's index connection cannot be described in the file: " + scrubDSNError(err, b.dsn)}
	}
	if strings.EqualFold(cfg.Net, "unix") {
		// Refused rather than rendered, for the same reason the CLI refuses
		// it: the generated file locates the index by host and port so it can
		// be run from another machine, and a socket path names one machine.
		return nil, &liveLegConfigError{msg: "this server's index is reached over a unix socket, which names only the machine this console runs on. " +
			"The file locates the index by host and port so it can be run anywhere, so give this server a host and port to include the live index"}
	}
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, &liveLegConfigError{msg: "this server's index address has no host and port to put in the file, so the live index cannot be included"}
	}
	if host == "" {
		// ":3306" parses, and the driver reads the empty host as localhost on
		// its own machine. The file cannot: it would carry HOST '', which
		// names nothing to a reader anywhere, and which the generator's
		// loopback warning does not recognize as a local address either. A
		// refusal that says to name the host is the only honest answer.
		return nil, &liveLegConfigError{msg: "this server's index address names a port but no host, and the file locates the index by host and port " +
			"so it can be run from another machine. Give this server's connection a host name or address to include the live index"}
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, &liveLegConfigError{msg: "this server's index address has an unusable port, so the live index cannot be included"}
	}
	if cfg.DBName == "" {
		return nil, &liveLegConfigError{msg: "this server's index connection names no database, so the live index has nothing to attach"}
	}
	return &views.LiveIndex{Host: host, Port: int(port), Database: cfg.DBName, User: cfg.User}, nil
}

// consoleCanOfferLiveLeg reports whether this server could carry the hot leg
// if asked. It decides only what the archives-only note SAYS, so it stays
// free of IO: a reader is told either why there is no route or what the route
// is, and asking the index a question to write a comment would put a network
// round trip on every download.
func consoleCanOfferLiveLeg(b *bundle) bool {
	_, err := consoleLiveTarget(b)
	return err == nil
}

// resolveConsoleLiveIndex fills in what the generated SQL cannot be correct
// without: the columns this index actually has, and which source its rows can
// be attributed to.
func resolveConsoleLiveIndex(ctx context.Context, b *bundle) (*views.LiveIndex, error) {
	li, err := consoleLiveTarget(b)
	if err != nil {
		return nil, err
	}
	cols, err := consoleLiveColumns(ctx, b.db, li.Database)
	if err != nil {
		return nil, err
	}
	li.TableColumns = cols
	consoleLiveAttribution(ctx, b.db, li)
	return li, nil
}

// consoleLiveColumns reads the index's binlog_events column set.
//
// NOT best effort, unlike the attribution below: it decides whether the file
// binds at all. The hot leg has no union_by_name, so one column named that
// this index does not have turns the whole download into a binder error that
// defines no view. A legacy registry index (the console never migrates one)
// is exactly that case.
func consoleLiveColumns(ctx context.Context, db *sql.DB, dbName string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT COLUMN_NAME FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'`, dbName)
	if err != nil {
		return nil, fmt.Errorf("read the index's binlog_events columns for the live index: %w", err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, fmt.Errorf("read the index's binlog_events columns for the live index: %w", err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the index's binlog_events columns for the live index: %w", err)
	}
	if len(cols) == 0 {
		return nil, &liveLegConfigError{msg: "this server's index database has no binlog_events table, " +
			"and the live index is a view over that table"}
	}
	return cols, nil
}

// consoleLiveAttribution records what the index says about the sources it
// serves. Attribution is only possible with exactly ONE registered source:
// every source writes into the same binlog_events and a row carries no
// identity of its own. Best effort, and each outcome is kept distinct, because
// the file states what was observed and an unreadable list is not an empty one.
func consoleLiveAttribution(ctx context.Context, db *sql.DB, li *views.LiveIndex) {
	var n int
	var id string
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(MIN(bintrail_id), '') FROM bintrail_servers`).Scan(&n, &id)
	if err != nil {
		var myErr *drivermysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1146 {
			// No such table: zero known sources, not an unreadable list. An
			// index that never ran the migration is not evidence of several.
			li.Attribution = views.AttributionUnregistered
			return
		}
		// Undetermined is what the FILE says, and it deliberately names no
		// cause: a revoked SELECT, a dropped connection and a timeout are
		// one sentence there. They are not one thing to fix, so the cause
		// goes where it can be acted on. Same split the archive-source read
		// makes: the file is what leaves the host, the log is what stays.
		slog.Warn("console: could not read the index's registered sources for the live leg of views.sql; "+
			"the file will say the sources could not be read", "error", err)
		li.Attribution = views.AttributionUndetermined
		return
	}
	switch {
	case n == 1 && id != "":
		li.BintrailID = id
	case n > 1:
		li.Attribution = views.AttributionMultiSource
	default:
		li.Attribution = views.AttributionUnregistered
	}
}
