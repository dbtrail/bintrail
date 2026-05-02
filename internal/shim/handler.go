package shim

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/bintrail/internal/query"
)

// Handler implements server.Handler. It serves the small subset of
// MySQL protocol the BYOS time-travel SQL story needs: USE <db>,
// `SELECT * FROM _flashback.<table> AS OF '<ts>' WHERE <col> = <value>`,
// and a handful of bookkeeping queries the standard MySQL clients send
// during connection setup.
//
// Anything else returns a clear error to the client. The MVP does not
// proxy non-flashback queries to the real MySQL — that's the job of
// ProxySQL sitting in front of the shim.
type Handler struct {
	server.EmptyHandler

	indexDB *sql.DB
	logger  *slog.Logger

	mu sync.Mutex
	db string // currently selected database (per COM_INIT_DB)
}

// NewHandler constructs a Handler bound to a bintrail index DSN.
func NewHandler(indexDB *sql.DB, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{indexDB: indexDB, logger: logger}
}

// UseDB stores the schema the client selected. _flashback queries
// without an explicit schema use this value.
func (h *Handler) UseDB(dbName string) error {
	h.mu.Lock()
	h.db = dbName
	h.mu.Unlock()
	return nil
}

// HandleQuery dispatches the incoming statement. The order matters: we
// first try to parse it as a _flashback query; that catches every
// statement Parse considers well-formed. Everything else falls through
// to a small allow-list of harmless setup queries (so MySQL clients
// don't choke during the handshake-adjacent SET/SELECT spam).
func (h *Handler) HandleQuery(qstr string) (*mysql.Result, error) {
	h.mu.Lock()
	currentDB := h.db
	h.mu.Unlock()

	// Step 1: is this a _flashback query? If yes, run it; if it's
	// recognised but malformed, return that error to the client. If
	// it's something else entirely, fall through to step 2.
	fq, perr := Parse(qstr, currentDB)
	if perr == nil {
		return h.runFlashback(fq)
	}
	if !errors.Is(perr, ErrNotFlashback) {
		return nil, perr
	}

	// Step 2: handshake noise. Modern MySQL clients (mysql CLI,
	// go-sql-driver) issue SET / SELECT @@variable / SHOW WARNINGS at
	// connection time; refusing them aborts the handshake. Reply with
	// empty success or known-safe values for the common cases.
	if isHandshakeNoise(qstr) {
		return &mysql.Result{Status: 2}, nil
	}

	return nil, fmt.Errorf(
		"this server only handles `SELECT * FROM _flashback.<table> AS OF '<ts>' WHERE <col> = <value>` queries; got: %s",
		strings.TrimSpace(qstr),
	)
}

// runFlashback resolves a parsed FlashbackQuery against the bintrail
// index and reconstructs the row's state at q.AsOf.
//
// MVP semantics: this returns the row_after of the most recent event
// for that PK at or before q.AsOf. That's the right answer for tables
// where the latest event before the cutoff captures the row's state,
// which holds for any INSERT/UPDATE — the core use case.
//
// Edge case (not yet handled): if the most recent event is a DELETE,
// the row didn't exist at q.AsOf and the result should be empty. The
// MVP returns the DELETE's row_before instead, which is technically
// the row's state immediately *before* deletion. A proper
// implementation would distinguish these two cases.
func (h *Handler) runFlashback(q FlashbackQuery) (*mysql.Result, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	engine := query.New(h.indexDB)
	rows, err := engine.Fetch(ctx, query.Options{
		Schema:   q.Schema,
		Table:    q.Table,
		PKValues: q.PKValue,
		Until:    &q.AsOf,
		Limit:    1,
	})
	if err != nil {
		return nil, fmt.Errorf("query bintrail index: %w", err)
	}

	if len(rows) == 0 {
		return emptyResult(), nil
	}

	// Pick the most recent event by (timestamp, event_id).
	sort.SliceStable(rows, func(i, j int) bool {
		if !rows[i].EventTimestamp.Equal(rows[j].EventTimestamp) {
			return rows[i].EventTimestamp.After(rows[j].EventTimestamp)
		}
		return rows[i].EventID > rows[j].EventID
	})
	latest := rows[0]

	// Prefer row_after (post-image after INSERT/UPDATE). Fall back to
	// row_before (the row's state captured at the point of DELETE).
	var image map[string]any
	switch {
	case len(latest.RowAfter) > 0:
		image = latest.RowAfter
	case len(latest.RowBefore) > 0:
		image = latest.RowBefore
	default:
		return emptyResult(), nil
	}

	return imageToResult(image)
}

// imageToResult turns a single-row JSON object into a mysql.Result
// shaped for the wire protocol. Column order is the JSON key order
// after sorting alphabetically — deterministic, and good enough for
// the MVP. A future revision can pick the order from the schema
// snapshot to match the source table's DDL.
func imageToResult(image map[string]any) (*mysql.Result, error) {
	if len(image) == 0 {
		return emptyResult(), nil
	}

	cols := make([]string, 0, len(image))
	for k := range image {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	row := make([]any, len(cols))
	for i, c := range cols {
		row[i] = image[c]
	}

	rs, err := mysql.BuildSimpleTextResultset(cols, [][]any{row})
	if err != nil {
		return nil, fmt.Errorf("build resultset: %w", err)
	}
	return &mysql.Result{Resultset: rs}, nil
}

// emptyResult is the wire-protocol "zero rows" reply. We still need a
// resultset (so the client gets a proper SELECT response, not an OK
// packet), so we use the original column list with no rows.
func emptyResult() *mysql.Result {
	rs, _ := mysql.BuildSimpleTextResultset([]string{"_flashback"}, nil)
	return &mysql.Result{Resultset: rs}
}

// isHandshakeNoise matches the handful of statements MySQL clients
// issue automatically and that have no meaningful behaviour for a
// shim. Returning success keeps the connection alive without us having
// to implement them.
func isHandshakeNoise(q string) bool {
	q = strings.TrimSpace(strings.ToLower(q))
	q = strings.TrimSuffix(q, ";")
	switch {
	case strings.HasPrefix(q, "set "):
		return true
	case strings.HasPrefix(q, "select @@"):
		return true
	case strings.HasPrefix(q, "show warnings"):
		return true
	case q == "select version()" || q == "select database()":
		return true
	}
	return false
}

// AcceptAuth is a credential provider that accepts any username and
// password. This is MVP-only: a real deployment must validate against
// the credentials configured in shim.yaml's mysql_user /
// mysql_pass_sha1, the same way ProxySQL does.
type AcceptAuth struct{}

// CheckUsername implements server.CredentialProvider.
func (AcceptAuth) CheckUsername(string) (bool, error) { return true, nil }

// GetCredential implements server.CredentialProvider.
//
// Returning the empty plaintext + found=true tells go-mysql/server to
// run the mysql_native_password challenge against an empty password.
// Clients that send an empty password will succeed; clients that send
// any password will see auth fail. This is intentionally permissive
// for the MVP — the shim has no real authentication yet.
func (AcceptAuth) GetCredential(string) (string, bool, error) {
	return "", true, nil
}
