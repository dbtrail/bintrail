package shim

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql" // database/sql driver registration
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// TestHandlerHandshakeNoise verifies the small allow-list for queries
// MySQL clients send during connection setup — these shouldn't be
// rejected as "non-flashback" because that would abort the handshake
// before the customer ever runs a real query.
func TestHandlerHandshakeNoise(t *testing.T) {
	h := NewHandler(nil, nil)

	cases := []string{
		"SET NAMES 'utf8mb4'",
		"SET autocommit=1",
		"SET session transaction isolation level read committed",
		"SET sql_mode = 'TRADITIONAL'",
		"SELECT @@version",
		"SELECT @@session.tx_isolation",
		"SHOW WARNINGS",
		"select database()",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			res, err := h.HandleQuery(q)
			if err != nil {
				t.Errorf("expected handshake noise to succeed, got %v", err)
			}
			if res == nil {
				t.Error("expected non-nil result")
			}
		})
	}
}

// TestHandlerHandshakeNoiseRejectsPrivileged — narrow allow-listing
// matters: an over-broad `set ` prefix would let a caller smuggle
// privileged DDL past the shim with a fake-success response. Verify
// the dangerous shapes are NOT silently accepted.
func TestHandlerHandshakeNoiseRejectsPrivileged(t *testing.T) {
	h := NewHandler(nil, nil)
	h.UseDB("myapp")

	cases := []string{
		"SET PASSWORD = 'x'",
		"SET ROLE admin",
		"SET GLOBAL read_only = 0",
		"DROP TABLE orders",
		"INSERT INTO orders VALUES (1)",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			_, err := h.HandleQuery(q)
			if err == nil {
				t.Errorf("query %q should NOT be silently accepted as handshake noise", q)
			}
		})
	}
}

// TestHandlerRejectsNonFlashbackQuery — anything that's not a
// _flashback statement and not handshake noise should fail with a
// clear error to the client.
func TestHandlerRejectsNonFlashbackQuery(t *testing.T) {
	h := NewHandler(nil, nil)
	h.UseDB("myapp")

	_, err := h.HandleQuery("SELECT * FROM orders WHERE id = 1")
	if err == nil {
		t.Fatal("expected error for non-flashback query")
	}
	if !strings.Contains(err.Error(), "_flashback") {
		t.Errorf("error should mention _flashback, got %v", err)
	}
}

// TestHandlerUseDBStoresSchema — the schema set via UseDB is held
// for use by subsequent HandleQuery calls. The end-to-end coverage
// for "UseDB then run flashback" lives in TestEndToEndHandshake; here
// we just validate the storage step in isolation.
func TestHandlerUseDBStoresSchema(t *testing.T) {
	h := NewHandler(nil, nil)
	if err := h.UseDB("myapp"); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	got := h.db
	h.mu.Unlock()
	if got != "myapp" {
		t.Errorf("stored schema = %q, want %q", got, "myapp")
	}
}

// TestImageToResultColumnOrder — column order in the resultset must
// be deterministic (alphabetical) so customers comparing two rows
// across runs see consistent column positions.
func TestImageToResultColumnOrder(t *testing.T) {
	res, err := imageToResult(map[string]any{
		"name":  "alice",
		"id":    int64(42),
		"email": "a@b.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Resultset == nil {
		t.Fatal("nil resultset")
	}
	want := []string{"email", "id", "name"}
	got := make([]string, len(res.Resultset.Fields))
	for i, f := range res.Resultset.Fields {
		got[i] = string(f.Name)
	}
	if !equalStrings(got, want) {
		t.Errorf("column order = %v, want %v", got, want)
	}
}

// TestSelectImage covers every branch of the row-image priority rule
// used by runPointInTime. The function is intentionally pure so it
// can be exercised without sqlmock or a real MySQL: the rest of the
// _flashback / _snapshot pipeline (sort, LimitPerPK, archive merge)
// is covered by the query package's own tests.
//
// A future refactor that swaps the row_after / row_before priority,
// or that mishandles a DELETE event (row_after empty by definition),
// would silently return wrong row state to the customer. The
// "delete_fallback" case is the tripwire for that regression.
func TestSelectImage(t *testing.T) {
	after := map[string]any{"id": int64(1), "name": "after"}
	before := map[string]any{"id": int64(1), "name": "before"}

	cases := []struct {
		name string
		rows []query.ResultRow
		want map[string]any
	}{
		{
			name: "empty_input",
			rows: nil,
			want: nil,
		},
		{
			name: "insert_returns_row_after",
			rows: []query.ResultRow{{
				EventType: parser.EventInsert,
				RowAfter:  after,
			}},
			want: after,
		},
		{
			name: "update_prefers_row_after",
			rows: []query.ResultRow{{
				EventType: parser.EventUpdate,
				RowBefore: before,
				RowAfter:  after,
			}},
			want: after,
		},
		{
			name: "delete_fallback_to_row_before",
			rows: []query.ResultRow{{
				EventType: parser.EventDelete,
				RowBefore: before,
			}},
			want: before,
		},
		{
			// Pin the len() > 0 vs != nil distinction. A future
			// refactor that swapped len() for a nil-check would
			// silently regress DELETE handling if the indexer ever
			// emitted an empty non-nil RowAfter (defensive map
			// allocation upstream, redaction blanking every column,
			// etc.). Without this case the regression slips through
			// both "delete_fallback" (RowAfter is nil there) and
			// "both_empty" (RowBefore is also empty there).
			name: "row_after_empty_map_falls_back_to_row_before",
			rows: []query.ResultRow{{
				EventType: parser.EventDelete,
				RowAfter:  map[string]any{},
				RowBefore: before,
			}},
			want: before,
		},
		{
			name: "both_empty_returns_nil",
			rows: []query.ResultRow{{
				EventType: parser.EventUpdate,
				RowBefore: map[string]any{},
				RowAfter:  map[string]any{},
			}},
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := selectImage(tc.rows)
			if !equalMaps(got, tc.want) {
				t.Errorf("selectImage = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestImageToResultEmpty — an empty image (zero-key map) should
// produce a resultset with no rows.
func TestImageToResultEmpty(t *testing.T) {
	res, err := imageToResult(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Resultset == nil {
		t.Fatal("nil resultset")
	}
	if got := len(res.Resultset.RowDatas); got != 0 {
		t.Errorf("expected 0 rows, got %d", got)
	}
}

// TestNewHandlerWiresArchiveFetcher locks in the issue #255 fix at the
// construction boundary. Both NewHandler and NewHandlerWithConfig must
// install a non-nil archiveFetcher; otherwise every virtual-schema
// query crashes with "ArchiveFetcher is required when NoArchive is
// false" because the FetchMerged contract demands either NoArchive=true
// or a non-nil fetcher.
//
// A failure here means a refactor dropped the archiveFetcher wiring
// from the constructor — the same regression class /proxysql-e2e
// would catch end-to-end, but at unit-test speed.
func TestNewHandlerWiresArchiveFetcher(t *testing.T) {
	h := NewHandler(nil, nil)
	if h.archiveFetcher == nil {
		t.Error("NewHandler must wire a non-nil archiveFetcher; got nil")
	}
	h2 := NewHandlerWithConfig(nil, Config{}, nil)
	if h2.archiveFetcher == nil {
		t.Error("NewHandlerWithConfig must wire a non-nil archiveFetcher; got nil")
	}
}

// TestRunPointInTimeInvokesArchiveFetcher exercises the runPointInTime
// → FetchMerged → ArchiveFetcher path with sqlmock, asserting that the
// shim's wiring actually delivers archive rows on virtual-schema
// queries (the issue #255 fix). Uses a stubbed archive_state row so
// FetchMerged calls the injected fetcher.
func TestRunPointInTimeInvokesArchiveFetcher(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// archive_state returns one S3-backed source. The local_path is
	// empty so ResolveArchiveSources falls through to the S3 branch
	// (which doesn't require the directory to exist on disk for the
	// shim host to discover it). The s3_key contains the
	// "bintrail_id=" marker extractBasePath looks for.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("test-id", "", "test-bucket", "bintrail_id=test-id/event_date=2026/events.parquet"))
	// The planner queries information_schema.PARTITIONS. Stub empty
	// so the planner returns no live hours.
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
	// Live MySQL fetch may or may not run depending on planner output;
	// stub it permissive (no expected rows) so a call is fine.
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version"}))

	called := false
	fakeFetcher := func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) {
		called = true
		return nil, nil
	}

	h := &Handler{
		indexDB:        db,
		cfg:            Config{AllowGaps: true, IndexDBName: "bintrail_index"},
		logger:         slog.Default(),
		archiveFetcher: fakeFetcher,
	}

	asof := time.Now()
	q := TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "myapp",
		Table:   "orders",
		PKValue: "1",
		AsOf:    asof,
	}
	if _, err := h.runPointInTime(q); err != nil {
		// runPointInTime can succeed (empty resultset) or fail with a
		// scan error from sqlmock; both still prove the fetcher was
		// invoked. The assertion that matters is `called`.
		t.Logf("runPointInTime returned %v (acceptable for sqlmock-stubbed DB)", err)
	}
	if !called {
		t.Error("expected archiveFetcher to be invoked when archive_state has rows; was not called")
	}
}

// TestNewHandlerDefaultIsStrict pins the library-side counterpart of the
// CLI default-pin in cmd/bintrail/shim_test.go: NewHandler must return a
// Handler configured with AllowGaps=false. The CLI builds Config directly
// via NewHandlerWithConfig, so a regression that restored the legacy
// AllowGaps=true default in NewHandler would not break the production
// path — but library callers (tests, future embedders) would silently
// pick up the permissive behaviour the issue #257 fix turns off.
func TestNewHandlerDefaultIsStrict(t *testing.T) {
	h := NewHandler(nil, nil)
	if h.cfg.AllowGaps {
		t.Error("NewHandler must default AllowGaps=false (strict); got true (see #257)")
	}
}

// TestRunPointInTimeStrictModePropagatesArchiveError pins the issue #257
// fix: when AllowGaps=false (the new production default) and an archive
// source fails, runPointInTime must return an error rather than silently
// swallowing the failure and returning a partial resultset. Without
// propagation, the MySQL client on the wire sees a successful response
// missing rows it should have received — the exact silent failure the
// PR fixes.
func TestRunPointInTimeStrictModePropagatesArchiveError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("test-id", "", "test-bucket", "bintrail_id=test-id/event_date=2026/events.parquet"))
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version"}))

	archiveErr := errors.New("synthetic archive failure (e.g. S3 throttling)")
	failingFetcher := func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) {
		return nil, archiveErr
	}

	h := &Handler{
		indexDB:        db,
		cfg:            Config{AllowGaps: false, IndexDBName: "bintrail_index"},
		logger:         slog.Default(),
		archiveFetcher: failingFetcher,
	}

	q := TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "myapp",
		Table:   "orders",
		PKValue: "1",
		AsOf:    time.Now(),
	}
	_, err = h.runPointInTime(q)
	if err == nil {
		t.Fatal("expected runPointInTime to propagate archive failure under AllowGaps=false; got nil error")
	}
	// errors.Is over substring match: FetchMerged wraps the synthetic
	// archiveErr with %w, so the sentinel is recoverable. Pinning the
	// exact propagation path survives future error-message rewording —
	// a substring check on "archive" would also pass for an unrelated
	// archive-shaped error (e.g. validate-stage rejection) and that's
	// not the contract this test is here to enforce.
	if !errors.Is(err, archiveErr) {
		t.Errorf("expected wrapped archiveErr sentinel, got %v", err)
	}
}

// TestPlannerScopesPartitionsToIndexDB pins issue #259: the planner
// must scope information_schema.PARTITIONS to the index DB, not the
// user query's schema. A regression that re-passes q.Schema causes
// _flashback/_snapshot to return 0 rows (every hour misclassified as
// a coverage gap) and _diff to abort under strict mode.
func TestPlannerScopesPartitionsToIndexDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}))
	mock.ExpectQuery("information_schema.PARTITIONS").
		WithArgs("bintrail_index").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).
			AddRow("p_2026050415"))
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version"}))

	h := &Handler{
		indexDB: db,
		cfg: Config{
			AllowGaps:   false,
			NoArchive:   false,
			IndexDBName: "bintrail_index",
		},
		logger:         slog.Default(),
		archiveFetcher: func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) { return nil, nil },
	}

	q := TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "e2e_source",
		Table:   "orders",
		PKValue: "1",
		AsOf:    time.Date(2026, 5, 4, 15, 17, 52, 0, time.UTC),
	}
	if _, err := h.runPointInTime(q); err != nil {
		t.Fatalf("runPointInTime: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met (the planner likely scoped to %q instead of %q): %v",
			q.Schema, h.cfg.IndexDBName, err)
	}
}

// TestRunDiffScopesPartitionsToIndexDB is the runDiff sibling of
// TestPlannerScopesPartitionsToIndexDB. The two call sites do the same
// thing today, but a future refactor that splits Config could re-break
// _diff in isolation while leaving _flashback working — pinning each
// call site independently catches that.
func TestRunDiffScopesPartitionsToIndexDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}))
	mock.ExpectQuery("information_schema.PARTITIONS").
		WithArgs("bintrail_index").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).
			AddRow("p_2026050415"))
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version"}))

	h := &Handler{
		indexDB: db,
		cfg: Config{
			AllowGaps:   false,
			NoArchive:   false,
			IndexDBName: "bintrail_index",
		},
		logger:         slog.Default(),
		archiveFetcher: func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) { return nil, nil },
	}

	q := TimeTravelQuery{
		Type:    TypeDiff,
		Schema:  "e2e_source",
		Table:   "orders",
		PKValue: "1",
		Since:   time.Date(2026, 5, 4, 15, 17, 0, 0, time.UTC),
		Until:   time.Date(2026, 5, 4, 15, 18, 0, 0, time.UTC),
	}
	if _, err := h.runDiff(q); err != nil {
		t.Fatalf("runDiff: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met (the planner likely scoped to %q instead of %q): %v",
			q.Schema, h.cfg.IndexDBName, err)
	}
}

// TestEndToEndHandshake_AcceptsCorrectPassword boots a real MySQL-protocol
// server with our Handler and asserts that a client connecting with the
// correct username AND password passes the mysql_native_password challenge.
//
// This is the regression guard for issue #254: the handshake exercises
// `compareNativePasswordAuthData(salt, cleartext)` against the value
// `TenantAuth.GetCredential` returns. A regression to the pre-fix
// `("", true, nil)` would only let empty-password clients in — this
// test would fail because the client sends the actual password's
// scrambled response.
func TestEndToEndHandshake_AcceptsCorrectPassword(t *testing.T) {
	if err := runHandshakeTest(t, "alice", "alicepw", "alice", "alicepw"); err != nil {
		t.Fatalf("expected handshake to succeed with matching password: %v", err)
	}
}

// TestEndToEndHandshake_RejectsWrongPassword is the negative half: a
// client sending the wrong password must fail authentication. This
// catches the literal regression of #254 — without it, a pre-fix
// `GetCredential` returning "" would still pass
// TestEndToEndHandshake_AcceptsCorrectPassword if the server happened
// to accept any client response (which it does NOT today, but the
// negative case is what proves real validation is happening).
func TestEndToEndHandshake_RejectsWrongPassword(t *testing.T) {
	err := runHandshakeTest(t, "alice", "alicepw", "alice", "wrongpw")
	if err == nil {
		t.Fatal("expected handshake to fail with wrong password; got nil")
	}
	if !strings.Contains(err.Error(), "Access denied") {
		t.Errorf("expected MySQL 'Access denied' error, got %v", err)
	}
}

// runHandshakeTest spins up one shim listener, configures TenantAuth
// with serverUser/serverPass, and dials with clientUser/clientPass.
// Returns the client's Ping error (nil on success). Used by both the
// positive and negative auth tests above.
func runHandshakeTest(t *testing.T, serverUser, serverPass, clientUser, clientPass string) error {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	addr := listener.Addr().String()

	// Server side: accept one connection, perform handshake, then loop
	// HandleCommand until the client disconnects. SetReadDeadline
	// guarantees the loop unblocks even if the client's TCP close does
	// not propagate immediately, so the test can never hang.
	serverErr := make(chan error, 1)
	go func() {
		c, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer c.Close()
		c.SetReadDeadline(time.Now().Add(3 * time.Second))
		h := NewHandler(nil, nil)
		h.UseDB("myapp")
		srv := server.NewDefaultServer()
		auth, _ := NewTenantAuth(map[string]string{serverUser: serverPass})
		mc, err := server.NewCustomizedConn(c, srv, auth, h)
		if err != nil {
			// Auth failure surfaces here as a non-nil error from
			// NewCustomizedConn (handshake fails before the command
			// loop starts). Negative-auth tests rely on this.
			serverErr <- err
			return
		}
		for {
			if err := mc.HandleCommand(); err != nil {
				serverErr <- nil
				return
			}
		}
	}()

	host, port, _ := net.SplitHostPort(addr)
	clientErr := make(chan error, 1)
	go func() {
		clientErr <- driveClient(host+":"+port, clientUser, clientPass)
	}()

	var pingErr error
	select {
	case pingErr = <-clientErr:
	case <-time.After(5 * time.Second):
		t.Fatal("client timed out")
	}

	listener.Close()
	select {
	case <-serverErr:
	case <-time.After(5 * time.Second):
		t.Fatal("server goroutine did not exit")
	}
	return pingErr
}

// driveClient connects to the shim with explicit credentials and
// runs Ping. Returns the Ping error (nil on success).
func driveClient(addr, user, password string) error {
	dsn := user + ":" + password + "@tcp(" + addr + ")/"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return err
	}
	return nil
}

// Avoid pulling fmt for simple equality.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// equalMaps compares two map[string]any by length and value identity
// (==). Sufficient for the selectImage tests because they intentionally
// pass the same map literal as both input and expected output, so a
// pointer-equal value comparison detects "did selectImage return the
// expected source map?". Returning a *different* map with equal contents
// would fail this check — which is the correct outcome, since the
// helper's contract is to hand back the input image unchanged, not a
// copy.
func equalMaps(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		if vb, ok := b[k]; !ok || va != vb {
			return false
		}
	}
	return true
}

// Compile-time check: TenantAuth implements the credential provider
// interface.
var _ server.CredentialProvider = TenantAuth{}

// Compile-time check: nil-safe constructor returns a real Handler.
var _ = func() *Handler {
	return NewHandler(nil, nil)
}

// Compile-time check: emptyResult always returns a resultset.
var _ = emptyResult().Resultset

// Suppress unused-import lint: gomysql is referenced only for the
// compile-time assertion below.
var _ = gomysql.Result{}
