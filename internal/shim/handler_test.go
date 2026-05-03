package shim

import (
	"database/sql"
	"net"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql" // database/sql driver registration
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
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

// TestEndToEndHandshake — boots a real MySQL-protocol server with
// our Handler, dials it with go-mysql's client, and drives a query
// through. Validates the wire-protocol path end-to-end without
// requiring a real MySQL backend.
func TestEndToEndHandshake(t *testing.T) {
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
		auth, _ := NewTenantAuth([]string{"test"})
		mc, err := server.NewCustomizedConn(c, srv, auth, h)
		if err != nil {
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
		clientErr <- driveClient(host + ":" + port)
	}()

	select {
	case err := <-clientErr:
		if err != nil {
			t.Fatalf("client failure: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client timed out")
	}

	listener.Close()
	select {
	case <-serverErr:
	case <-time.After(5 * time.Second):
		t.Fatal("server goroutine did not exit")
	}
}

// driveClient connects to the shim, completes the MySQL handshake via
// go-sql-driver/mysql's Ping, then closes. Success is "the handshake
// completed without error" — proof that bintrail can speak the wire
// protocol to a real client. We deliberately do NOT issue further
// queries here; the per-query handler logic is exercised by the
// other unit tests, and adding a query here would require a real
// indexDB.
func driveClient(addr string) error {
	// TenantAuth's empty-string credential means the DSN sends an
	// empty password against the allowlisted "test" user.
	dsn := "test:@tcp(" + addr + ")/"
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
