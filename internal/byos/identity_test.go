package byos

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/serverid"
)

func TestLoadSourceIdentityHappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnRows(
		sqlmock.NewRows([]string{"@@server_uuid"}).
			AddRow("11111111-2222-3333-4444-555555555555"))

	ident, err := LoadSourceIdentity(context.Background(), db, "repluser:secret@tcp(10.0.0.5:3306)/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.ServerUUID != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("ServerUUID = %q", ident.ServerUUID)
	}
	if ident.Host != "10.0.0.5" || ident.Port != 3306 || ident.User != "repluser" {
		t.Errorf("identity = %+v, want host=10.0.0.5 port=3306 user=repluser", ident)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestLoadSourceIdentityServerUUIDQueryFails confirms that on a MySQL source
// (VERSION() does not contain "MariaDB") a failed @@server_uuid query is still
// surfaced as an error — never silently replaced by a synthesized anchor. The
// synthesis path is reserved for genuine MariaDB sources (next test).
func TestLoadSourceIdentityServerUUIDQueryFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnError(errors.New("connection refused"))
	// On failure, LoadSourceIdentity probes VERSION() to disambiguate MariaDB
	// (no @@server_uuid, expected) from a real MySQL failure. A MySQL version
	// string must keep the error propagating.
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.36"))

	_, err = LoadSourceIdentity(context.Background(), db, "u:p@tcp(h:3306)/")
	if err == nil {
		t.Fatal("expected error when @@server_uuid query fails on a MySQL source")
	}
	if !strings.Contains(err.Error(), "server_uuid") {
		t.Errorf("error %q should mention server_uuid", err)
	}
	// Pin that the VERSION() probe is actually issued on the failure path — if the
	// DetectFlavor guard were removed, this would catch it (unconsumed expectation).
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestLoadSourceIdentityUnknownFlavorPropagates covers the case where both
// @@server_uuid AND VERSION() fail, so DetectFlavor returns "" (flavor
// undeterminable). The original error MUST propagate — bintrail must never
// fabricate a synthesized identity for a server whose flavor it cannot confirm
// is MariaDB (a refactor of the guard to `== "mysql"` would regress exactly here).
func TestLoadSourceIdentityUnknownFlavorPropagates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnError(errors.New("connection reset"))
	mock.ExpectQuery("SELECT VERSION").WillReturnError(errors.New("connection reset")) // → DetectFlavor returns ""

	_, err = LoadSourceIdentity(context.Background(), db, "u:p@tcp(h:3306)/")
	if err == nil {
		t.Fatal("expected error when flavor is undeterminable (VERSION() also fails)")
	}
	if !strings.Contains(err.Error(), "server_uuid") {
		t.Errorf("error %q must propagate the original server_uuid failure, not synthesize", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestLoadSourceIdentityMariaDBSynthesizes verifies that a MariaDB source (no
// @@server_uuid; VERSION() contains "MariaDB") gets a stable synthesized anchor
// derived from its address rather than an error or an empty identity.
func TestLoadSourceIdentityMariaDBSynthesizes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnError(
		errors.New("Error 1193: Unknown system variable 'server_uuid'"))
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"VERSION()"}).AddRow("11.4.2-MariaDB-1:11.4.2+maria~ubu2404"))

	ident, err := LoadSourceIdentity(context.Background(), db, "repl:secret@tcp(10.0.0.7:3306)/")
	if err != nil {
		t.Fatalf("unexpected error for MariaDB source: %v", err)
	}
	want := serverid.SyntheticServerUUID("10.0.0.7", 3306)
	if ident.ServerUUID != want {
		t.Errorf("ServerUUID = %q, want synthesized %q", ident.ServerUUID, want)
	}
	if ident.ServerUUID == "" {
		t.Error("synthesized ServerUUID must not be empty")
	}
	if ident.Host != "10.0.0.7" || ident.Port != 3306 || ident.User != "repl" {
		t.Errorf("identity = %+v, want host=10.0.0.7 port=3306 user=repl", ident)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestLoadSourceIdentityBadDSN(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// The DSN is parsed before any query, so an invalid DSN fails fast with no
	// DB round-trip (no mock expectations needed). config.ParseSourceDSN rejects
	// unix sockets — use that as the easy bad-DSN trigger.
	_, err = LoadSourceIdentity(context.Background(), db, "u:p@unix(/tmp/sock)/")
	if err == nil {
		t.Fatal("expected error for unix-socket DSN")
	}
	if !strings.Contains(err.Error(), "unix socket") {
		t.Errorf("error %q should mention unix socket", err)
	}
}
