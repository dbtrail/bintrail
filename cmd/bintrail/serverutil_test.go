package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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

	ident, err := loadSourceIdentity(context.Background(), db, "repluser:secret@tcp(10.0.0.5:3306)/")
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

func TestLoadSourceIdentityServerUUIDQueryFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnError(errors.New("connection refused"))

	_, err = loadSourceIdentity(context.Background(), db, "u:p@tcp(h:3306)/")
	if err == nil {
		t.Fatal("expected error when @@server_uuid query fails")
	}
	if !strings.Contains(err.Error(), "server_uuid") {
		t.Errorf("error %q should mention server_uuid", err)
	}
}

func TestLoadSourceIdentityBadDSN(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnRows(
		sqlmock.NewRows([]string{"@@server_uuid"}).AddRow("uuid-x"))

	// parseSourceDSN rejects unix sockets — use that as the easy bad-DSN trigger.
	_, err = loadSourceIdentity(context.Background(), db, "u:p@unix(/tmp/sock)/")
	if err == nil {
		t.Fatal("expected error for unix-socket DSN")
	}
	if !strings.Contains(err.Error(), "unix socket") {
		t.Errorf("error %q should mention unix socket", err)
	}
}
