package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/bintrail/internal/byos"
)

const (
	testDSN  = "repluser:secret@tcp(10.0.0.5:3306)/"
	testUUID = "11111111-2222-3333-4444-555555555555"
	altUUID  = "66666666-7777-8888-9999-aaaaaaaaaaaa"
)

func newIdentPtr(ident byos.SourceIdentity) *atomic.Pointer[byos.SourceIdentity] {
	p := &atomic.Pointer[byos.SourceIdentity]{}
	p.Store(&ident)
	return p
}

func TestCheckSourceIdentity_Stable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnRows(
		sqlmock.NewRows([]string{"@@server_uuid"}).AddRow(testUUID))

	prev := newIdentPtr(byos.SourceIdentity{ServerUUID: testUUID, Host: "10.0.0.5", Port: 3306, User: "repluser"})

	if err := checkSourceIdentity(context.Background(), db, testDSN, prev); err != nil {
		t.Fatalf("unexpected error on stable identity: %v", err)
	}
	if got := prev.Load().ServerUUID; got != testUUID {
		t.Errorf("ServerUUID after stable check = %q, want %q", got, testUUID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCheckSourceIdentity_ChangeDetected(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnRows(
		sqlmock.NewRows([]string{"@@server_uuid"}).AddRow(altUUID))

	prev := newIdentPtr(byos.SourceIdentity{ServerUUID: testUUID, Host: "10.0.0.5", Port: 3306, User: "repluser"})

	err = checkSourceIdentity(context.Background(), db, testDSN, prev)
	if err == nil {
		t.Fatal("expected error on UUID change, got nil")
	}
	if !strings.Contains(err.Error(), testUUID) || !strings.Contains(err.Error(), altUUID) {
		t.Errorf("error should mention both UUIDs; got: %v", err)
	}
	if !strings.Contains(err.Error(), "identity changed") {
		t.Errorf("error should contain 'identity changed'; got: %v", err)
	}
	// Pointer should not be updated with the new UUID — the operator must
	// restart the agent, and downstream records in the current process
	// should continue using the last known-good identity.
	if got := prev.Load().ServerUUID; got != testUUID {
		t.Errorf("ServerUUID after change detection = %q, want previous %q (not overwritten)", got, testUUID)
	}
}

func TestCheckSourceIdentity_TransientError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnError(errors.New("connection reset by peer"))

	prev := newIdentPtr(byos.SourceIdentity{ServerUUID: testUUID, Host: "10.0.0.5", Port: 3306, User: "repluser"})

	if err := checkSourceIdentity(context.Background(), db, testDSN, prev); err != nil {
		t.Fatalf("transient DB error should not abort the stream; got: %v", err)
	}
	if got := prev.Load().ServerUUID; got != testUUID {
		t.Errorf("ServerUUID after transient error = %q, want previous %q", got, testUUID)
	}
}

func TestCheckSourceIdentity_HostUpdateStable(t *testing.T) {
	// UUID unchanged but host/port/user shift (e.g. a CNAME move). The
	// captured identity must refresh so metadata records reflect the new
	// connection attributes on the next flush.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnRows(
		sqlmock.NewRows([]string{"@@server_uuid"}).AddRow(testUUID))

	prev := newIdentPtr(byos.SourceIdentity{ServerUUID: testUUID, Host: "old-host", Port: 3306, User: "repluser"})

	newDSN := "repluser:secret@tcp(new-host:3306)/"
	if err := checkSourceIdentity(context.Background(), db, newDSN, prev); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := prev.Load().Host; got != "new-host" {
		t.Errorf("Host after CNAME update = %q, want new-host", got)
	}
}
