package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/bintrail/internal/buffer"
	"github.com/dbtrail/bintrail/internal/byos"
	"github.com/dbtrail/bintrail/internal/parser"
)

const (
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
	failures := 0

	if err := checkSourceIdentity(context.Background(), db, prev, &failures); err != nil {
		t.Fatalf("unexpected error on stable identity: %v", err)
	}
	if failures != 0 {
		t.Errorf("failures = %d, want 0 on success", failures)
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
	failures := 0

	err = checkSourceIdentity(context.Background(), db, prev, &failures)
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
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCheckSourceIdentity_TransientErrorTolerated(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnError(errors.New("connection reset by peer"))

	prev := newIdentPtr(byos.SourceIdentity{ServerUUID: testUUID})
	failures := 0

	if err := checkSourceIdentity(context.Background(), db, prev, &failures); err != nil {
		t.Fatalf("transient DB error should not abort at first failure; got: %v", err)
	}
	if failures != 1 {
		t.Errorf("failures = %d, want 1 after one failed call", failures)
	}
	if got := prev.Load().ServerUUID; got != testUUID {
		t.Errorf("ServerUUID after transient error = %q, want previous %q", got, testUUID)
	}
}

func TestCheckSourceIdentity_BoundedRetryAborts(t *testing.T) {
	// After maxIdentityFailures consecutive failures, the helper should
	// abort the stream instead of logging WARN forever (as would happen
	// under revoked credentials or a dropped user).
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Expect exactly maxIdentityFailures failing queries.
	for range maxIdentityFailures {
		mock.ExpectQuery("SELECT @@server_uuid").WillReturnError(errors.New("access denied"))
	}

	prev := newIdentPtr(byos.SourceIdentity{ServerUUID: testUUID})
	failures := 0

	var lastErr error
	for i := 0; i < maxIdentityFailures; i++ {
		lastErr = checkSourceIdentity(context.Background(), db, prev, &failures)
		if i < maxIdentityFailures-1 && lastErr != nil {
			t.Fatalf("checkSourceIdentity aborted too early at iteration %d: %v", i, lastErr)
		}
	}
	if lastErr == nil {
		t.Fatal("expected error after maxIdentityFailures consecutive failures, got nil")
	}
	if !strings.Contains(lastErr.Error(), "consecutive times") {
		t.Errorf("error should mention consecutive failures; got: %v", lastErr)
	}
	if !strings.Contains(lastErr.Error(), "access denied") {
		t.Errorf("error should wrap the underlying DB error; got: %v", lastErr)
	}
}

func TestCheckSourceIdentity_ContextCanceledSilent(t *testing.T) {
	// On shutdown the ctx is canceled mid-query. That should not be
	// counted as a failure and should not log a spurious warning.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnError(context.Canceled)

	prev := newIdentPtr(byos.SourceIdentity{ServerUUID: testUUID})
	failures := 0

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := checkSourceIdentity(ctx, db, prev, &failures); err != nil {
		t.Fatalf("canceled ctx should not produce an error; got: %v", err)
	}
	if failures != 0 {
		t.Errorf("canceled ctx should not increment failures; got %d", failures)
	}
}

func TestCheckSourceIdentity_NilPointerIsBug(t *testing.T) {
	// prev is a non-nil atomic pointer with no value stored. This is a
	// programmer error (the agent always seeds the identity at startup);
	// the helper must refuse to proceed rather than silently stamping
	// empty metadata.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@server_uuid").WillReturnRows(
		sqlmock.NewRows([]string{"@@server_uuid"}).AddRow(testUUID))

	prev := &atomic.Pointer[byos.SourceIdentity]{} // no Store
	failures := 0

	err = checkSourceIdentity(context.Background(), db, prev, &failures)
	if err == nil {
		t.Fatal("expected BUG error for uninitialized pointer, got nil")
	}
	if !strings.Contains(err.Error(), "BUG") {
		t.Errorf("error should flag the programmer error; got: %v", err)
	}
}

// TestByosStreamLoop_IdentityChangeAbortsLoop exercises the full integration
// path: the identityTicker arm in byosStreamLoop must invoke
// checkSourceIdentity, drain any pending batch to the buffer before
// returning, and surface the error to the caller.
func TestByosStreamLoop_IdentityChangeAbortsLoop(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// First tick returns a different UUID → the loop should abort.
	mock.ExpectQuery("SELECT @@server_uuid").WillReturnRows(
		sqlmock.NewRows([]string{"@@server_uuid"}).AddRow(altUUID))

	buf := buffer.New(buffer.Config{})
	events := make(chan parser.Event, 4)
	// Push an event so we can assert the final flushBatch drained it.
	events <- parser.Event{
		Schema:    "mydb",
		Table:     "t",
		EventType: parser.EventInsert,
		Timestamp: time.Now().UTC(),
		PKValues:  "1",
	}

	fc := &byosFlushConfig{
		serverID:         "srv-A",
		sourceIdent:      newIdentPtr(byos.SourceIdentity{ServerUUID: testUUID}),
		sourceDB:         db,
		identityInterval: 10 * time.Millisecond,
		flushInterval:    time.Hour, // disable flush-ticker so only identity-ticker fires
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- byosStreamLoop(ctx, events, buf, 100, fc)
	}()

	select {
	case got := <-errCh:
		if got == nil {
			t.Fatal("byosStreamLoop returned nil; want identity-change error")
		}
		if !strings.Contains(got.Error(), "identity changed") {
			t.Errorf("returned error = %v, want one containing 'identity changed'", got)
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatal("byosStreamLoop did not return within deadline after identity change")
	}

	// The pending event must have been drained by the final flushBatch
	// before the loop returned — otherwise the operator loses events
	// captured after the last regular flush tick.
	if got := buf.Len(); got != 1 {
		t.Errorf("buffer len after abort = %d, want 1 (pending event drained)", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
