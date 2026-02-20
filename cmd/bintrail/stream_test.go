package main

import (
	"testing"
)

// ─── parseSourceDSN ──────────────────────────────────────────────────────────

func TestParseSourceDSN_tcp(t *testing.T) {
	dsn := "root:secret@tcp(db.example.com:3306)/mydb"
	host, port, user, pass, err := parseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "db.example.com" {
		t.Errorf("host: expected db.example.com, got %q", host)
	}
	if port != 3306 {
		t.Errorf("port: expected 3306, got %d", port)
	}
	if user != "root" {
		t.Errorf("user: expected root, got %q", user)
	}
	if pass != "secret" {
		t.Errorf("password: expected secret, got %q", pass)
	}
}

func TestParseSourceDSN_noPassword(t *testing.T) {
	dsn := "repl@tcp(127.0.0.1:13306)/"
	host, port, user, pass, err := parseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "127.0.0.1" {
		t.Errorf("host: expected 127.0.0.1, got %q", host)
	}
	if port != 13306 {
		t.Errorf("port: expected 13306, got %d", port)
	}
	if user != "repl" {
		t.Errorf("user: expected repl, got %q", user)
	}
	if pass != "" {
		t.Errorf("password: expected empty, got %q", pass)
	}
}

func TestParseSourceDSN_unixSocket(t *testing.T) {
	dsn := "root@unix(/var/run/mysqld/mysqld.sock)/test"
	_, _, _, _, err := parseSourceDSN(dsn)
	if err == nil {
		t.Error("expected error for unix socket DSN, got nil")
	}
}

func TestParseSourceDSN_invalid(t *testing.T) {
	_, _, _, _, err := parseSourceDSN("not-a-valid-dsn::::")
	if err == nil {
		t.Error("expected error for invalid DSN, got nil")
	}
}

// ─── resolveStart ────────────────────────────────────────────────────────────

func TestResolveStart_noStateNoFlags(t *testing.T) {
	_, _, _, _, _, err := resolveStart("", "", 4, nil)
	if err == nil {
		t.Error("expected error when no flags and no saved state")
	}
}

func TestResolveStart_positionFlagsNoState(t *testing.T) {
	mode, file, gtidStr, pos, accGTID, err := resolveStart("binlog.000001", "", 4, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position, got %q", mode)
	}
	if file != "binlog.000001" {
		t.Errorf("expected file=binlog.000001, got %q", file)
	}
	if pos != 4 {
		t.Errorf("expected pos=4, got %d", pos)
	}
	if gtidStr != "" {
		t.Errorf("expected empty gtidStr, got %q", gtidStr)
	}
	if accGTID != nil {
		t.Error("expected nil accGTID in position mode")
	}
}

func TestResolveStart_flagsOverrideSavedState(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000010",
		binlogPos:  9999,
	}
	// Providing --start-file should override the saved state.
	mode, file, _, _, _, err := resolveStart("binlog.000020", "", 100, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position, got %q", mode)
	}
	if file != "binlog.000020" {
		t.Errorf("expected file=binlog.000020, got %q", file)
	}
}

func TestResolveStart_resumePosition(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000005",
		binlogPos:  1234,
	}
	mode, file, _, pos, accGTID, err := resolveStart("", "", 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position, got %q", mode)
	}
	if file != "binlog.000005" {
		t.Errorf("expected file=binlog.000005, got %q", file)
	}
	if pos != 1234 {
		t.Errorf("expected pos=1234, got %d", pos)
	}
	if accGTID != nil {
		t.Error("expected nil accGTID in position mode")
	}
}

func TestResolveStart_resumeGTID(t *testing.T) {
	gtidSet := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100"
	saved := &streamState{
		mode:    "gtid",
		gtidSet: gtidSet,
	}
	mode, _, returnedGTID, _, accGTID, err := resolveStart("", "", 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid, got %q", mode)
	}
	if returnedGTID != gtidSet {
		t.Errorf("expected GTID=%q, got %q", gtidSet, returnedGTID)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID in gtid mode")
	}
}

func TestResolveStart_mutuallyExclusive(t *testing.T) {
	_, _, _, _, _, err := resolveStart("binlog.000001", "uuid:1", 4, nil)
	if err == nil {
		t.Error("expected error for mutually exclusive --start-file and --start-gtid")
	}
}

func TestResolveStart_gtidFlagsNoState(t *testing.T) {
	gtidSet := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
	mode, _, returnedGTID, _, accGTID, err := resolveStart("", gtidSet, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid, got %q", mode)
	}
	if returnedGTID != gtidSet {
		t.Errorf("expected GTID=%q, got %q", gtidSet, returnedGTID)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID")
	}
}
