package main

import (
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
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

// TestParseSourceDSN_ipv6 verifies IPv6 addresses are parsed correctly.
func TestParseSourceDSN_ipv6(t *testing.T) {
	dsn := "root:pw@tcp([::1]:3306)/db"
	host, port, _, _, err := parseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error for IPv6 DSN: %v", err)
	}
	if host != "::1" {
		t.Errorf("host: expected ::1, got %q", host)
	}
	if port != 3306 {
		t.Errorf("port: expected 3306, got %d", port)
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

// TestResolveStart_invalidSavedGTID verifies that a corrupt gtid_set in
// stream_state results in a clear error (not a panic).
func TestResolveStart_invalidSavedGTID(t *testing.T) {
	saved := &streamState{mode: "gtid", gtidSet: "not-a-valid-gtid"}
	_, _, _, _, _, err := resolveStart("", "", 4, saved)
	if err == nil {
		t.Error("expected error for invalid saved GTID set")
	}
}

// TestResolveStart_invalidStartGTIDFlag verifies that --start-gtid with an
// invalid GTID string is rejected with a clear error.
func TestResolveStart_invalidStartGTIDFlag(t *testing.T) {
	_, _, _, _, _, err := resolveStart("", "garbage-gtid", 0, nil)
	if err == nil {
		t.Error("expected error for invalid --start-gtid value")
	}
}

// ─── GTID accumulation ────────────────────────────────────────────────────────

// TestStreamState_gtidAccumulation verifies that accGTID.Update correctly
// accumulates multiple GTIDs from a single server UUID into a range.
func TestStreamState_gtidAccumulation(t *testing.T) {
	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562" // go-mysql lowercases UUIDs
	gs, err := gomysql.ParseMysqlGTIDSet(uuid + ":1")
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet: %v", err)
	}
	acc := gs.(*gomysql.MysqlGTIDSet)

	for _, gtid := range []string{uuid + ":2", uuid + ":3", uuid + ":4"} {
		if err := acc.Update(gtid); err != nil {
			t.Fatalf("Update(%q): %v", gtid, err)
		}
	}

	got := acc.String()
	// Should contain the UUID and a range covering 1-4.
	if !strings.Contains(got, uuid) {
		t.Errorf("expected UUID in GTID set string, got %q", got)
	}
	if !strings.Contains(got, "1-4") {
		t.Errorf("expected range 1-4 in GTID set string, got %q", got)
	}
}

// TestStreamState_gtidAccumulationMultiServer verifies accumulation across
// two different server UUIDs — each gets its own range entry.
func TestStreamState_gtidAccumulationMultiServer(t *testing.T) {
	uuid1 := "3e11fa47-71ca-11e1-9e33-c80aa9429562" // go-mysql lowercases UUIDs
	uuid2 := "7d93a8e1-0b3c-11e2-ab3d-0022114ef123"

	gs, _ := gomysql.ParseMysqlGTIDSet(uuid1 + ":1")
	acc := gs.(*gomysql.MysqlGTIDSet)

	if err := acc.Update(uuid2 + ":1"); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := acc.Update(uuid1 + ":2"); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got := acc.String()
	if !strings.Contains(got, uuid1) {
		t.Errorf("expected %s in result, got %q", uuid1, got)
	}
	if !strings.Contains(got, uuid2) {
		t.Errorf("expected %s in result, got %q", uuid2, got)
	}
}

// ─── cobra command wiring ─────────────────────────────────────────────────────

// TestStreamCmd_registered verifies that streamCmd is wired into the root command.
func TestStreamCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "stream" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'stream' command to be registered under rootCmd")
	}
}

// TestStreamCmd_requiredFlags verifies that the three required flags are marked
// as required so cobra enforces them before RunE is called.
func TestStreamCmd_requiredFlags(t *testing.T) {
	for _, flagName := range []string{"index-dsn", "source-dsn", "server-id"} {
		ann := streamCmd.Annotations
		_ = ann
		flag := streamCmd.Flag(flagName)
		if flag == nil {
			t.Errorf("flag --%s not registered", flagName)
			continue
		}
		// cobra marks required flags in the Annotations map.
		if streamCmd.Flag(flagName).Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("flag --%s is not marked required", flagName)
		}
	}
}

// TestStreamCmd_defaults verifies that optional flags have the expected defaults.
func TestStreamCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"batch-size", "1000"},
		{"checkpoint", "10"},
		{"start-pos", "4"},
	}
	for _, tc := range cases {
		f := streamCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}
