package cliapp

import (
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// TestAgentCmdSourceFlavorFlag pins the flag registration and its default:
// every pre-existing agent invocation (no flag, no env) must keep streaming
// with the MySQL flavor. Mirrors TestStreamCmd_sourceFlavorDefault.
func TestAgentCmdSourceFlavorFlag(t *testing.T) {
	f := agentCmd.Flag("source-flavor")
	if f == nil {
		t.Fatal("flag --source-flavor not registered on agentCmd")
	}
	if f.DefValue != "mysql" {
		t.Errorf("expected default source-flavor=mysql, got %q", f.DefValue)
	}
}

func TestNormalizeAgentFlavor(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "", want: gomysql.MySQLFlavor},
		{in: "mysql", want: gomysql.MySQLFlavor},
		{in: "mariadb", want: gomysql.MariaDBFlavor},
		// The BYOS stream is a binlog reader; postgres is a different capturer.
		{in: "postgres", wantErr: true},
		// go-mysql's flavor literals are lowercase; a case-mismatch must fail
		// loudly here, not surface as a cryptic syncer handshake error.
		{in: "MySQL", wantErr: true},
		{in: "percona", wantErr: true},
	}
	for _, tc := range tests {
		got, err := normalizeAgentFlavor(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("normalizeAgentFlavor(%q) = %q, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("normalizeAgentFlavor(%q) error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("normalizeAgentFlavor(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestBYOSSyncerConfigFlavor is the wiring test for the syncer config:
// hardwire "mysql" back into byosSyncerConfig (or drop its MariaDB branch)
// and a case here fails.
func TestBYOSSyncerConfigFlavor(t *testing.T) {
	my := byosSyncerConfig(42, gomysql.MySQLFlavor, "src-host", 3306, "u", "pw")
	if my.Flavor != gomysql.MySQLFlavor {
		t.Errorf("mysql config Flavor = %q, want %q", my.Flavor, gomysql.MySQLFlavor)
	}
	if my.DumpCommandFlag&replication.BINLOG_SEND_ANNOTATE_ROWS_EVENT != 0 {
		t.Error("mysql config must not set the MariaDB ANNOTATE dump flag")
	}
	if my.FillZeroLogPos {
		t.Error("FillZeroLogPos is the MariaDB 11.4+ compensation (#1117); the mysql config keeps it off (streamrun parity)")
	}
	if my.ServerID != 42 || my.Host != "src-host" || my.Port != 3306 || my.User != "u" || my.Password != "pw" {
		t.Errorf("connection fields not carried through: %+v", my)
	}

	ma := byosSyncerConfig(42, gomysql.MariaDBFlavor, "src-host", 3306, "u", "pw")
	if ma.Flavor != gomysql.MariaDBFlavor {
		t.Errorf("mariadb config Flavor = %q, want %q (a hardwired mysql flavor makes the syncer parse MariaDB GTID events as MySQL's)", ma.Flavor, gomysql.MariaDBFlavor)
	}
	if ma.DumpCommandFlag&replication.BINLOG_SEND_ANNOTATE_ROWS_EVENT == 0 {
		t.Error("mariadb config must request ANNOTATE_ROWS (#699): MariaDB only forwards them to a replica that set the dump flag")
	}
	if !ma.FillZeroLogPos {
		t.Error("mariadb config must set FillZeroLogPos (#1117 zero-LogPos compensation)")
	}
}

// TestParseBYOSStartGTIDFlavor pins that --start-gtid is parsed with the
// configured flavor: before the flavor flag, the parse was hardwired to
// "mysql" and a MariaDB GTID set was unusable.
func TestParseBYOSStartGTIDFlavor(t *testing.T) {
	if _, err := parseBYOSStartGTID(gomysql.MariaDBFlavor, "0-2-71"); err != nil {
		t.Errorf("mariadb flavor rejected a MariaDB GTID set: %v", err)
	}
	if _, err := parseBYOSStartGTID(gomysql.MySQLFlavor, "0-2-71"); err == nil {
		t.Error("mysql flavor accepted a MariaDB GTID set — the flavor is not reaching the parser")
	}
	// Lowercase UUID on purpose: go-mysql lowercases GTID UUIDs.
	if _, err := parseBYOSStartGTID(gomysql.MySQLFlavor, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"); err != nil {
		t.Errorf("mysql flavor rejected a MySQL GTID set: %v", err)
	}
	if _, err := parseBYOSStartGTID(gomysql.MySQLFlavor, "not-a-gtid"); err == nil {
		t.Error("garbage GTID set parsed without error")
	} else if !strings.Contains(err.Error(), "parse start GTID set") {
		t.Errorf("want the wrapped parse error, got %v", err)
	}
}
