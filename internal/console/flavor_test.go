package console

import (
	"net/url"
	"strings"
	"testing"
)

func TestNormalizeFlavor(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"", FlavorMySQL, false},
		{"mysql", FlavorMySQL, false},
		{"MySQL", FlavorMySQL, false},
		{"  mysql  ", FlavorMySQL, false},
		{"mariadb", FlavorMariaDB, false},
		{"postgres", FlavorPostgres, false},
		{"postgresql", FlavorPostgres, false},
		{"POSTGRES", FlavorPostgres, false},
		{"oracle", "", true},
		{"pg", "", true},
	}
	for _, tc := range cases {
		got, err := NormalizeFlavor(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("NormalizeFlavor(%q): expected error, got %q", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("NormalizeFlavor(%q): unexpected error %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("NormalizeFlavor(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestServerEntrySourceFlavor(t *testing.T) {
	if got := (ServerEntry{}).SourceFlavor(); got != FlavorMySQL {
		t.Errorf("blank flavor should default to mysql, got %q", got)
	}
	if got := (ServerEntry{Flavor: "postgres"}).SourceFlavor(); got != FlavorPostgres {
		t.Errorf("postgres flavor = %q", got)
	}
	if !(ServerEntry{Flavor: "postgres"}).IsPostgres() {
		t.Error("IsPostgres should be true for postgres")
	}
	if (ServerEntry{Flavor: "mysql"}).IsPostgres() {
		t.Error("IsPostgres should be false for mysql")
	}
	// A hand-edited junk value degrades to mysql rather than crashing.
	if got := (ServerEntry{Flavor: "nonsense"}).SourceFlavor(); got != FlavorMySQL {
		t.Errorf("junk flavor should degrade to mysql, got %q", got)
	}
}

func TestPGReplDSN(t *testing.T) {
	// URL with no existing query → replication=database is added.
	got, err := PGReplDSN("postgres://u:p@h:5432/appdb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q := mustQuery(t, got).Get("replication"); q != "database" {
		t.Errorf("replication param = %q, want database (%q)", q, got)
	}

	// Existing query params are preserved.
	got, err = PGReplDSN("postgres://u:p@h:5432/appdb?sslmode=require")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	qv := mustQuery(t, got)
	if qv.Get("replication") != "database" || qv.Get("sslmode") != "require" {
		t.Errorf("expected both replication and sslmode, got %q", got)
	}

	// A DSN that already carries a replication param is rejected (would double it).
	if _, err := PGReplDSN("postgres://u:p@h:5432/appdb?replication=database"); err == nil {
		t.Error("expected error for a DSN already carrying replication")
	}
}

func TestBuildPGSourceDSN(t *testing.T) {
	// Raw postgres:// DSN is stored verbatim after validation.
	got, err := buildPGSourceDSN(serverRequest{SourceDSN: strPtr("postgres://u:p@h:5432/appdb")}, "")
	if err != nil {
		t.Fatalf("raw valid: %v", err)
	}
	if got != "postgres://u:p@h:5432/appdb" {
		t.Errorf("raw should be verbatim, got %q", got)
	}

	// Explicit clear.
	if got, err := buildPGSourceDSN(serverRequest{SourceDSN: strPtr("")}, "postgres://x/y"); err != nil || got != "" {
		t.Errorf("clear: got %q err %v", got, err)
	}

	// Errors: raw missing dbname, raw with replication, raw+structured password,
	// non-postgres scheme, structured missing dbname.
	badRaw := []serverRequest{
		{SourceDSN: strPtr("postgres://u:p@h:5432/")},
		{SourceDSN: strPtr("postgres://u:p@h:5432/appdb?replication=database")},
		{SourceDSN: strPtr("postgres://u:p@h:5432/appdb"), SourcePassword: strPtr("x")},
		{SourceDSN: strPtr("mysql://u:p@h/appdb")},
		{SourceHost: "h", SourceUser: "u"}, // structured, no database
	}
	for i, req := range badRaw {
		if _, err := buildPGSourceDSN(req, ""); err == nil {
			t.Errorf("badRaw[%d]: expected error", i)
		}
	}

	// Structured build → canonical postgres:// URL, default port 5432.
	got, err = buildPGSourceDSN(serverRequest{SourceHost: "h", SourceUser: "u", SourcePassword: strPtr("p"), SourceDatabase: "appdb"}, "")
	if err != nil {
		t.Fatalf("structured: %v", err)
	}
	u := mustParse(t, got)
	if u.Scheme != "postgres" || u.Hostname() != "h" || u.Port() != "5432" || strings.TrimPrefix(u.Path, "/") != "appdb" {
		t.Errorf("structured build wrong: %q", got)
	}
	if pw, _ := u.User.Password(); pw != "p" || u.User.Username() != "u" {
		t.Errorf("structured creds wrong: %q", got)
	}

	// Keep-stored: all-empty request returns the stored DSN unchanged.
	if got, err := buildPGSourceDSN(serverRequest{}, "postgres://u:p@h:5432/appdb"); err != nil || got != "postgres://u:p@h:5432/appdb" {
		t.Errorf("keep-stored: got %q err %v", got, err)
	}

	// Merge: host-only edit keeps the stored port, password, and query params.
	got, err = buildPGSourceDSN(serverRequest{SourceHost: "newhost"}, "postgres://u:secret@h:6000/appdb?sslmode=require")
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	u = mustParse(t, got)
	if u.Hostname() != "newhost" || u.Port() != "6000" {
		t.Errorf("merge host/port wrong: %q", got)
	}
	if pw, _ := u.User.Password(); pw != "secret" {
		t.Errorf("merge should keep stored password: %q", got)
	}
	if u.Query().Get("sslmode") != "require" {
		t.Errorf("merge should preserve query params: %q", got)
	}
}

func TestValidatePGSourceMonitorConfig(t *testing.T) {
	// PG source with a DSN but no slot/publication is rejected.
	if err := validatePGSourceMonitorConfig(FlavorPostgres, "postgres://x/y", "", "pub"); err == nil {
		t.Error("expected error: PG monitored source without a slot")
	}
	if err := validatePGSourceMonitorConfig(FlavorPostgres, "postgres://x/y", "slot", ""); err == nil {
		t.Error("expected error: PG monitored source without a publication")
	}
	// PG source with both is fine.
	if err := validatePGSourceMonitorConfig(FlavorPostgres, "postgres://x/y", "slot", "pub"); err != nil {
		t.Errorf("PG with slot+pub should pass: %v", err)
	}
	// Index-only PG entry (no source DSN) needs neither.
	if err := validatePGSourceMonitorConfig(FlavorPostgres, "", "", ""); err != nil {
		t.Errorf("index-only PG should pass: %v", err)
	}
	// MySQL never triggers the gate.
	if err := validatePGSourceMonitorConfig(FlavorMySQL, "u:p@tcp(h:3306)/", "", ""); err != nil {
		t.Errorf("mysql should pass: %v", err)
	}
}

func TestFillPGSourceDSNParts(t *testing.T) {
	var dto serverDTO
	fillPGSourceDSNParts(&dto, "postgres://repl:secret@h:5432/appdb")
	if !dto.HasSource || dto.SourceHost != "h" || dto.SourcePort != "5432" ||
		dto.SourceUser != "repl" || dto.SourceDatabase != "appdb" || !dto.HasSourcePassword {
		t.Errorf("decompose wrong: %+v", dto)
	}
	// No password → HasSourcePassword false.
	var dto2 serverDTO
	fillPGSourceDSNParts(&dto2, "postgres://repl@h:5432/appdb")
	if dto2.HasSourcePassword {
		t.Error("no-password DSN should report HasSourcePassword=false")
	}
	// The raw DSN never leaks into any DTO field.
	if strings.Contains(dto.SourceHost+dto.SourceUser+dto.SourceDatabase, "secret") {
		t.Error("password leaked into DTO parts")
	}
}

// TestPGSourceDSN_specialCharsRoundTrip drives a password carrying URL-significant
// characters (@ : / — the classic DSN-corruption case) through the full path a
// real source takes: structured build → stored query DSN → PGReplDSN (which
// re-runs url.String() + q.Encode()) and → fillPGSourceDSNParts (the DTO decode).
// url.UserPassword/u.User.Password() percent-encode/decode asymmetries are exactly
// where a mis-encode would corrupt the live connection or the DTO echo.
func TestPGSourceDSN_specialCharsRoundTrip(t *testing.T) {
	const pw = "p@ss:w/rd"
	stored, err := buildPGSourceDSN(serverRequest{
		SourceHost: "pg.prod", SourceUser: "repl", SourcePassword: strPtr(pw), SourceDatabase: "appdb",
	}, "")
	if err != nil {
		t.Fatalf("structured build: %v", err)
	}
	// The stored query DSN must decode back to the exact password.
	if got, _ := mustParse(t, stored).User.Password(); got != pw {
		t.Errorf("stored password did not round-trip: got %q want %q (dsn %q)", got, pw, stored)
	}
	// The derived replication DSN re-encodes the whole URL — creds must survive it.
	repl, err := PGReplDSN(stored)
	if err != nil {
		t.Fatalf("PGReplDSN: %v", err)
	}
	ru := mustParse(t, repl)
	if got, _ := ru.User.Password(); got != pw {
		t.Errorf("repl password did not round-trip: got %q want %q (dsn %q)", got, pw, repl)
	}
	if ru.Query().Get("replication") != "database" {
		t.Errorf("repl DSN missing replication=database: %q", repl)
	}
	// The DTO decode exposes host/user/db but never a raw password substring.
	var dto serverDTO
	fillPGSourceDSNParts(&dto, stored)
	if dto.SourceUser != "repl" || dto.SourceHost != "pg.prod" || dto.SourceDatabase != "appdb" || !dto.HasSourcePassword {
		t.Errorf("DTO decode wrong with special chars: %+v", dto)
	}
	if strings.Contains(dto.SourceHost+dto.SourceUser+dto.SourceDatabase+dto.SourcePort, "p@ss") {
		t.Error("raw password substring leaked into a DTO part")
	}
}

func TestBuildSourceDSNDispatch(t *testing.T) {
	// buildSourceDSN routes postgres to the PG builder and mysql/mariadb to the
	// MySQL builder (dbname-less server-level DSN).
	pg, err := buildSourceDSN(serverRequest{SourceHost: "h", SourceUser: "u", SourcePassword: strPtr("p"), SourceDatabase: "db"}, "", FlavorPostgres)
	if err != nil || !strings.HasPrefix(pg, "postgres://") {
		t.Errorf("postgres dispatch: got %q err %v", pg, err)
	}
	my, err := buildSourceDSN(serverRequest{SourceHost: "h", SourceUser: "u"}, "", FlavorMariaDB)
	if err != nil || strings.HasPrefix(my, "postgres://") {
		t.Errorf("mariadb should route through MySQL builder: got %q err %v", my, err)
	}
}

func mustParse(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return u
}

func mustQuery(t *testing.T, s string) url.Values {
	t.Helper()
	return mustParse(t, s).Query()
}
