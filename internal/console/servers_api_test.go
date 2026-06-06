package console

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

const secretPW = "s3cr3t-hunter2"

// newRegistryServer builds a Server over a file-backed registry (no boot
// entry) with the full middleware chain, for exercising the /api/servers CRUD
// surface end to end.
func newRegistryServer(t *testing.T) *Server {
	t.Helper()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func doServersReq(t *testing.T, srv *Server, method, path, body string) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	var rdr *strings.Reader
	if body == "" {
		rdr = strings.NewReader("")
	} else {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, rdr)
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec, rec.Body.Bytes()
}

// TestServersAPINeverLeaksSecrets: list and get responses must not contain the
// password or any DSN string — the core masking invariant.
func TestServersAPINeverLeaksSecrets(t *testing.T) {
	srv := newRegistryServer(t)

	rec, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","host":"10.0.0.5","port":"3306","user":"forensics","password":"`+secretPW+`","dbname":"binlog_index"}`)
	if rec.Code != 201 {
		t.Fatalf("create code = %d, body = %s", rec.Code, body)
	}
	if strings.Contains(string(body), secretPW) {
		t.Fatalf("create response leaked the password: %s", body)
	}
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if !created.HasPassword {
		t.Error("has_password should be true")
	}
	if created.Host != "10.0.0.5" || created.User != "forensics" || created.DBName != "binlog_index" {
		t.Errorf("masked parts wrong: %+v", created)
	}

	for _, path := range []string{"/api/servers", "/api/servers/" + created.ID} {
		rec, body := doServersReq(t, srv, "GET", path, "")
		if rec.Code != 200 {
			t.Fatalf("%s code = %d", path, rec.Code)
		}
		if strings.Contains(string(body), secretPW) || strings.Contains(string(body), "@tcp(") {
			t.Errorf("%s leaked a secret or DSN string: %s", path, body)
		}
	}
}

// TestServersAPIKeepPassword: PUT with password omitted keeps the stored
// secret; "" clears it; a value replaces it. Asserted by re-parsing the STORED
// DSN, never via an echoed string (nothing echoes it).
func TestServersAPIKeepPassword(t *testing.T) {
	srv := newRegistryServer(t)
	_, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","host":"h","user":"u","password":"`+secretPW+`","dbname":"db"}`)
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	storedPW := func() string {
		e, ok := srv.cm.reg.Get(created.ID)
		if !ok {
			t.Fatal("entry vanished")
		}
		cfg, err := mysql.ParseDSN(e.DSN)
		if err != nil {
			t.Fatal(err)
		}
		return cfg.Passwd
	}

	// Omitted password → keep.
	if rec, b := doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod-renamed","host":"h","user":"u","dbname":"db"}`); rec.Code != 200 {
		t.Fatalf("rename code = %d, body = %s", rec.Code, b)
	}
	if pw := storedPW(); pw != secretPW {
		t.Errorf("omitted password must keep the stored one; got %q", pw)
	}

	// Explicit new value → replace.
	doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod-renamed","host":"h","user":"u","password":"new-pw","dbname":"db"}`)
	if pw := storedPW(); pw != "new-pw" {
		t.Errorf("explicit password must replace; got %q", pw)
	}

	// Explicit "" → clear.
	doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod-renamed","host":"h","user":"u","password":"","dbname":"db"}`)
	if pw := storedPW(); pw != "" {
		t.Errorf("empty-string password must clear; got %q", pw)
	}
}

func TestServersAPIValidation(t *testing.T) {
	srv := newRegistryServer(t)

	cases := []struct {
		name, body string
		wantCode   int
	}{
		{"missing dbname", `{"name":"x","host":"h","user":"u"}`, 400},
		{"missing host", `{"name":"x","user":"u","dbname":"db"}`, 400},
		{"missing user", `{"name":"x","host":"h","dbname":"db"}`, 400},
		{"missing name", `{"host":"h","user":"u","dbname":"db"}`, 400},
		{"dsn without dbname", `{"name":"x","dsn":"u:p@tcp(h:3306)/"}`, 400},
		{"invalid json", `{nope`, 400},
	}
	for _, tc := range cases {
		rec, body := doServersReq(t, srv, "POST", "/api/servers", tc.body)
		if rec.Code != tc.wantCode {
			t.Errorf("%s: code = %d, want %d (body=%s)", tc.name, rec.Code, tc.wantCode, body)
		}
	}

	// Duplicate name → 409.
	doServersReq(t, srv, "POST", "/api/servers", `{"name":"dup","host":"h","user":"u","dbname":"db"}`)
	rec, _ := doServersReq(t, srv, "POST", "/api/servers", `{"name":"dup","host":"h2","user":"u","dbname":"db"}`)
	if rec.Code != 409 {
		t.Errorf("duplicate name: code = %d, want 409", rec.Code)
	}

	// Unknown id → 404 on get/put/delete/test.
	for _, m := range []struct{ method, path string }{
		{"GET", "/api/servers/ffffffffffffffff"},
		{"PUT", "/api/servers/ffffffffffffffff"},
		{"DELETE", "/api/servers/ffffffffffffffff"},
		{"POST", "/api/servers/ffffffffffffffff/test"},
	} {
		rec, _ := doServersReq(t, srv, m.method, m.path, `{"name":"x","host":"h","user":"u","dbname":"db"}`)
		if rec.Code != 404 {
			t.Errorf("%s %s: code = %d, want 404", m.method, m.path, rec.Code)
		}
	}
}

// TestServersAPIEphemeralImmutable: the command-line (boot) entry shows up in
// the list but can never be edited or deleted.
func TestServersAPIEphemeralImmutable(t *testing.T) {
	db, mock, closeFn := newSQLMock(t)
	defer closeFn()
	_ = mock

	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		DB: db, DBName: "binlog_index", BootDSN: "cli:" + secretPW + "@tcp(127.0.0.1:3306)/binlog_index",
	})
	if err != nil {
		t.Fatal(err)
	}

	rec, body := doServersReq(t, srv, "GET", "/api/servers", "")
	if rec.Code != 200 {
		t.Fatalf("list code = %d", rec.Code)
	}
	if strings.Contains(string(body), secretPW) {
		t.Fatalf("boot DSN password leaked: %s", body)
	}
	var resp serversResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Servers) != 1 || resp.Servers[0].Kind != "ephemeral" || resp.Servers[0].ID != bootServerID {
		t.Fatalf("expected exactly the ephemeral boot entry, got %+v", resp.Servers)
	}
	if resp.DefaultID != bootServerID {
		t.Errorf("default_id = %q, want %q", resp.DefaultID, bootServerID)
	}
	if resp.Servers[0].Editable || resp.Servers[0].Deletable {
		t.Error("the boot entry must not be editable/deletable")
	}

	if rec, _ := doServersReq(t, srv, "PUT", "/api/servers/default",
		`{"name":"x","host":"h","user":"u","dbname":"db"}`); rec.Code != 409 {
		t.Errorf("edit boot entry: code = %d, want 409", rec.Code)
	}
	if rec, _ := doServersReq(t, srv, "DELETE", "/api/servers/default", ""); rec.Code != 409 {
		t.Errorf("delete boot entry: code = %d, want 409", rec.Code)
	}
}

// TestResolveHeader: the X-Bintrail-Server header picks the bundle; empty
// falls back to the default; an unknown id 404s; a configured-but-unreachable
// registry server fails on selection (502), not at boot.
func TestResolveHeader(t *testing.T) {
	db, _, closeFn := newSQLMock(t)
	defer closeFn()

	srv := newRegistryServer(t)
	srv.cm.boot = &bundle{db: db, dbName: "bootdb", noArchive: true}
	srv.cm.boot.engine = nil // never queried below; resolution is the subject

	// Empty header → boot.
	req := httptest.NewRequest("GET", "/api/events", nil)
	b, err := srv.resolve(req)
	if err != nil || b.dbName != "bootdb" {
		t.Fatalf("empty header: bundle=%v err=%v, want boot", b, err)
	}
	// Explicit boot id → boot.
	req.Header.Set(serverHeader, bootServerID)
	if b, err = srv.resolve(req); err != nil || b.dbName != "bootdb" {
		t.Fatalf("default id: bundle=%v err=%v, want boot", b, err)
	}
	// Unknown id → ErrUnknownServer (404 at the HTTP layer).
	req.Header.Set(serverHeader, "ffffffffffffffff")
	if _, err = srv.resolve(req); !errors.Is(err, ErrUnknownServer) {
		t.Fatalf("unknown id: err=%v, want ErrUnknownServer", err)
	}
	// A registry entry pointing at a dead host fails on SELECTION with 502 and
	// a scrubbed error.
	added, err := srv.cm.reg.Add(ServerEntry{Name: "dead", DSN: "u:" + secretPW + "@tcp(127.0.0.1:1)/db?timeout=200ms"})
	if err != nil {
		t.Fatal(err)
	}
	req2 := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/capabilities", nil)
	req2.Host = "127.0.0.1:8090"
	req2.Header.Set("Authorization", "Bearer t")
	req2.Header.Set(serverHeader, added.ID)
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req2)
	if rec2.Code != 502 {
		t.Fatalf("dead server selection: code = %d, want 502 (body=%s)", rec2.Code, rec2.Body.String())
	}
	if strings.Contains(rec2.Body.String(), secretPW) {
		t.Errorf("dead-server error leaked the password: %s", rec2.Body.String())
	}
}

// TestServersAPIRequiresToken: every server-management endpoint sits behind
// the bearer token — the write surface (a local file) must not be reachable
// by a cross-site request.
func TestServersAPIRequiresToken(t *testing.T) {
	srv := newRegistryServer(t)
	for _, m := range []struct{ method, path string }{
		{"GET", "/api/servers"},
		{"POST", "/api/servers"},
		{"PUT", "/api/servers/abc"},
		{"DELETE", "/api/servers/abc"},
		{"POST", "/api/servers/abc/test"},
		{"POST", "/api/servers/test"},
	} {
		req := httptest.NewRequest(m.method, "http://127.0.0.1:8090"+m.path, strings.NewReader("{}"))
		req.Host = "127.0.0.1:8090"
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != 401 {
			t.Errorf("%s %s without token: code = %d, want 401", m.method, m.path, rec.Code)
		}
	}
}

func TestScrubDSNError(t *testing.T) {
	dsn := "user:" + secretPW + "@tcp(10.0.0.5:3306)/binlog_index"
	err := errors.New("failed to ping MySQL: dial tcp: connect refused for " + dsn + " (password " + secretPW + ")")
	got := scrubDSNError(err, dsn)
	if strings.Contains(got, secretPW) {
		t.Errorf("scrubbed error still contains the password: %s", got)
	}
	if strings.Contains(got, dsn) {
		t.Errorf("scrubbed error still contains the DSN: %s", got)
	}
}

func TestBuildDSNMerge(t *testing.T) {
	stored := "olduser:" + secretPW + "@tcp(oldhost:3306)/olddb?tls=preferred"

	// Structured edit, password omitted: host/user/db change, password and
	// params survive.
	got, err := buildDSN(serverRequest{Name: "x", Host: "newhost", Port: "3307", User: "newuser", DBName: "newdb"}, stored)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := mysql.ParseDSN(got)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "newhost:3307" || cfg.User != "newuser" || cfg.DBName != "newdb" {
		t.Errorf("merge wrong: %+v", cfg)
	}
	if cfg.Passwd != secretPW {
		t.Errorf("password lost in merge: %q", cfg.Passwd)
	}
	if cfg.TLSConfig != "preferred" {
		t.Errorf("DSN params lost in merge: tls=%q", cfg.TLSConfig)
	}

	// Default port.
	got, err = buildDSN(serverRequest{Name: "x", Host: "h", User: "u", DBName: "db"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg, _ := mysql.ParseDSN(got); cfg.Addr != "h:3306" {
		t.Errorf("default port: addr = %q, want h:3306", cfg.Addr)
	}

	// Host-only edit keeps a stored NON-default port (the merge is symmetric:
	// a port-only edit keeps the host, a host-only edit keeps the port —
	// defaulting to 3306 here would silently rewrite :3307 connections).
	got, err = buildDSN(serverRequest{Name: "x", Host: "newhost"}, "u:p@tcp(db.internal:3307)/binlog_index")
	if err != nil {
		t.Fatal(err)
	}
	if cfg, _ := mysql.ParseDSN(got); cfg.Addr != "newhost:3307" {
		t.Errorf("host-only edit: addr = %q, want newhost:3307 (stored port preserved)", cfg.Addr)
	}

	// Port-only edit keeps the stored host (the pre-existing symmetric case).
	got, err = buildDSN(serverRequest{Name: "x", Port: "3308"}, "u:p@tcp(db.internal:3307)/binlog_index")
	if err != nil {
		t.Fatal(err)
	}
	if cfg, _ := mysql.ParseDSN(got); cfg.Addr != "db.internal:3308" {
		t.Errorf("port-only edit: addr = %q, want db.internal:3308 (stored host preserved)", cfg.Addr)
	}

	// Raw DSN wins over structured fields and must name a database.
	if _, err := buildDSN(serverRequest{DSN: "u:p@tcp(h:3306)/"}, ""); err == nil {
		t.Error("raw DSN without dbname must be rejected")
	}
	if got, err := buildDSN(serverRequest{DSN: "u:p@tcp(h:3306)/db", Host: "ignored"}, ""); err != nil || !strings.Contains(got, "@tcp(h:3306)/db") {
		t.Errorf("raw DSN should be used verbatim: %q err=%v", got, err)
	}
}

// TestResolveNoServers: an empty console (no boot entry, empty registry) must
// return the user-facing errNoServers, which the HTTP layer maps to 404.
func TestResolveNoServers(t *testing.T) {
	cm := newConnManager(nil, false)
	if _, err := cm.Resolve(t.Context(), ""); !errors.Is(err, errNoServers) {
		t.Fatalf("Resolve on empty console: err = %v, want errNoServers", err)
	}
}

// TestRegistryErrStatus locks the registry-error → HTTP status contract,
// including the string-matched 400 branches a refactor could silently break.
func TestRegistryErrStatus(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{ErrDuplicateName, 409},
		{ErrRegistryReadOnly, 409},
		{ErrUnknownServer, 404},
		{errors.New("server name is required"), 400},
		{errors.New(`"default" is reserved for the command-line server`), 400},
		{errors.New("disk exploded"), 500},
	}
	for _, tc := range cases {
		if got := registryErrStatus(tc.err); got != tc.want {
			t.Errorf("registryErrStatus(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

// TestBuildDSNRejectsRawDSNPlusPassword: a raw dsn carries its own password;
// accepting a structured password alongside would silently drop one of them.
func TestBuildDSNRejectsRawDSNPlusPassword(t *testing.T) {
	pw := "x"
	if _, err := buildDSN(serverRequest{DSN: "u:p@tcp(h:3306)/db", Password: &pw}, ""); err == nil {
		t.Fatal("dsn + structured password must be rejected, not silently merged")
	}
}

// TestCapabilityMatrix: the pure-config reconstruct gate over
// baseline × no_archive × profile.
func TestCapabilityMatrix(t *testing.T) {
	cases := []struct {
		name    string
		entry   ServerEntry
		profile bool
		want    bool
	}{
		{"baseline dir", ServerEntry{BaselineDir: "/b"}, false, true},
		{"baseline s3", ServerEntry{BaselineS3: "s3://b/"}, false, true},
		{"no baseline", ServerEntry{}, false, false},
		{"no_archive kills it", ServerEntry{BaselineDir: "/b", NoArchive: true}, false, false},
		{"profile kills it", ServerEntry{BaselineDir: "/b"}, true, false},
	}
	for _, tc := range cases {
		cm := newConnManager(nil, tc.profile)
		if got := cm.capability(tc.entry); got != tc.want {
			t.Errorf("%s: capability = %v, want %v", tc.name, got, tc.want)
		}
	}
}
