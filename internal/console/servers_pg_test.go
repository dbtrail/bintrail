package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestHandleServersCreate_postgres drives the +Add server handler end-to-end for
// a PostgreSQL source: the DTO echoes flavor + the decomposed non-secret source
// parts (slot/publication/database), the source password never leaks, and the
// stored source DSN is a postgres:// QUERY DSN (no replication param — that is
// derived at monitor/baseline time).
func TestHandleServersCreate_postgres(t *testing.T) {
	srv := newRegistryServer(t)
	const srcPW = "srcpw-should-not-leak"
	body := `{"name":"pgprod","host":"idx","user":"bt","password":"ipw","dbname":"binlog_index",` +
		`"flavor":"postgres","source_host":"pg.prod","source_port":"5432","source_user":"repl",` +
		`"source_password":"` + srcPW + `","source_database":"appdb","source_slot":"bt_slot","source_publication":"bt_pub"}`
	rec, resp := doServersReq(t, srv, "POST", "/api/servers", body)
	if rec.Code != 201 {
		t.Fatalf("pg create code=%d body=%s", rec.Code, resp)
	}
	if strings.Contains(string(resp), srcPW) {
		t.Fatal("create response leaked the source password")
	}
	var dto serverDTO
	if err := json.Unmarshal(resp, &dto); err != nil {
		t.Fatal(err)
	}
	if dto.Flavor != FlavorPostgres {
		t.Errorf("flavor = %q, want postgres", dto.Flavor)
	}
	if dto.SourceHost != "pg.prod" || dto.SourcePort != "5432" || dto.SourceUser != "repl" || dto.SourceDatabase != "appdb" {
		t.Errorf("decomposed source parts wrong: %+v", dto)
	}
	if dto.SourceSlot != "bt_slot" || dto.SourcePublication != "bt_pub" {
		t.Errorf("slot/publication wrong: slot=%q pub=%q", dto.SourceSlot, dto.SourcePublication)
	}
	if !dto.HasSourcePassword {
		t.Error("has_source_password should be true")
	}
	e, ok := srv.cm.reg.Get(dto.ID)
	if !ok {
		t.Fatal("entry not stored")
	}
	if !strings.HasPrefix(e.SourceDSN, "postgres://") || strings.Contains(e.SourceDSN, "replication=") {
		t.Errorf("stored source DSN must be a postgres:// query DSN with no replication param, got %q", e.SourceDSN)
	}
}

// TestHandleServersCreate_postgresRequiresSlotPublication: a monitorable PG
// source (a source DSN configured) with no slot/publication is a clear 400 —
// capture cannot start without them. Missing either half fails (symmetric on the
// slot/publication ||), and — the stronger contract — no incomplete entry is
// persisted: a rejected create must not leave a half-written PG source behind.
func TestHandleServersCreate_postgresRequiresSlotPublication(t *testing.T) {
	srv := newRegistryServer(t)
	const base = `{"name":"pgnoslot","host":"idx","user":"bt","password":"ipw","dbname":"binlog_index",` +
		`"flavor":"postgres","source_host":"pg.prod","source_user":"repl","source_password":"p","source_database":"appdb"`
	for _, tc := range []struct{ name, extra string }{
		{"neither", `}`},
		{"slot only", `,"source_slot":"bt_slot"}`},
		{"publication only", `,"source_publication":"bt_pub"}`},
	} {
		rec, resp := doServersReq(t, srv, "POST", "/api/servers", base+tc.extra)
		if rec.Code != 400 {
			t.Fatalf("%s: code=%d, want 400 (body=%s)", tc.name, rec.Code, resp)
		}
	}
	if n := srv.cm.reg.Len(); n != 0 {
		t.Fatalf("a rejected PG create must not persist an entry, registry has %d", n)
	}
}

// TestHandleServersUpdate_flavorImmutable: flavor is fixed at create (the capture
// engine, per-source index DB layout, and stream_state are all keyed to it). A
// PUT changing it is refused with a clear 400.
func TestHandleServersUpdate_flavorImmutable(t *testing.T) {
	srv := newRegistryServer(t)
	rec, resp := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"m1","host":"idx","user":"bt","password":"ipw","dbname":"binlog_index"}`)
	if rec.Code != 201 {
		t.Fatalf("create code=%d body=%s", rec.Code, resp)
	}
	var dto serverDTO
	if err := json.Unmarshal(resp, &dto); err != nil {
		t.Fatal(err)
	}
	if dto.Flavor != FlavorMySQL {
		t.Fatalf("a flavor-less create should default to mysql, got %q", dto.Flavor)
	}
	rec, resp = doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID,
		`{"name":"m1","host":"idx","user":"bt","password":"ipw","dbname":"binlog_index","flavor":"postgres"}`)
	if rec.Code != 400 {
		t.Fatalf("flavor change: code=%d, want 400 (body=%s)", rec.Code, resp)
	}
	if !strings.Contains(string(resp), "flavor cannot be changed") {
		t.Errorf("want a clear immutability message, got: %s", resp)
	}
}
