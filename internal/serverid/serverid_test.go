package serverid

import (
	"fmt"
	"testing"
)

func makeServer(bintrailID, serverUUID, host string, port uint16, username string) Server {
	return Server{
		BintrailID: bintrailID,
		ServerUUID: serverUUID,
		Host:       host,
		Port:       port,
		Username:   username,
	}
}

func TestResolveIdentity_Rule1_ExactMatch(t *testing.T) {
	servers := []Server{
		makeServer("bt-1", "uuid-1", "db01", 3306, "bintrail"),
	}
	matched, rule := resolveIdentity(servers, "uuid-1", "db01", 3306, "bintrail")
	if rule != resNoChange {
		t.Fatalf("expected resNoChange, got %d", rule)
	}
	if matched == nil || matched.BintrailID != "bt-1" {
		t.Errorf("expected matched bt-1, got %v", matched)
	}
}

func TestResolveIdentity_Rule2_Migration(t *testing.T) {
	// UUID matches, host changed (server migrated to new host).
	servers := []Server{
		makeServer("bt-1", "uuid-1", "db01", 3306, "bintrail"),
	}
	matched, rule := resolveIdentity(servers, "uuid-1", "db02", 3306, "bintrail")
	if rule != resMigration {
		t.Fatalf("expected resMigration, got %d", rule)
	}
	if matched == nil || matched.BintrailID != "bt-1" {
		t.Errorf("expected matched bt-1, got %v", matched)
	}
}

func TestResolveIdentity_Rule2_Migration_PortAndUser(t *testing.T) {
	// UUID matches, port and username both changed.
	servers := []Server{
		makeServer("bt-1", "uuid-1", "db01", 3306, "bintrail"),
	}
	matched, rule := resolveIdentity(servers, "uuid-1", "db01", 3307, "bintrail_ro")
	if rule != resMigration {
		t.Fatalf("expected resMigration, got %d", rule)
	}
	if matched == nil || matched.BintrailID != "bt-1" {
		t.Errorf("expected matched bt-1, got %v", matched)
	}
}

func TestResolveIdentity_Rule3_UUIDRegen(t *testing.T) {
	// host+port+user match, UUID changed (auto.cnf deleted and MySQL restarted).
	servers := []Server{
		makeServer("bt-1", "uuid-1", "db01", 3306, "bintrail"),
	}
	matched, rule := resolveIdentity(servers, "uuid-new", "db01", 3306, "bintrail")
	if rule != resUUIDRegen {
		t.Fatalf("expected resUUIDRegen, got %d", rule)
	}
	if matched == nil || matched.BintrailID != "bt-1" {
		t.Errorf("expected matched bt-1, got %v", matched)
	}
}

func TestResolveIdentity_Rule4_NewServer(t *testing.T) {
	// Neither UUID nor host+port+user match any existing record.
	servers := []Server{
		makeServer("bt-1", "uuid-1", "db01", 3306, "bintrail"),
	}
	matched, rule := resolveIdentity(servers, "uuid-9", "db99", 3307, "root")
	if rule != resNew {
		t.Fatalf("expected resNew, got %d", rule)
	}
	if matched != nil {
		t.Error("expected nil matched for new server")
	}
}

func TestResolveIdentity_Rule4_EmptyList(t *testing.T) {
	// No servers registered yet — always a new server.
	matched, rule := resolveIdentity(nil, "uuid-1", "db01", 3306, "bintrail")
	if rule != resNew {
		t.Fatalf("expected resNew, got %d", rule)
	}
	if matched != nil {
		t.Error("expected nil matched")
	}
}

func TestResolveIdentity_Rule5_Conflict(t *testing.T) {
	// UUID matches bt-1 but host+port+user match bt-2 — cloned server.
	servers := []Server{
		makeServer("bt-1", "uuid-1", "db01", 3306, "bintrail"),
		makeServer("bt-2", "uuid-2", "db02", 3306, "bintrail"),
	}
	// Present with uuid-1 (belongs to bt-1) from host db02:3306/bintrail (belongs to bt-2).
	matched, rule := resolveIdentity(servers, "uuid-1", "db02", 3306, "bintrail")
	if rule != resConflict {
		t.Fatalf("expected resConflict, got %d", rule)
	}
	if matched != nil {
		t.Error("expected nil matched for conflict")
	}
}

func TestResolveIdentity_UsernameScope(t *testing.T) {
	// Two servers share the same host+port but use different usernames.
	// Presenting with clientA's credentials should only match clientA's record.
	servers := []Server{
		makeServer("bt-1", "uuid-1", "localhost", 3306, "bintrail_clientA"),
		makeServer("bt-2", "uuid-2", "localhost", 3306, "bintrail_clientB"),
	}
	matched, rule := resolveIdentity(servers, "uuid-99", "localhost", 3306, "bintrail_clientA")
	if rule != resUUIDRegen {
		t.Fatalf("expected resUUIDRegen for clientA UUID regen, got %d", rule)
	}
	if matched == nil || matched.BintrailID != "bt-1" {
		t.Errorf("expected bt-1, got %v", matched)
	}
}
func TestDeriveServerID(t *testing.T) {
	const dsn = "user:pass@tcp(source.example.com:3306)/mydb"
	id1, err := DeriveServerID(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	id2, err := DeriveServerID(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id1 != id2 {
		t.Errorf("DeriveServerID is not deterministic: %d vs %d", id1, id2)
	}

	// Range invariant: must be >= 100M to keep distance from the 1-1000 zone
	// most production replicas use. The PR-review caught a typo that broke
	// this — keep the assertion strict.
	if id1 < 100000000 {
		t.Errorf("DeriveServerID produced %d, expected >= 100000000", id1)
	}

	// Distinct DSNs produce distinct IDs. With 100 generated inputs the
	// probability of any collision in a 4.2B range is < 10^-15 — this catches
	// regressions like a stub returning a constant, or a hash truncation bug
	// that compresses into a 16-bit space.
	seen := make(map[uint32]string, 100)
	for i := range 100 {
		gen := fmt.Sprintf("u:p@tcp(host%d.example.com:3306)/db", i)
		id, err := DeriveServerID(gen)
		if err != nil {
			t.Fatalf("DeriveServerID(%q) error: %v", gen, err)
		}
		if id < 100000000 {
			t.Errorf("DeriveServerID(%q) = %d, below floor 100000000", gen, id)
		}
		if prev, dup := seen[id]; dup {
			t.Errorf("collision: %q and %q both produced %d", prev, gen, id)
		}
		seen[id] = gen
	}

	// Bad DSN returns an error rather than silently substituting a
	// non-deterministic value (the silent-failure fix).
	if _, err := DeriveServerID("not-a-dsn"); err == nil {
		t.Error("expected error for unparseable DSN; got nil")
	}
}
