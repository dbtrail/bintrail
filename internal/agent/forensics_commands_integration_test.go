//go:build integration

package agent

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/forensics"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// integrationForensicsHandler returns a DefaultHandler wired to the shared
// test MySQL server, the way `bintrail agent --source-dsn ...` wires it.
// Forensics inspects server-global state, so it connects to the server
// itself rather than a per-test database.
func integrationForensicsHandler(t *testing.T) *DefaultHandler {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	db, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatalf("connect to test MySQL: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return &DefaultHandler{SourceDB: db}
}

// TestIntegrationDispatch_forensicsCapabilities round-trips a
// forensics_capabilities command through dispatch against the live server
// and checks the response marshals to the SaaS wire shape.
func TestIntegrationDispatch_forensicsCapabilities(t *testing.T) {
	h := integrationForensicsHandler(t)
	ctx := t.Context()

	resp := dispatch(ctx, h, Command{ID: "int-caps", Type: "forensics_capabilities"})
	if resp.Error != "" {
		t.Fatalf("dispatch error: %s", resp.Error)
	}
	caps, ok := resp.Data.(forensics.Capabilities)
	if !ok {
		t.Fatalf("Data type = %T, want forensics.Capabilities", resp.Data)
	}
	if caps.ServerInfo.Version == "" {
		t.Error("ServerInfo.Version is empty on a live server")
	}
	validVariants := []string{"mysql", "percona", "mariadb"}
	if !slices.Contains(validVariants, caps.ServerInfo.Variant) {
		t.Errorf("ServerInfo.Variant = %q, want one of %v", caps.ServerInfo.Variant, validVariants)
	}

	// The channel serializes the Response with encoding/json — prove the
	// full envelope survives that and carries the contract field names.
	wire, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	for _, key := range []string{`"id":"int-caps"`, `"performance_schema"`, `"audit_log"`, `"server_info"`} {
		if !strings.Contains(string(wire), key) {
			t.Errorf("wire response missing %s: %s", key, wire)
		}
	}
}

// TestIntegrationDispatch_forensicsEnrich enriches this test's own pinned
// connection through the dispatch path, plus a ghost ID that must come back
// in not_found with fallback queries.
func TestIntegrationDispatch_forensicsEnrich(t *testing.T) {
	h := integrationForensicsHandler(t)
	ctx := t.Context()

	caps, err := h.HandleForensicsCapabilities(ctx)
	if err != nil {
		t.Fatalf("HandleForensicsCapabilities: %v", err)
	}
	if !caps.PerformanceSchema.Enabled || !caps.PerformanceSchema.ThreadsAccessible {
		t.Skip("performance_schema threads not accessible on the test server")
	}

	// Pin a dedicated connection so its session stays visible in
	// performance_schema.threads while the enrichment (on other pool
	// connections) runs.
	conn, err := h.SourceDB.Conn(ctx)
	if err != nil {
		t.Fatalf("pin connection: %v", err)
	}
	defer conn.Close()
	var myID int64
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&myID); err != nil {
		t.Fatalf("CONNECTION_ID(): %v", err)
	}

	const ghostID int64 = 999999999
	payload, err := json.Marshal(ForensicsEnrichRequest{ThreadIDs: []int64{myID, ghostID}})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	resp := dispatch(ctx, h, Command{ID: "int-enrich", Type: "forensics_enrich", Data: payload})
	if resp.Error != "" {
		t.Fatalf("dispatch error: %s", resp.Error)
	}
	res, ok := resp.Data.(forensics.EnrichResult)
	if !ok {
		t.Fatalf("Data type = %T, want forensics.EnrichResult", resp.Data)
	}

	ti, ok := res.Threads[fmt.Sprintf("%d", myID)]
	if !ok {
		t.Fatalf("own connection %d not found in %v", myID, res.Threads)
	}
	if ti.ConnectionID != myID {
		t.Errorf("ConnectionID = %d, want %d", ti.ConnectionID, myID)
	}
	if ti.User == "" {
		t.Error("User is empty for a live session")
	}
	if !slices.Contains(res.NotFound, ghostID) {
		t.Errorf("ghost ID missing from NotFound: %v", res.NotFound)
	}
	if len(res.FallbackQueries) == 0 {
		t.Error("expected fallback queries for the ghost ID")
	}

	wire, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	for _, key := range []string{`"threads"`, fmt.Sprintf(`"connection_id":%d`, myID), `"not_found"`} {
		if !strings.Contains(string(wire), key) {
			t.Errorf("wire response missing %s: %s", key, wire)
		}
	}
}

// TestIntegrationDispatch_forensicsUsers lists accounts on the live server
// through dispatch; the test user itself must be present.
func TestIntegrationDispatch_forensicsUsers(t *testing.T) {
	h := integrationForensicsHandler(t)

	resp := dispatch(t.Context(), h, Command{ID: "int-users", Type: "forensics_users"})
	if resp.Error != "" {
		t.Fatalf("dispatch error: %s", resp.Error)
	}
	res, ok := resp.Data.(ForensicsUsersResult)
	if !ok {
		t.Fatalf("Data type = %T, want ForensicsUsersResult", resp.Data)
	}
	if len(res.Users) == 0 {
		t.Fatal("no users returned from a live server")
	}
	if !slices.Contains(res.Users, "root") {
		t.Errorf("expected 'root' among users, got %v", res.Users)
	}
}
