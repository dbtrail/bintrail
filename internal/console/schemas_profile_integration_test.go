//go:build integration

package console

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The unit tests prove distinctSchemas skips the snapshot half when told the
// session is restricted. This proves the HANDLER tells it: GET /api/schemas is
// driven end to end against a real index, once unprofiled and once with a real
// profiled session, and the two listings must differ on exactly the
// archive-only schema (#1326).
//
// Mutating handleSchemas to pass false, or distinctSchemas to ignore the
// parameter, leaves the unit tests passing and breaks this one.
func TestIntegrationProfiledSessionSchemaDropdownIsLiveOnly(t *testing.T) {
	// A server that DOES read archives (NoArchive unset), so the only exclusion
	// under test is the session's own profile — with NoArchive:true the control
	// case would drop the archive-only schema for the server-wide reason and
	// prove nothing about profiles.
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	// One live schema...
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "users", 1 /*INSERT*/, "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))
	// ...and a snapshot that also knows a schema with NO live events — the
	// archive-only shape from the issue: it survives in Parquet and the latest
	// snapshot, but a profiled (archive-excluded) read can never reach it.
	// Inserted BEFORE New: the resolver is loaded once when the bundle opens.
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 11:00:00", "app", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 11:00:00", "legacy_billing", "invoices", "id", 1, "PRI", "int", "NO")
	// The profile exists in the index, matching a real deployment: the schemas
	// endpoint doesn't resolve it, but the session's data reads do, and a
	// nonexistent profile would be refused 403 there.
	if _, err := db.Exec(`INSERT INTO profiles (name) VALUES ('analyst')`); err != nil {
		t.Fatalf("seed profile: %v", err)
	}
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken})
	if err != nil {
		t.Fatal(err)
	}

	get := func(t *testing.T, profile string) schemasResponse {
		t.Helper()
		r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/schemas", nil)
		r.Host = "127.0.0.1:8090"
		if profile != "" {
			r = r.WithContext(context.WithValue(r.Context(),
				policyCtxKey{}, &ext.AccessPolicy{Profile: profile, Permissions: ext.AllPermissions()}))
		}
		w := httptest.NewRecorder()
		srv.handleSchemas(w, r)
		if w.Code != 200 {
			t.Fatalf("schemas code = %d, body = %s", w.Code, w.Body.String())
		}
		var sr schemasResponse
		if err := json.Unmarshal(w.Body.Bytes(), &sr); err != nil {
			t.Fatalf("decode: %v\n%s", err, w.Body.String())
		}
		return sr
	}

	// Control first, and non-vacuously: the unprofiled listing must OFFER the
	// archive-only schema, or the profiled assertion below tests a fixture in
	// which there was nothing to withhold.
	unprofiled := get(t, "")
	if want := []string{"app", "legacy_billing"}; !reflect.DeepEqual(unprofiled.Schemas, want) {
		t.Fatalf("unprofiled schemas = %v, want %v (the archive-backed union)", unprofiled.Schemas, want)
	}
	if want := []string{"legacy_billing"}; !reflect.DeepEqual(unprofiled.SnapshotOnly, want) {
		t.Fatalf("unprofiled snapshot_only = %v, want %v", unprofiled.SnapshotOnly, want)
	}

	profiled := get(t, "analyst")
	if want := []string{"app"}; !reflect.DeepEqual(profiled.Schemas, want) {
		t.Errorf("profiled schemas = %v, want %v — the dropdown offers a schema this session's every query answers zero rows for", profiled.Schemas, want)
	}
	if len(profiled.SnapshotOnly) != 0 {
		t.Errorf("profiled snapshot_only = %v, want none (snapshot half skipped)", profiled.SnapshotOnly)
	}
}
