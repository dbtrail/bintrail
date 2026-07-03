//go:build integration

package forensics

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// integrationSourceDB opens a connection to the shared test MySQL server —
// forensics inspects server-global state (performance_schema, plugins), so
// these tests connect to the server itself rather than a per-test database.
func integrationSourceDB(t *testing.T) *sql.DB {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	db, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatalf("connect to test MySQL: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestIntegrationDetectCapabilities asserts shape and internal consistency of
// the detection result against the live server. The container's
// performance_schema/audit state is whatever it is, so the assertions are
// invariants, not exact server config.
func TestIntegrationDetectCapabilities(t *testing.T) {
	db := integrationSourceDB(t)
	ctx := t.Context()

	caps, err := DetectCapabilities(ctx, db)
	if err != nil {
		t.Fatalf("DetectCapabilities: %v", err)
	}

	if caps.ServerInfo.Version == "" {
		t.Error("ServerInfo.Version is empty on a live server")
	}
	validVariants := []string{"mysql", "percona", "mariadb"}
	if !slices.Contains(validVariants, caps.ServerInfo.Variant) {
		t.Errorf("ServerInfo.Variant = %q, want one of %v", caps.ServerInfo.Variant, validVariants)
	}

	// Consistency: consumer/threads flags are only meaningful under an
	// enabled performance_schema.
	if !caps.PerformanceSchema.Enabled {
		if caps.PerformanceSchema.Consumers.EventsStatementsHistory ||
			caps.PerformanceSchema.Consumers.EventsStatementsHistoryLong ||
			caps.PerformanceSchema.ThreadsAccessible {
			t.Errorf("p_s disabled but sub-capabilities set: %+v", caps.PerformanceSchema)
		}
	}
	// Consistency: no plugin → no plugin metadata.
	if !caps.AuditLog.Installed {
		if caps.AuditLog.PluginName != "" || caps.AuditLog.Variant != "" {
			t.Errorf("audit not installed but metadata set: %+v", caps.AuditLog)
		}
	}

	// The setup guide composes from any capability state.
	guide := BuildSetupGuide(caps)
	if guide.Summary == "" {
		t.Error("BuildSetupGuide returned an empty summary")
	}
	if len(guide.Recommendations) == 0 && !strings.Contains(guide.Summary, "fully configured") {
		t.Errorf("no recommendations but summary is not the fully-configured one: %q", guide.Summary)
	}
}

// TestIntegrationConsumerToggle exercises detection and guide generation with
// the events_statements_history_long consumer both OFF and ON, restoring the
// server's original state afterwards.
func TestIntegrationConsumerToggle(t *testing.T) {
	db := integrationSourceDB(t)
	ctx := t.Context()

	caps, err := DetectCapabilities(ctx, db)
	if err != nil {
		t.Fatalf("DetectCapabilities: %v", err)
	}
	if !caps.PerformanceSchema.Enabled {
		t.Skip("performance_schema disabled on the test server")
	}

	const consumer = "events_statements_history_long"
	var orig string
	if err := db.QueryRowContext(ctx,
		"SELECT ENABLED FROM performance_schema.setup_consumers WHERE NAME = ?", consumer,
	).Scan(&orig); err != nil {
		t.Skipf("cannot read setup_consumers: %v", err)
	}
	setConsumer := func(val string) {
		t.Helper()
		// context.Background(), not t.Context(): the restore runs from
		// t.Cleanup, after the test context is already canceled.
		if _, err := db.ExecContext(context.Background(),
			"UPDATE performance_schema.setup_consumers SET ENABLED = ? WHERE NAME = ?", val, consumer); err != nil {
			t.Fatalf("toggle consumer to %s: %v", val, err)
		}
	}
	t.Cleanup(func() { setConsumer(orig) })

	// OFF: detection reports it off and the guide recommends enabling it.
	setConsumer("NO")
	capsOff, err := DetectCapabilities(ctx, db)
	if err != nil {
		t.Fatalf("DetectCapabilities (consumer off): %v", err)
	}
	if capsOff.PerformanceSchema.Consumers.EventsStatementsHistoryLong {
		t.Error("consumer toggled NO but detection reports it enabled")
	}
	guideOff := BuildSetupGuide(capsOff)
	foundRec := false
	for _, rec := range guideOff.Recommendations {
		if rec.Title == "Enable global statement history consumer" {
			foundRec = true
			if len(rec.RuntimeSQL) == 0 || !strings.Contains(rec.RuntimeSQL[0], consumer) {
				t.Errorf("recommendation lacks the runtime UPDATE for %s: %v", consumer, rec.RuntimeSQL)
			}
		}
	}
	if !foundRec {
		t.Errorf("guide with consumer off is missing the history_long recommendation: %v", recTitles(guideOff))
	}

	// ON: detection reports it on and the recommendation disappears.
	setConsumer("YES")
	capsOn, err := DetectCapabilities(ctx, db)
	if err != nil {
		t.Fatalf("DetectCapabilities (consumer on): %v", err)
	}
	if !capsOn.PerformanceSchema.Consumers.EventsStatementsHistoryLong {
		t.Error("consumer toggled YES but detection reports it disabled")
	}
	for _, rec := range BuildSetupGuide(capsOn).Recommendations {
		if rec.Title == "Enable global statement history consumer" {
			t.Error("guide still recommends enabling history_long after it was enabled")
		}
	}
}

// TestIntegrationEnrichThreads enriches this test's own pinned connection —
// the one session guaranteed to be live — plus an ID guaranteed not to exist.
func TestIntegrationEnrichThreads(t *testing.T) {
	db := integrationSourceDB(t)
	ctx := t.Context()

	caps, err := DetectCapabilities(ctx, db)
	if err != nil {
		t.Fatalf("DetectCapabilities: %v", err)
	}
	if !caps.PerformanceSchema.Enabled || !caps.PerformanceSchema.ThreadsAccessible {
		t.Skip("performance_schema threads not accessible on the test server")
	}

	// Pin a dedicated connection so its session stays visible in
	// performance_schema.threads while the enrichment (on other pool
	// connections) runs.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin connection: %v", err)
	}
	defer conn.Close()
	var myID int64
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&myID); err != nil {
		t.Fatalf("CONNECTION_ID(): %v", err)
	}

	const ghostID int64 = 999999999
	res, err := EnrichThreads(ctx, db, []int64{myID, ghostID})
	if err != nil {
		t.Fatalf("EnrichThreads: %v", err)
	}

	if res.Source != "performance_schema" {
		t.Errorf("Source = %q, want performance_schema", res.Source)
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
	if slices.Contains(res.NotFound, myID) {
		t.Errorf("own live connection reported as not found: %v", res.NotFound)
	}
	if !slices.Contains(res.NotFound, ghostID) {
		t.Errorf("ghost ID missing from NotFound: %v", res.NotFound)
	}
	if len(res.FallbackQueries) == 0 {
		t.Fatal("expected fallback queries for the ghost ID")
	}
	for _, q := range res.FallbackQueries {
		if !strings.Contains(q.SQL, fmt.Sprintf("%d", ghostID)) {
			t.Errorf("fallback SQL does not reference the ghost ID: %s", q.SQL)
		}
	}
}

// TestIntegrationActivity runs the three query modes live. Ring-buffer
// contents depend on server history, so assertions target the contract:
// no errors, and fallback (with executable SQL) whenever data is absent.
func TestIntegrationActivity(t *testing.T) {
	db := integrationSourceDB(t)
	ctx := t.Context()

	caps, err := DetectCapabilities(ctx, db)
	if err != nil {
		t.Fatalf("DetectCapabilities: %v", err)
	}

	t.Run("user_activity nonexistent user falls back", func(t *testing.T) {
		res, err := Activity(ctx, db, ActivityQuery{Type: QueryUserActivity, User: "bt_no_such_user_702"})
		if err != nil {
			t.Fatalf("Activity: %v", err)
		}
		if res.Source != "fallback" {
			t.Errorf("Source = %q, want fallback for a user with no activity", res.Source)
		}
		if len(res.FallbackQueries) == 0 {
			t.Error("expected fallback queries")
		}
		if res.Note == "" {
			t.Error("expected a diagnostic note")
		}
	})

	t.Run("connection_history sees own session", func(t *testing.T) {
		// Pin a connection so at least one root session is guaranteed live.
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("pin connection: %v", err)
		}
		defer conn.Close()
		var user string
		if err := conn.QueryRowContext(ctx, "SELECT SUBSTRING_INDEX(CURRENT_USER(), '@', 1)").Scan(&user); err != nil {
			t.Fatalf("CURRENT_USER(): %v", err)
		}

		res, err := Activity(ctx, db, ActivityQuery{Type: QueryConnectionHistory, User: user})
		if err != nil {
			t.Fatalf("Activity: %v", err)
		}
		if caps.PerformanceSchema.Enabled && caps.PerformanceSchema.ThreadsAccessible {
			if res.Source != "performance_schema" || res.Count < 1 {
				t.Errorf("source=%q count=%d, want performance_schema with >=1 connection", res.Source, res.Count)
			}
			for _, c := range res.Connections {
				if _, ok := c["connection_id"]; !ok {
					t.Errorf("connection entry missing connection_id: %v", c)
				}
			}
		}
	})

	t.Run("ddl_history returns cleanly", func(t *testing.T) {
		res, err := Activity(ctx, db, ActivityQuery{Type: QueryDDLHistory})
		if err != nil {
			t.Fatalf("Activity: %v", err)
		}
		if res.Source != "performance_schema" && res.Source != "fallback" {
			t.Errorf("Source = %q, want performance_schema or fallback", res.Source)
		}
		if res.Source == "fallback" && len(res.FallbackQueries) == 0 {
			t.Error("fallback source but no fallback queries")
		}
	})
}

func TestIntegrationListUsers(t *testing.T) {
	db := integrationSourceDB(t)

	users, err := ListUsers(t.Context(), db)
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if !slices.Contains(users, "root") {
		t.Errorf("users = %v, want it to contain root", users)
	}
}
