//go:build integration

package console

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// ─── /api/status names only the tables the session may read (#1452) ──────────
//
// The capture-health detail names the tables whose capture stopped, and a
// table NAME is exactly what every other listing withholds from a restricted
// session. These drive GET /api/status through srv.Handler() (route, authz
// and the real status renderer) and assert on the SERIALIZED body: a denied
// name must appear nowhere in it, which catches the explanation prose as well
// as the per-reason list.

// statusSkipLedger names one table the restricted sessions below may read
// (app.users) and one they may not (app.secrets), under two reasons, so the
// scoped rendering has both a partly and a fully withheld entry to get right.
const statusSkipLedger = `{"table_not_in_snapshot":{"count":5,"last_at":"2026-08-04T19:49:33Z","tables":["app.users","app.secrets"]},` +
	`"table_excluded_from_snapshot":{"count":2,"last_at":"2026-08-04T19:50:00Z","tables":["app.secrets"],"last_detail":"no primary key"}}`

func seedStatusSkipLedger(t *testing.T, srv *Server) {
	t.Helper()
	testutil.MustExec(t, srv.cm.boot.db,
		`INSERT INTO stream_state (id, mode, server_id, last_checkpoint, capture_skips)
		 VALUES (1, 'gtid', 7, UTC_TIMESTAMP(), '`+statusSkipLedger+`')`)
}

type statusCaptureHealth struct {
	TotalSkipped int64 `json:"total_skipped"`
	Skipped      map[string]struct {
		Count          int64    `json:"count"`
		Tables         []string `json:"tables"`
		TablesWithheld int      `json:"tables_withheld"`
	} `json:"skipped"`
	Explanation []string `json:"explanation"`
}

func statusFor(t *testing.T, srv *Server, bearer string) (string, statusCaptureHealth) {
	t.Helper()
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/status", bearer)
	if rec.Code != 200 {
		t.Fatalf("GET /api/status = %d: %s", rec.Code, rec.Body.String())
	}
	var parsed struct {
		Stream struct {
			CaptureHealth statusCaptureHealth `json:"capture_health"`
		} `json:"stream"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("status JSON: %v\n%s", err, rec.Body.String())
	}
	return rec.Body.String(), parsed.Stream.CaptureHealth
}

// assertScopedStatus is the shared verdict for every restricted session: the
// denied name is absent from the whole body, the visible one is present, the
// counts are the ledger's, and each reason says how many names it withheld.
func assertScopedStatus(t *testing.T, body string, ch statusCaptureHealth) {
	t.Helper()
	if strings.Contains(body, "secrets") {
		t.Errorf("a table the session may not read is named in /api/status:\n%s", body)
	}
	if !strings.Contains(body, "app.users") {
		t.Errorf("the table the session MAY read must still be named:\n%s", body)
	}
	if ch.TotalSkipped != 7 {
		t.Errorf("total_skipped = %d, want 7: scoping must never change a count", ch.TotalSkipped)
	}
	nis := ch.Skipped["table_not_in_snapshot"]
	if nis.Count != 5 || nis.TablesWithheld != 1 || strings.Join(nis.Tables, ",") != "app.users" {
		t.Errorf("table_not_in_snapshot = %+v, want count 5, tables [app.users], tables_withheld 1", nis)
	}
	exc := ch.Skipped["table_excluded_from_snapshot"]
	if exc.Count != 2 || exc.TablesWithheld != 1 || len(exc.Tables) != 0 {
		t.Errorf("table_excluded_from_snapshot = %+v, want count 2, no tables, tables_withheld 1", exc)
	}
	joined := strings.Join(ch.Explanation, "\n")
	if !strings.Contains(joined, "1 table outside your access") {
		t.Errorf("the explanation must count the withheld name instead of dropping it silently:\n%s", joined)
	}
}

// TestIntegrationStatusCaptureSkipsUnrestrictedSeesEveryName pins the floor:
// a credential with no data scope gets the ledger verbatim, names and all,
// with no tables_withheld key anywhere.
func TestIntegrationStatusCaptureSkipsUnrestrictedSeesEveryName(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	seedStatusSkipLedger(t, srv)

	body, ch := statusFor(t, srv, "static-tok")
	for _, name := range []string{"app.users", "app.secrets"} {
		if !strings.Contains(body, name) {
			t.Errorf("an unrestricted session must see %q:\n%s", name, body)
		}
	}
	if strings.Contains(body, "tables_withheld") || strings.Contains(body, "outside your access") {
		t.Errorf("nothing is withheld from an unrestricted session, so nothing may say so:\n%s", body)
	}
	if ch.TotalSkipped != 7 || ch.Skipped["table_not_in_snapshot"].Count != 5 {
		t.Errorf("ledger counts not carried: %+v", ch)
	}
	if got := strings.Join(ch.Skipped["table_not_in_snapshot"].Tables, ","); got != "app.users,app.secrets" {
		t.Errorf("tables = %q, want both names in ledger order", got)
	}
}

// TestIntegrationStatusCaptureSkipsPolicyDenyScopesNames: a session whose
// policy DENIES app.secrets (#1449, no profile in the index needed).
func TestIntegrationStatusCaptureSkipsPolicyDenyScopesNames(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	seedStatusSkipLedger(t, srv)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		DenyTables: []ext.TableRef{{Schema: "app", Table: "secrets"}},
	})
	body, ch := statusFor(t, srv, scoped)
	assertScopedStatus(t, body, ch)

	// Per-session, not per-process: the static token on the same server still
	// sees every name after the scoped read.
	if body, _ := statusFor(t, srv, "static-tok"); !strings.Contains(body, "app.secrets") {
		t.Errorf("the restriction leaked from the session to the process:\n%s", body)
	}
}

// TestIntegrationStatusCaptureSkipsAllowListScopesNames: allow-list mode
// withholds every table it does not name, with no deny entry at all.
func TestIntegrationStatusCaptureSkipsAllowListScopesNames(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	seedStatusSkipLedger(t, srv)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		AllowTables: []ext.TableRef{{Schema: "app", Table: "users"}},
	})
	body, ch := statusFor(t, srv, scoped)
	assertScopedStatus(t, body, ch)
}

// TestIntegrationStatusCaptureSkipsProfileScopesNames: the pre-existing data
// profile path (#1075), resolved against the index at request time, scopes
// the same way. A profile the index does not define is refused, not silently
// enforced as nothing.
func TestIntegrationStatusCaptureSkipsProfileScopesNames(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	seedSensitiveProfile(t, srv)
	seedStatusSkipLedger(t, srv)
	body, ch := statusFor(t, srv, scopedBearer(t, srv, "sensitive"))
	assertScopedStatus(t, body, ch)

	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/status", scopedBearer(t, srv, "ghost")); rec.Code != 403 {
		t.Errorf("a session with an undefined profile must be refused (403), got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestIntegrationStatusCaptureSkipsStartupFloorScopesNames: the startup
// --profile floor (Config.DenyTables) is part of the scope too, exactly as it
// is for the pickers, so a STATIC token on a floored console has the denied
// name withheld. Every other case here starts floor-less; without this one a
// tableVisible that read only session fields would pass them all.
func TestIntegrationStatusCaptureSkipsStartupFloorScopesNames(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	srv, err := New(Config{
		DB:         db,
		DBName:     dbName,
		Listen:     "127.0.0.1:8090",
		Token:      "static-tok",
		DenyTables: []query.SchemaTable{{Schema: "app", Table: "secrets"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	seedStatusSkipLedger(t, srv)
	body, ch := statusFor(t, srv, "static-tok")
	assertScopedStatus(t, body, ch)
}
