package console

import (
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// newFlipServer builds the shape the flip matters for: an archive source AND a
// baseline, so the file has both halves and leaving one out is visible.
func newFlipServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	// One expectation PER request the tests here make. sqlmock consumes them,
	// and a read that runs out does not fail the test -- it comes back as
	// ArchiveDiscoveryFailed, which renders a file with no events view for
	// entirely the wrong reason. That near-miss is why the count is generous
	// and why every assertion below also checks the discovery-failed wording is
	// absent.
	for range 8 {
		mock.ExpectQuery("FROM archive_state").WillReturnRows(
			sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
				AddRow("aaaa", nil, "bkt", "events/bintrail_id=aaaa/f.parquet"))
	}

	srv := newViewsServer(t, dir, false)
	srv.cm.boot.db = db
	return srv
}

// The console download is the other producer of this file, and it has its own
// copy of the decision — a route that forgot to set OmitEvents would serve the
// expensive file forever while the CLI served the cheap one, with nothing to
// notice the divergence.

func TestViewsAPI_eventsViewIsOptIn(t *testing.T) {
	srv := newFlipServer(t)

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("default download: status %d: %s", rec.Code, body)
	}
	if strings.Contains(string(body), `CREATE OR REPLACE VIEW "events"`) {
		t.Errorf("the default download still defines the events view, so every reader "+
			"pays one Parquet footer read per archived file:\n%s", body)
	}
	// The impostor: an unreadable archive_state ALSO yields no events view, and
	// would make the assertion above pass while proving nothing about the flip.
	if strings.Contains(string(body), "archive_state could not be read") {
		t.Fatalf("archive discovery failed, so the missing events view says nothing "+
			"about the default:\n%s", body)
	}
	if !strings.Contains(string(body), "-- events: not included in this file.") {
		t.Errorf("the default file does not say the events view was left out:\n%s", body)
	}
	// The state views are what the cheap file is FOR; without them this would
	// pass on an empty response.
	if !strings.Contains(string(body), `CREATE OR REPLACE VIEW "state_`) {
		t.Fatalf("the default download defines no state view either:\n%s", body)
	}

	rec, body = doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 200 {
		t.Fatalf("include_events=1: status %d: %s", rec.Code, body)
	}
	if !strings.Contains(string(body), `CREATE OR REPLACE VIEW "events"`) {
		t.Errorf("include_events=1 did not add the events view:\n%s", body)
	}
}

// TestViewsAPI_liveLegNeedsTheEventsView: the leg hangs on that view, so asking
// for it alone is a request the route cannot honour. A 200 carrying a file
// without the leg would be the worst answer — the client asked, got success,
// and has to read the SQL to discover it did not happen.
func TestViewsAPI_liveLegNeedsTheEventsView(t *testing.T) {
	srv := newFlipServer(t)

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_live=1", "")
	if rec.Code != 400 {
		t.Fatalf("include_live without include_events: status %d, want 400: %s", rec.Code, body)
	}
	if !strings.Contains(string(body), "include_events=1") {
		t.Errorf("the refusal never names the parameter that fixes it: %s", body)
	}
}

// TestViewsAPI_includeEventsIsStrict: isTrue would read an unrecognized value as
// false, which here means a 200 and a file missing the view the caller asked
// for. Same reasoning as include_live, and it has to be asserted separately
// because they are separate parameters reaching separate call sites.
func TestViewsAPI_includeEventsIsStrict(t *testing.T) {
	srv := newFlipServer(t)
	for _, v := range []string{"yes", "on", "2", "maybe"} {
		rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events="+v, "")
		if rec.Code != 400 {
			t.Errorf("include_events=%q: status %d, want 400: %s", v, rec.Code, body)
		}
	}
	for _, v := range []string{"1", "true"} {
		rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events="+v, "")
		if rec.Code != 200 {
			t.Errorf("include_events=%q: status %d, want 200: %s", v, rec.Code, body)
		}
		if !strings.Contains(string(body), `CREATE OR REPLACE VIEW "events"`) {
			t.Errorf("include_events=%q was accepted but added no events view", v)
		}
	}
}
