package console

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// newFollowServer builds a console over a baselines root holding an older and a
// newer snapshot, with the pointer published at whichever the caller names.
// Passing "" publishes none, which is what every root written before this
// feature looks like.
func newFollowServer(t *testing.T, pointAt string) *Server {
	t.Helper()
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-01T00-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	if pointAt != "" {
		if err := baseline.PublishCurrentPointer(filepath.Join(dir, pointAt)); err != nil {
			t.Fatal(err)
		}
	}
	return newViewsServer(t, dir, false)
}

func stateReadPath(t *testing.T, raw []byte) string {
	body := string(raw)
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if i := strings.Index(line, "read_parquet('"); i >= 0 {
			rest := line[i+len("read_parquet('"):]
			return rest[:strings.Index(rest, "'")]
		}
	}
	t.Fatalf("no state view in:\n%s", body)
	return ""
}

// TestViewsAPI_stateViewsFollowTheCurrentPointer is the console half of #1484,
// and it exists because this route is the SECOND producer of the file. A
// download that kept pinning while `bintrail views` followed would leave every
// console user's schema silently frozen, with the CLI's behaviour as the only
// evidence anything was wrong.
func TestViewsAPI_stateViewsFollowTheCurrentPointer(t *testing.T) {
	srv := newFollowServer(t, "2026-06-10T12-00-00Z")
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	got := stateReadPath(t, body)
	if filepath.Base(filepath.Dir(filepath.Dir(got))) != baseline.CurrentLinkName {
		t.Fatalf("state view reads %q, want it through the %s pointer", got, baseline.CurrentLinkName)
	}
	if !strings.Contains(string(body), "views follow the `"+baseline.CurrentLinkName+"` pointer") {
		t.Fatal("the downloaded file does not say it follows the pointer")
	}
}

// TestViewsAPI_pinSnapshotIsHonoured covers the checkbox: the operator asked
// for a fixed point in time and must get a path that cannot move.
func TestViewsAPI_pinSnapshotIsHonoured(t *testing.T) {
	srv := newFollowServer(t, "2026-06-10T12-00-00Z")
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?pin_snapshot=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	got := stateReadPath(t, body)
	if !strings.Contains(got, "2026-06-10T12-00-00Z") {
		t.Fatalf("pinned download reads %q, want the snapshot directory", got)
	}
	if strings.Contains(got, baseline.CurrentLinkName) {
		t.Fatalf("pin_snapshot=1 still went through the pointer: %q", got)
	}
	if strings.Contains(string(body), "follow the `"+baseline.CurrentLinkName+"` pointer") {
		t.Fatal("a pinned file claims to follow the pointer")
	}
}

// TestViewsAPI_rootWithNoPointerIsPinned is every existing installation on the
// day it upgrades: no pointer exists until the next backup completes. The
// download must be exactly what it was, with no path through a link that is
// not there.
func TestViewsAPI_rootWithNoPointerIsPinned(t *testing.T) {
	srv := newFollowServer(t, "")
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	got := stateReadPath(t, body)
	if !strings.Contains(got, "2026-06-10T12-00-00Z") {
		t.Fatalf("reads %q, want the newest snapshot directory", got)
	}
	if strings.Contains(got, baseline.CurrentLinkName) {
		t.Fatalf("emitted a path through a pointer that does not exist: %q", got)
	}
}

// TestViewsAPI_pinSnapshotIsStrict keeps the parameter from failing open. A
// typo that silently means "false" would hand back a following file to someone
// who asked for a pinned one, and nothing in the response would say so.
func TestViewsAPI_pinSnapshotIsStrict(t *testing.T) {
	srv := newFollowServer(t, "2026-06-10T12-00-00Z")
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?pin_snapshot=yes", "")
	if rec.Code != 400 {
		t.Fatalf("code = %d, want 400; body = %s", rec.Code, body)
	}
	if !strings.Contains(string(body), "pin_snapshot") {
		t.Fatalf("the refusal does not name the parameter: %s", body)
	}
}
