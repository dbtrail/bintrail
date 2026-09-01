package console

import (
	"strings"
	"testing"
)

// A generated views.sql names ONE backup location and every state view
// resolves a path under it. That is why #1571's fix here is a warning and not
// a merge: a file carrying both a local directory and an s3:// prefix
// produces views that half of its readers cannot open.
//
// What silence cost was quieter and worse. When the newest snapshot has aged
// out of local retention but still lives in the bucket, the file pins the
// older local one and reads as current: the state views describe a week-old
// table and nothing in the file says so.
func TestViewsFile_namesANewerSnapshotItDoesNotRead(t *testing.T) {
	local, bucketish := t.TempDir(), t.TempDir()
	writeBaselineFixture(t, local, "2026-06-03T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, bucketish, "2026-06-10T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, local, bucketish)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if !strings.Contains(sql, "2026-06-03T12:00:00Z") {
		t.Fatalf("the file no longer pins the location it was asked for:\n%s", firstLines(sql, 20))
	}
	if !strings.Contains(sql, "2026-06-10T12:00:00Z") || !strings.Contains(sql, bucketish) {
		t.Errorf("the file pins the 06-03 snapshot and says nothing about the 06-10 one in the "+
			"other location. A reader cannot tell the state views are describing a week-old "+
			"table:\n%s", firstLines(sql, 20))
	}
	// The warning must not become a correction: the paths still have to
	// resolve, so nothing from the other location may appear as a view source.
	for _, line := range strings.Split(sql, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		if strings.Contains(line, bucketish) {
			t.Errorf("a view reads from the OTHER location: %q. The file names one root and its "+
				"paths only resolve there; mixing them produces views nobody can open", line)
		}
	}
}

// The mirror case: when the pinned location IS the newest, the file says
// nothing. A note that fires either way is noise, and a reader who learns to
// skip it stops reading the one that matters.
func TestViewsFile_saysNothingWhenItAlreadyPinsTheNewest(t *testing.T) {
	local, bucketish := t.TempDir(), t.TempDir()
	writeBaselineFixture(t, local, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, bucketish, "2026-06-03T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, local, bucketish)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if strings.Contains(string(body), "NOTE: a newer snapshot") {
		t.Errorf("the file warns about a newer snapshot while already pinning the newest:\n%s",
			firstLines(string(body), 20))
	}
}

// An unreadable second location must not fail the download. The file is
// still correct about the location it does read; the warning is the only
// thing lost, and it degrades to silence rather than to an error.
func TestViewsFile_survivesAnUnreadableSecondLocation(t *testing.T) {
	local := t.TempDir()
	writeBaselineFixture(t, local, "2026-06-10T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, local, "/definitely/not/a/directory/1571")
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, want the file anyway; the location it reads is intact. body = %s",
			rec.Code, firstLines(string(body), 8))
	}
	if !strings.Contains(string(body), "2026-06-10T12:00:00Z") {
		t.Error("the pinned snapshot is missing from a file whose own location was readable")
	}
}

func firstLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}
