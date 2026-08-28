package console

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// stubScheduleReporter stands in for the watch daemon's schedule loop.
type stubScheduleReporter struct {
	full    bool
	running map[string]bool
}

func (s *stubScheduleReporter) ScheduleState(id string) BackupScheduleState {
	return BackupScheduleState{Running: s.running[id]}
}
func (s *stubScheduleReporter) FullBackups() bool { return s.full }

// newScheduleServer builds a watch-shaped server with the schedule loop
// present, a persisted history, and one registry server that can run either
// method. Returns the server and the entry id.
func newScheduleServer(t *testing.T, rep *stubScheduleReporter) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	reg, err := LoadRegistry(dir + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	hist, err := OpenBaselineHistory(dir + "/console-baseline-history.json")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{},
		BaselineHistory: hist}
	if rep != nil {
		cfg.BackupSchedules = rep
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := reg.Add(ServerEntry{Name: "wp", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx",
		SourceDSN: "src:pw@tcp(127.0.0.1:3306)/", BaselineDir: dir + "/backups"})
	if err != nil {
		t.Fatal(err)
	}
	// A primed bundle so /api/baselines resolves the entry without opening
	// its index.
	srv.cm.bundles[e.ID] = &bundle{}
	return srv, e.ID
}

func scheduleOf(t *testing.T, body []byte) *backupScheduleDTO {
	t.Helper()
	var got struct {
		Schedule *backupScheduleDTO `json:"schedule"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	return got.Schedule
}

func TestBackupScheduleAPI_saveListRemove(t *testing.T) {
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
	path := "/api/servers/" + id + "/backup-schedule"

	rec, body := doServersReq(t, srv, "PUT", path, `{"every":"1d","at":"03:00","method":"backup"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	got := scheduleOf(t, body)
	if got == nil || !got.Runnable || got.NextRun == "" || got.Every != "1d" || got.At != "03:00" || got.Method != BackupMethodFull {
		t.Fatalf("PUT response = %+v", got)
	}
	if !strings.HasSuffix(got.NextRun, "T03:00:00Z") {
		t.Fatalf("next_run %q is not on the 03:00 grid", got.NextRun)
	}

	// The listing carries it for the selected server.
	rec, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if rec.Code != 200 {
		t.Fatalf("GET /api/baselines code=%d body=%s", rec.Code, body)
	}
	if got = scheduleOf(t, body); got == nil || got.Every != "1d" || !got.Runnable {
		t.Fatalf("listing schedule = %+v", got)
	}
	// Persisted, with the defaults spelled out.
	e, _ := srv.cm.reg.Get(id)
	if e.BackupSchedule == nil || e.BackupSchedule.At != "03:00" || e.BackupSchedule.Method != BackupMethodFull {
		t.Fatalf("stored = %+v", e.BackupSchedule)
	}

	// An edit of the connection keeps it.
	rec, body = doServersReq(t, srv, "PUT", "/api/servers/"+id, `{"name":"wp2","host":"127.0.0.1","port":"3306","user":"idx","dbname":"idx"}`)
	if rec.Code != 200 {
		t.Fatalf("server edit code=%d body=%s", rec.Code, body)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule == nil {
		t.Fatal("editing the server dropped its schedule")
	}

	rec, body = doServersReq(t, srv, "DELETE", path, "")
	if rec.Code != 200 {
		t.Fatalf("DELETE code=%d body=%s", rec.Code, body)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule != nil {
		t.Fatal("DELETE left the schedule in place")
	}
	if rec, _ = doServersReq(t, srv, "DELETE", path, ""); rec.Code != 200 {
		t.Fatalf("removing an absent schedule = %d, want 200", rec.Code)
	}
	_, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if scheduleOf(t, body) != nil {
		t.Fatal("the listing still shows a removed schedule")
	}
}

func TestBackupScheduleAPI_refusals(t *testing.T) {
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: false})
	path := "/api/servers/" + id + "/backup-schedule"
	cases := []struct {
		name string
		body string
		want string
	}{
		{"too often", `{"every":"1m"}`, "too often"},
		{"bad clock", `{"every":"1d","at":"3pm"}`, "HH:MM"},
		{"bad method", `{"every":"1d","method":"snapshot"}`, "method"},
		{"full backup with creation off", `{"every":"1d","method":"backup"}`, "BINTRAIL_CONSOLE_BASELINE_TRIGGER=0"},
		{"not json", `{`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec, body := doServersReq(t, srv, "PUT", path, c.body)
			if rec.Code != 400 {
				t.Fatalf("code=%d body=%s, want 400", rec.Code, body)
			}
			if !strings.Contains(string(body), c.want) {
				t.Fatalf("body %s does not say %q", body, c.want)
			}
			if e, _ := srv.cm.reg.Get(id); e.BackupSchedule != nil {
				t.Fatal("a refused schedule was saved anyway")
			}
		})
	}
	// A refresh schedule needs no creation opt-in.
	if rec, body := doServersReq(t, srv, "PUT", path, `{"every":"1h","method":"refresh"}`); rec.Code != 200 {
		t.Fatalf("refresh with creation off: code=%d body=%s", rec.Code, body)
	}
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/default/backup-schedule", `{"every":"1d"}`); rec.Code != 409 {
		t.Fatalf("boot entry: code=%d body=%s, want 409", rec.Code, body)
	}
	if rec, _ := doServersReq(t, srv, "PUT", "/api/servers/nope/backup-schedule", `{"every":"1d"}`); rec.Code != 404 {
		t.Fatalf("unknown server: code=%d, want 404", rec.Code)
	}
}

// Without the loop the write is refused, with the daemon named, and the
// capability says so; the read-only console gets its own words.
func TestBackupScheduleAPI_needsTheLoop(t *testing.T) {
	srv, id := newScheduleServer(t, nil) // watch, no baseline features
	rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"1d"}`)
	if rec.Code != 403 || !strings.Contains(string(body), "BINTRAIL_CONSOLE_BASELINE_TRIGGER=0") {
		t.Fatalf("watch without features: code=%d body=%s", rec.Code, body)
	}
	srv.cm.boot = &bundle{}
	_, body = doServersReq(t, srv, "GET", "/api/capabilities", "")
	if !strings.Contains(string(body), `"backup_schedule":false`) {
		t.Fatalf("capabilities did not report the loop absent: %s", body)
	}

	ro := newRegistryServer(t) // serve
	e, _ := ro.cm.reg.Add(ServerEntry{Name: "wp", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx"})
	rec, body = doServersReq(t, ro, "PUT", "/api/servers/"+e.ID+"/backup-schedule", `{"every":"1d"}`)
	if rec.Code != 403 || !strings.Contains(string(body), "watch daemon") {
		t.Fatalf("read-only console: code=%d body=%s", rec.Code, body)
	}

	// A schedule that IS in the file (saved by a daemon that could run it)
	// is still reported, as not runnable, so it is never silently inert.
	live, id2 := newScheduleServer(t, &stubScheduleReporter{full: true})
	if rec, body := doServersReq(t, live, "PUT", "/api/servers/"+id2+"/backup-schedule", `{"every":"1d"}`); rec.Code != 200 {
		t.Fatalf("seed: code=%d body=%s", rec.Code, body)
	}
	live.backupSchedules = nil
	_, body = doServersReqHeader(t, live, "GET", "/api/baselines", "", id2)
	got := scheduleOf(t, body)
	if got == nil || got.Runnable || !strings.Contains(got.Reason, "BINTRAIL_CONSOLE_BASELINE_TRIGGER=0") {
		t.Fatalf("dormant schedule = %+v, want reported as not runnable with the reason", got)
	}
	// And the creation opt-in going away flips a full-backup schedule the
	// same way, while a refresh one stays runnable.
	live.backupSchedules = &stubScheduleReporter{full: false}
	_, body = doServersReqHeader(t, live, "GET", "/api/baselines", "", id2)
	if got = scheduleOf(t, body); got == nil || got.Runnable || !strings.Contains(got.Reason, "can still rebuild") {
		t.Fatalf("full schedule with creation off = %+v", got)
	}
}

func TestBackupScheduleAPI_lastRunAndSkipComeFromTheHistory(t *testing.T) {
	rep := &stubScheduleReporter{full: true, running: map[string]bool{}}
	srv, id := newScheduleServer(t, rep)
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"6h","method":"refresh"}`); rec.Code != 200 {
		t.Fatalf("seed: code=%d body=%s", rec.Code, body)
	}
	h := srv.baselineHistory
	if err := h.Append(BaselineRunRecord{ServerID: id, Kind: BaselineRunRefresh, Trigger: BaselineRunTriggerScheduled,
		StartedAt: "2026-08-28T09:00:00Z", FinishedAt: "2026-08-28T09:04:00Z", SnapshotTime: "2026-08-28T09:00:00Z",
		Tables: 12, Carried: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.AppendSkip(BaselineRunRecord{ServerID: id, Kind: BaselineRunRefresh, SkipReason: "busy",
		StartedAt: "2026-08-28T15:00:00Z", FinishedAt: "2026-08-28T15:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	rep.running[id] = true
	_, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	got := scheduleOf(t, body)
	if got == nil || got.LastRun == nil || got.LastRun.Method != BackupMethodRefresh || !got.LastRun.OK ||
		got.LastRun.Tables != 12 || got.LastRun.Carried != 3 || got.LastRun.SnapshotTime != "2026-08-28T09:00:00Z" {
		t.Fatalf("last_run = %+v", got)
	}
	if got.LastSkipped == nil || got.LastSkipped.Reason != "busy" || got.LastSkipped.At != "2026-08-28T15:00:00Z" {
		t.Fatalf("last_skipped = %+v", got.LastSkipped)
	}
	if !got.Running {
		t.Fatal("the loop's running state did not reach the listing")
	}
	// A failed run is reported as such, error included.
	if err := h.Append(BaselineRunRecord{ServerID: id, Kind: BaselineRunRefresh, Trigger: BaselineRunTriggerScheduled,
		StartedAt: "2026-08-28T21:00:00Z", FinishedAt: "2026-08-28T21:00:30Z", Error: "capture gap", Refused: 1}); err != nil {
		t.Fatal(err)
	}
	_, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if got = scheduleOf(t, body); got.LastRun == nil || got.LastRun.OK || got.LastRun.Error != "capture gap" || got.LastRun.Refused != 1 {
		t.Fatalf("failed last_run = %+v", got.LastRun)
	}
}

// The schedule endpoints are classified, so a scoped session cannot reach
// them with a read permission; the guard test over apiRoutePerms covers
// "classified at all", this pins the tier.
func TestBackupScheduleAPI_permissionTier(t *testing.T) {
	for _, m := range []string{"PUT", "DELETE"} {
		got, ok := permForRoute(m, "/api/servers/abc/backup-schedule")
		if !ok {
			t.Fatalf("%s backup-schedule is unclassified", m)
		}
		if got != permForRouteMust(t, "PUT", "/api/baseline-refresh") {
			t.Fatalf("%s backup-schedule = %v, want the same tier as PUT /api/baseline-refresh", m, got)
		}
	}
}

func permForRouteMust(t *testing.T, method, path string) any {
	t.Helper()
	p, ok := permForRoute(method, path)
	if !ok {
		t.Fatalf("%s %s is unclassified", method, path)
	}
	return p
}

// Sanity for the helper the tests above lean on: the header-selected
// listing really is served for the primed entry.
func TestBackupScheduleAPI_listingResolvesThePrimedEntry(t *testing.T) {
	srv, id := newScheduleServer(t, &stubScheduleReporter{})
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/baselines", nil)
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer t")
	req.Header.Set("X-Bintrail-Server", id)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
}
