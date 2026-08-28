package console

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// stubScheduleReporter stands in for the watch daemon's schedule loop.
type stubScheduleReporter struct {
	full     bool
	refusal  error
	state    map[string]BackupScheduleState
	observed []string // "<id> <identity>" per Observe call
}

func (s *stubScheduleReporter) ScheduleState(id string) BackupScheduleState {
	return s.state[id]
}
func (s *stubScheduleReporter) FullBackups() (bool, error) { return s.full, s.refusal }
func (s *stubScheduleReporter) Observe(id string, sched BackupSchedule, _ time.Time) {
	s.observed = append(s.observed, id+" "+sched.Identity())
}

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
	rep := &stubScheduleReporter{full: true}
	srv, id := newScheduleServer(t, rep)
	path := "/api/servers/" + id + "/backup-schedule"

	rec, body := doServersReq(t, srv, "PUT", path, `{"every":"1d","at":"03:00","method":"backup"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	got := scheduleOf(t, body)
	if got == nil || !got.Runnable || got.NextRun == "" || got.Every != "1d" || got.At != "03:00" || got.Method != BackupMethodFull {
		t.Fatalf("PUT response = %+v", got)
	}
	// The loop is told at save time, with the normalized schedule, so the
	// next_run this response promises is the slot that fires.
	if len(rep.observed) != 1 || rep.observed[0] != id+" 1d|03:00|backup" {
		t.Fatalf("Observe calls = %v, want one for the saved schedule", rep.observed)
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
		{"full backup with creation off", `{"every":"1d","method":"backup"}`, "BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1"},
		{"not json", `{`, "invalid JSON body"},
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
	if n := len(srv.backupSchedules.(*stubScheduleReporter).observed); n != 0 {
		t.Fatalf("a refused schedule was observed by the loop %d time(s)", n)
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
	if rec.Code != 403 || !strings.Contains(string(body), "BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1") {
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
	if got == nil || got.Runnable || !strings.Contains(got.Reason, "BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1") {
		t.Fatalf("dormant schedule = %+v, want reported as not runnable with the reason", got)
	}
	// The words the listing reports with are the words the write refuses
	// with, on both process shapes.
	if !strings.Contains(got.Reason, scheduleRefusalNoLoop) {
		t.Fatalf("listing reason %q differs from the PUT refusal", got.Reason)
	}
	// The supervisor's standing refusal (lock-mode misconfiguration) reaches
	// the listing as the reason, so a next run is not promised for a
	// schedule that can never start.
	live.backupSchedules = &stubScheduleReporter{full: true, refusal: errors.New("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: unknown mode")}
	_, body = doServersReqHeader(t, live, "GET", "/api/baselines", "", id2)
	if got = scheduleOf(t, body); got == nil || got.Runnable || !strings.Contains(got.Reason, "unknown mode") {
		t.Fatalf("misconfigured lock mode = %+v, want not runnable with the supervisor's reason", got)
	}
	if rec, body := doServersReq(t, live, "PUT", "/api/servers/"+id2+"/backup-schedule", `{"every":"1d"}`); rec.Code != 400 || !strings.Contains(string(body), "unknown mode") {
		t.Fatalf("PUT under a misconfigured lock mode: code=%d body=%s, want 400 with the reason", rec.Code, body)
	}
	// And the creation opt-in going away flips a full-backup schedule the
	// same way.
	live.backupSchedules = &stubScheduleReporter{full: false}
	_, body = doServersReqHeader(t, live, "GET", "/api/baselines", "", id2)
	if got = scheduleOf(t, body); got == nil || got.Runnable || !strings.Contains(got.Reason, "can still rebuild") {
		t.Fatalf("full schedule with creation off = %+v", got)
	}
	// A rebuild schedule is subject to neither the opt-in nor the lock mode.
	live.backupSchedules = &stubScheduleReporter{full: false, refusal: errors.New("bad lock mode")}
	if rec, body := doServersReq(t, live, "PUT", "/api/servers/"+id2+"/backup-schedule", `{"every":"1d","method":"refresh"}`); rec.Code != 200 {
		t.Fatalf("a rebuild schedule is not subject to the lock mode: code=%d body=%s", rec.Code, body)
	}
	_, body = doServersReqHeader(t, live, "GET", "/api/baselines", "", id2)
	if got = scheduleOf(t, body); got == nil || !got.Runnable {
		t.Fatalf("rebuild schedule with creation off = %+v, want runnable", got)
	}
}

func TestBackupScheduleAPI_lastRunAndSkipComeFromTheHistory(t *testing.T) {
	rep := &stubScheduleReporter{full: true, state: map[string]BackupScheduleState{}}
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
	rep.state[id] = BackupScheduleState{Running: true, LastStartedAt: "2026-08-28T18:30:00Z", LastMethod: BackupMethodRefresh,
		Last: &BaselineStatus{State: "running", Since: "2026-08-28T18:30:00Z"}}
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
	// A job in flight is not yet a run: the history's record stays.
	if got.Running && got.LastRun.StartedAt != "2026-08-28T21:00:00Z" {
		t.Fatalf("a running job replaced the last recorded run: %+v", got.LastRun)
	}
}

// The loop's in-memory view fills in where the history cannot: a job that
// panicked (no record) or a history that would not open. The newer source
// wins; a history record of the same job is never older than the loop's
// stamp, so it is never displaced.
func TestBackupScheduleAPI_lastRunFallsBackToTheLoop(t *testing.T) {
	rep := &stubScheduleReporter{full: true, state: map[string]BackupScheduleState{}}
	srv, id := newScheduleServer(t, rep)
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"1d"}`); rec.Code != 200 {
		t.Fatalf("seed: code=%d body=%s", rec.Code, body)
	}
	// Older history record, newer in-memory failure with no record (a panic).
	if err := srv.baselineHistory.Append(BaselineRunRecord{ServerID: id, Kind: BaselineRunDump, Trigger: BaselineRunTriggerScheduled,
		StartedAt: "2026-08-27T03:00:00Z", FinishedAt: "2026-08-27T03:04:00Z", Tables: 3}); err != nil {
		t.Fatal(err)
	}
	// A REBUILD, with the anchor At the supervisor stamps at start: a failed
	// run must not name it as a published snapshot.
	rep.state[id] = BackupScheduleState{LastStartedAt: "2026-08-28T03:00:00Z", LastMethod: BackupMethodRefresh,
		Last: &BaselineStatus{State: "failed", Since: "2026-08-28T03:00:00Z", At: "2026-08-28T03:00:00Z",
			FinishedAt: "2026-08-28T03:00:01Z", LastError: "panic: nil map"}}
	_, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	got := scheduleOf(t, body)
	if got.LastRun == nil || got.LastRun.OK || got.LastRun.Error != "panic: nil map" || got.LastRun.StartedAt != "2026-08-28T03:00:00Z" {
		t.Fatalf("last_run = %+v, want the loop's newer failure", got.LastRun)
	}
	if got.LastRun.SnapshotTime != "" {
		t.Fatalf("a failed rebuild named a snapshot it never published: %+v", got.LastRun)
	}
	rep.state[id].Last.State, rep.state[id].Last.LastError = "succeeded", ""
	_, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if got = scheduleOf(t, body); got.LastRun == nil || !got.LastRun.OK || got.LastRun.SnapshotTime != "2026-08-28T03:00:00Z" {
		t.Fatalf("a succeeded rebuild did not name its snapshot: %+v", got.LastRun)
	}
	rep.state[id].Last.State, rep.state[id].Last.LastError = "failed", "panic: nil map"
	// A skip only the loop knows about (history unavailable for it) shows
	// too, and a newer one beats the history's.
	if _, err := srv.baselineHistory.AppendSkip(BaselineRunRecord{ServerID: id, Kind: BaselineRunDump, SkipReason: "old",
		StartedAt: "2026-08-26T03:00:00Z", FinishedAt: "2026-08-26T03:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	st := rep.state[id]
	st.LastSkippedAt, st.LastSkipReason = "2026-08-29T03:00:00Z", "busy"
	rep.state[id] = st
	_, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if got = scheduleOf(t, body); got.LastSkipped == nil || got.LastSkipped.Reason != "busy" {
		t.Fatalf("last_skipped = %+v, want the loop's newer skip", got.LastSkipped)
	}
	// The same job once recorded: the record wins (it is not older).
	if err := srv.baselineHistory.Append(BaselineRunRecord{ServerID: id, Kind: BaselineRunDump, Trigger: BaselineRunTriggerScheduled,
		StartedAt: "2026-08-28T03:00:00Z", FinishedAt: "2026-08-28T03:00:01Z", Error: "panic: nil map", Tables: 7}); err != nil {
		t.Fatal(err)
	}
	_, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if got = scheduleOf(t, body); got.LastRun == nil || got.LastRun.Tables != 7 {
		t.Fatalf("last_run = %+v, want the history's record of the same job", got.LastRun)
	}
	// History unavailable: the loop's view is all there is, and the DTO says
	// the history is missing so the page does not claim "not run yet".
	srv.baselineHistory = nil
	_, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if got = scheduleOf(t, body); !got.HistoryUnavailable || got.LastRun == nil || got.LastRun.Error != "panic: nil map" ||
		got.LastSkipped == nil || got.LastSkipped.Reason != "busy" {
		t.Fatalf("without a history = %+v, want history_unavailable and the loop's last run and skip", got)
	}
	// A process that never has a history (no loop) is not "unavailable":
	// it has nothing to show and says why the schedule is not runnable.
	srv.backupSchedules = nil
	_, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if got = scheduleOf(t, body); got.HistoryUnavailable {
		t.Fatalf("a process with no loop reported the history unavailable: %+v", got)
	}
}

// A registry written by a newer bintrail is read-only: the schedule verbs
// refuse rather than answer 200 while the file, and the loop, keep the old
// schedule.
func TestBackupScheduleAPI_readOnlyRegistry(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/console-servers.yaml"
	if err := os.WriteFile(path, []byte("version: 2\nservers:\n  - id: abc\n    name: wp\n    index_dsn: idx:pw@tcp(127.0.0.1:3306)/idx\n    source_dsn: src:pw@tcp(127.0.0.1:3306)/\n    baseline_dir: /b\n    backup_schedule:\n      every: 1d\n      at: \"03:00\"\n      method: backup\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{},
		BackupSchedules: &stubScheduleReporter{full: true}})
	if err != nil {
		t.Fatal(err)
	}
	srv.cm.bundles["abc"] = &bundle{}
	for _, tc := range []struct{ method, body string }{{"PUT", `{"every":"6h"}`}, {"DELETE", ""}} {
		rec, body := doServersReq(t, srv, tc.method, "/api/servers/abc/backup-schedule", tc.body)
		if rec.Code != 409 {
			t.Fatalf("%s on a read-only registry: code=%d body=%s, want 409", tc.method, rec.Code, body)
		}
	}
	_, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", "abc")
	if got := scheduleOf(t, body); got == nil || got.Every != "1d" {
		t.Fatalf("the read-only file's schedule is not what the listing shows: %+v", got)
	}
}

// The schedule round-trips through the file, including a key a newer
// bintrail might add inside it, across a PUT that rewrites the schedule.
func TestBackupScheduleAPI_fileRoundTripKeepsExtra(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/console-servers.yaml"
	if err := os.WriteFile(path, []byte("version: 1\nservers:\n  - id: abc\n    name: wp\n    index_dsn: idx:pw@tcp(127.0.0.1:3306)/idx\n    source_dsn: src:pw@tcp(127.0.0.1:3306)/\n    baseline_dir: "+dir+"\n    backup_schedule:\n      every: 1d\n      at: \"03:00\"\n      method: backup\n      future_key: 42\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{},
		BackupSchedules: &stubScheduleReporter{full: true}})
	if err != nil {
		t.Fatal(err)
	}
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/abc/backup-schedule", `{"every":"6h","at":"01:00","method":"refresh"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	again, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := again.Get("abc")
	if e.BackupSchedule == nil || e.BackupSchedule.Every != "6h" || e.BackupSchedule.At != "01:00" || e.BackupSchedule.Method != BackupMethodRefresh {
		t.Fatalf("reloaded schedule = %+v", e.BackupSchedule)
	}
	if e.BackupSchedule.Extra["future_key"] != 42 {
		t.Fatalf("the forward-compat key inside the schedule did not survive the PUT: %+v", e.BackupSchedule.Extra)
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
