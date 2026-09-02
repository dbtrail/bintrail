package console

import (
	"regexp"
	"strings"
	"testing"
)

// The Backups & snapshots settings page (#1582): wire names, coverage of the
// daemon rows, the move of the per-server fields out of the server form, and
// the passthrough that makes the move safe.

// TestBackupSettingsWireNamesMatchTheFrontend pins the JSON keys the Go DTOs
// emit against what the page reads. A renamed tag on either side renders a
// page of blanks with the whole suite green otherwise.
func TestBackupSettingsWireNamesMatchTheFrontend(t *testing.T) {
	js := readAsset(t, "app.js")
	page := jsFunctionBody(t, js, "buildBackupSettings") +
		jsFunctionBody(t, js, "backupDaemonCard") +
		jsFunctionBody(t, js, "backupServersPanel") +
		jsFunctionBody(t, js, "backupServerRow")
	// Dotted READS, not bare tokens: "value" also matches dir.value.trim()
	// and "baseline_dir" matches the input's name: attribute, so a renamed
	// JSON tag stayed green while the page rendered blanks. The dotted form
	// is the actual dereference of the wire field.
	for _, read := range []string{
		"settings.daemon", "settings.servers", "settings.registry_read_only",
		"row.key", "row.value", "row.on", "row.cli", "row.needs_restart", "row.err",
		"srv.baseline_dir", "srv.baseline_s3", "srv.no_archive",
		"srv.resolved_dir", "srv.resolved_s3", "srv.source",
		"srv.schedule_every", "srv.schedule_at", "srv.schedule_refusal",
	} {
		if !strings.Contains(page, read) {
			t.Errorf("the page never reads %q; the server emits it and the page renders a blank instead", read)
		}
	}
}

// TestBackupSettingsDaemonRowsAreAllLabeled: every key the handler emits has
// a label in BACKUP_DAEMON_ROWS, or the page falls back to the raw key — a
// flag name where the label's whole job is saying what the flag means.
func TestBackupSettingsDaemonRowsAreAllLabeled(t *testing.T) {
	js := readAsset(t, "app.js")
	block := regexp.MustCompile(`const BACKUP_DAEMON_ROWS = \{[^}]+\}`).FindString(js)
	if block == "" {
		t.Fatal("BACKUP_DAEMON_ROWS is gone from app.js")
	}
	// The canonical key set, spelled here and in the handler; the API test
	// pins the handler side against the CLI names.
	for _, key := range []string{
		"baseline_dir", "baseline_s3", "baseline_retain", "refresh_every",
		"lock_mode", "trigger", "staging_dir", "verify_interval", "verify_tables",
	} {
		if !strings.Contains(block, key+":") {
			t.Errorf("BACKUP_DAEMON_ROWS has no label for %q; the row would render its raw key", key)
		}
	}
}

// TestServerFormCarriesTheBackupFieldsAsPassthrough is the wipe hazard the
// move created (#1582): PUT /api/servers/{id} REPLACES the entry, so a form
// that stopped sending baseline_dir/baseline_s3/no_archive would silently
// clear a server's backup configuration on every unrelated edit. The fields
// left the visible form for the settings page; they must survive in it as
// hidden passthroughs, prefilled and submitted like before.
func TestServerFormCarriesTheBackupFieldsAsPassthrough(t *testing.T) {
	js := readAsset(t, "app.js")
	form := jsFunctionBody(t, js, "buildServerForm")
	for _, field := range []string{`name: "baseline_dir"`, `name: "baseline_s3"`, `name: "no_archive"`} {
		if !strings.Contains(form, field) {
			t.Errorf("buildServerForm no longer carries %s; a plain edit now WIPES that field on the entry", field)
		}
	}
	// Hidden, not visible: the settings page is the one editor. A visible
	// duplicate saves to one store from two places, one of them stale.
	if strings.Contains(form, `srvField("Backup dir"`) || strings.Contains(form, `srvField("Backup S3"`) {
		t.Error("the server form still renders visible backup-location fields; they moved to the settings page")
	}
	body := jsFunctionBody(t, js, "serverFormBody")
	for _, read := range []string{"f.baseline_dir.value", "f.baseline_s3.value", "f.no_archive.checked"} {
		if !strings.Contains(body, read) {
			t.Errorf("serverFormBody no longer sends %s; the PUT will replace the entry without it", read)
		}
	}
	// And the prefill still fills the hidden halves, or the passthrough
	// passes empty strings through — the exact wipe it exists to prevent.
	show := jsFunctionBody(t, js, "showServerForm")
	if !strings.Contains(show, `"baseline_dir", "baseline_s3"`) {
		t.Error("showServerForm's prefill list no longer covers the hidden backup fields")
	}
	if !strings.Contains(show, "form.elements.no_archive.checked = !!prefill.no_archive") {
		t.Error("showServerForm no longer prefills no_archive; the hidden checkbox submits unchecked for every edit")
	}
}

// TestBackupSettingsPageIsWired: route in ROUTES, a renderRoute arm, the
// monitor gate, and a nav item — the four halves that make a page reachable.
func TestBackupSettingsPageIsWired(t *testing.T) {
	js := readAsset(t, "app.js")
	if !regexp.MustCompile(`"backup-settings"\]?`).MatchString(js) {
		t.Fatal("backup-settings is not in ROUTES")
	}
	if !strings.Contains(js, `case "backup-settings": return renderBackupSettings();`) {
		t.Error("renderRoute has no arm for backup-settings; the URL falls through to Overview")
	}
	// NOT monitor-gated, deliberately (the Access profiles precedent): the
	// server Edit form's backup fields became passthroughs, so this page is
	// the ONLY editor of the registry's backup location — and the registry
	// is state the standalone serve edits too. Gating the page left serve
	// with no UI path to a backup location at all. The daemon-side cards
	// inside the page carry the monitor gate instead.
	if strings.Contains(js, `route === "backup-settings") && !capsCache.monitor`) ||
		strings.Contains(js, `"backup-settings" || route`) {
		t.Error("backup-settings is behind the monitor gate again; on serve the per-server backup " +
			"location would have NO editor anywhere in the UI")
	}
	body := jsFunctionBody(t, js, "buildBackupSettings")
	if !strings.Contains(body, "capsCache.monitor") {
		t.Error("buildBackupSettings no longer gates the daemon-side cards on monitor; on serve the " +
			"daemon card renders empty rows and reads as an unconfigured install")
	}
	html := readAsset(t, "index.html")
	if !strings.Contains(html, `data-route="backup-settings"`) {
		t.Error("index.html has no nav item for backup-settings")
	}
	navRE := regexp.MustCompile(`(?s)data-route="backup-settings"[^>]*>`)
	if nav := navRE.FindString(html); strings.Contains(nav, `data-capability="monitor"`) {
		t.Error("the backup-settings nav item is capability-gated on monitor; serve users could not " +
			"reach the only editor of the per-server backup location")
	}
}
