package ext

import (
	"net/http"
	"slices"
	"testing"
)

type stubSettings struct {
	id    string
	label string
}

func (p stubSettings) ID() string                      { return p.id }
func (p stubSettings) Label() string                   { return p.label }
func (p stubSettings) Script() string                  { return "/ext-settings/" + p.id + "/panel.js" }
func (stubSettings) StaticHandler(string) http.Handler { return http.NotFoundHandler() }
func (stubSettings) DataHandler(string, ConsoleSettingsContextFunc) http.Handler {
	return http.NotFoundHandler()
}

func TestConsoleSettingsEmptyByDefault(t *testing.T) {
	t.Cleanup(ResetForTest)
	if got := ConsoleSettings(); len(got) != 0 {
		t.Fatalf("ConsoleSettings() = %v, want empty — the OSS build installs no settings panel", got)
	}
}

// Additive is the whole point of the registry: a second install must not
// displace the first (the single-slot seam it replaces silently kept whichever
// call ran last, so wiring order decided which panel existed).
func TestRegisterConsoleSettingsIsAdditive(t *testing.T) {
	t.Cleanup(ResetForTest)
	RegisterConsoleSettings(stubSettings{id: "users", label: "Users"})
	RegisterConsoleSettings(stubSettings{id: "keys", label: "Keys"})

	got := ConsoleSettings()
	if len(got) != 2 {
		t.Fatalf("ConsoleSettings() has %d providers, want 2 (a second install must not replace the first)", len(got))
	}
	if got[0].ID() != "users" || got[1].ID() != "keys" {
		t.Errorf("ConsoleSettings() ids = %q,%q — install order must be preserved so nav items don't shuffle", got[0].ID(), got[1].ID())
	}
}

// A nil install must be dropped, not appended: the console would otherwise
// dereference it at mount time and take the daemon down at startup.
func TestRegisterConsoleSettingsIgnoresNil(t *testing.T) {
	t.Cleanup(ResetForTest)
	RegisterConsoleSettings(nil)
	if got := ConsoleSettings(); len(got) != 0 {
		t.Fatalf("ConsoleSettings() = %v after registering nil, want empty", got)
	}
}

// The returned slice must not alias the registry — a caller appending to it
// would otherwise install a panel nobody registered.
func TestConsoleSettingsReturnsCopy(t *testing.T) {
	t.Cleanup(ResetForTest)
	RegisterConsoleSettings(stubSettings{id: "users", label: "Users"})
	got := ConsoleSettings()
	got[0] = stubSettings{id: "hijacked", label: "Hijacked"}
	if ConsoleSettings()[0].ID() != "users" {
		t.Error("mutating the ConsoleSettings() result changed the registry")
	}
}

// The setters do not validate IDs — validation belongs to the console at mount
// time, so a caller's typo degrades to "no panel" instead of panicking the
// daemon during startup wiring.
func TestRegisterConsoleSettingsDoesNotValidateID(t *testing.T) {
	t.Cleanup(ResetForTest)
	RegisterConsoleSettings(stubSettings{id: "Bad ID/../x"})
	if len(ConsoleSettings()) != 1 {
		t.Fatal("RegisterConsoleSettings rejected an invalid ID; validation belongs to the console, not the setter")
	}
}

func TestConsoleSettingsContextAllows(t *testing.T) {
	scoped := ConsoleSettingsContext{
		Identity:    "ops@example.com",
		Permissions: []Permission{PermSettingsRead},
	}
	if !scoped.Allows(PermSettingsRead) {
		t.Error("Allows(settings:read) = false for a session that holds it")
	}
	if scoped.Allows(PermSettingsWrite) {
		t.Error("Allows(settings:write) = true for a session that does NOT hold it")
	}

	// A policy-less session holds everything — including permissions defined
	// after this build, which is why FullAccess is a flag and not just a longer
	// list. A panel must not start refusing a full-access operator the day the
	// core defines a new permission.
	full := ConsoleSettingsContext{Permissions: nil, FullAccess: true}
	if !full.Allows(PermSettingsWrite) || !full.Allows(Permission("some:future-permission")) {
		t.Error("a FullAccess context denied a permission; a policy-less session holds every permission, present and future")
	}
}

// settings:write must be a permission the core defines, or a route table / policy
// referencing it would silently never match a granted permission.
func TestSettingsWriteIsAKnownPermission(t *testing.T) {
	if !slices.Contains(AllPermissions(), PermSettingsWrite) {
		t.Errorf("AllPermissions() omits %q — a permission missing from the list is never reported to the SPA nor guarded by the route-table tests", PermSettingsWrite)
	}
}
