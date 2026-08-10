package ext

import (
	"context"
	"net/http"
	"testing"

	"github.com/dbtrail/dbtrail/indexquery"
)

// Compile-time pin on the Fetch field's exact shape: an accidental signature
// change (e.g. dropping the *QueryPlan return) fails here, not only in the
// console wiring that constructs a ConsoleQueryContext.
var _ = func(cqc ConsoleQueryContext) {
	var _ func(context.Context, indexquery.Options) ([]indexquery.ResultRow, *indexquery.QueryPlan, error) = cqc.Fetch
}

type stubConsoleView struct{ id string }

func (s stubConsoleView) ID() string                      { return s.id }
func (stubConsoleView) Label() string                     { return "Example View" }
func (stubConsoleView) Script() string                    { return "/ext/example/view.js" }
func (stubConsoleView) StaticHandler(string) http.Handler { return http.NotFoundHandler() }
func (stubConsoleView) DataHandler(string, ConsoleQueryContextFunc) http.Handler {
	return http.NotFoundHandler()
}

func TestConsoleViewDefaultsNil(t *testing.T) {
	if ConsoleView() != nil {
		t.Fatal("ConsoleView() != nil by default — the OSS build must have no provider installed")
	}
}

func TestSetConsoleViewRoundTrip(t *testing.T) {
	SetConsoleView(stubConsoleView{id: "example"})
	t.Cleanup(func() { SetConsoleView(nil) })

	p := ConsoleView()
	if p == nil {
		t.Fatal("ConsoleView() = nil after SetConsoleView")
	}
	if got := p.ID(); got != "example" {
		t.Errorf("ID() = %q, want %q", got, "example")
	}
	if got := p.Label(); got != "Example View" {
		t.Errorf("Label() = %q, want %q", got, "Example View")
	}

	SetConsoleView(nil)
	if ConsoleView() != nil {
		t.Error("SetConsoleView(nil) did not clear the provider")
	}
}

// SetConsoleView must never reject a bad ID — validation is the console's job at
// mount time, so a provider with a typo'd ID installs cleanly here and the
// console skips it (degrade, don't crash the daemon).
func TestSetConsoleViewDoesNotValidateID(t *testing.T) {
	SetConsoleView(stubConsoleView{id: "Bad ID/../x"})
	t.Cleanup(func() { SetConsoleView(nil) })
	if ConsoleView() == nil {
		t.Fatal("SetConsoleView rejected a provider with an invalid ID; validation belongs to the console, not the setter")
	}
}

func TestConsoleViewsEmptyByDefault(t *testing.T) {
	t.Cleanup(ResetForTest)
	if got := ConsoleViews(); len(got) != 0 {
		t.Fatalf("ConsoleViews() = %v, want empty — the OSS build installs no extension view", got)
	}
}

// The registry must be additive AND must keep the legacy slot working: an
// embedding build that already calls SetConsoleView keeps its view, and a build
// that also registers one gets both rather than whichever wiring ran last.
func TestConsoleViewsRegistryIsAdditiveAndKeepsTheLegacySlot(t *testing.T) {
	t.Cleanup(ResetForTest)
	SetConsoleView(stubConsoleView{id: "legacy"})
	t.Cleanup(func() { SetConsoleView(nil) })
	RegisterConsoleView(stubConsoleView{id: "first"})
	RegisterConsoleView(stubConsoleView{id: "second"})

	got := ConsoleViews()
	if len(got) != 3 {
		t.Fatalf("ConsoleViews() has %d providers, want 3 (legacy slot + two registered)", len(got))
	}
	// Order is part of the contract: the nav must not shuffle between restarts.
	for i, want := range []string{"legacy", "first", "second"} {
		if got[i].ID() != want {
			t.Errorf("ConsoleViews()[%d].ID() = %q, want %q", i, got[i].ID(), want)
		}
	}
	// The legacy accessor keeps reporting the slot only — an existing caller's
	// meaning must not shift under it.
	if p := ConsoleView(); p == nil || p.ID() != "legacy" {
		t.Errorf("ConsoleView() = %v, want the legacy-slot provider", p)
	}
}

// With nothing in the legacy slot the registry stands alone — a build that only
// ever calls RegisterConsoleView must not need SetConsoleView first.
func TestConsoleViewsRegistryWithoutLegacySlot(t *testing.T) {
	t.Cleanup(ResetForTest)
	RegisterConsoleView(stubConsoleView{id: "only"})
	got := ConsoleViews()
	if len(got) != 1 || got[0].ID() != "only" {
		t.Fatalf("ConsoleViews() = %v, want exactly the registered provider", got)
	}
	if ConsoleView() != nil {
		t.Error("ConsoleView() reports a provider that was never installed in the slot")
	}
}

func TestRegisterConsoleViewIgnoresNil(t *testing.T) {
	t.Cleanup(ResetForTest)
	RegisterConsoleView(nil)
	if got := ConsoleViews(); len(got) != 0 {
		t.Fatalf("ConsoleViews() = %v after registering nil, want empty", got)
	}
}

func TestValidConsoleViewID(t *testing.T) {
	valid := []string{"a", "example", "my-view", "view2", "0", "a-b-c-1"}
	for _, id := range valid {
		if !ValidConsoleViewID(id) {
			t.Errorf("ValidConsoleViewID(%q) = false, want true", id)
		}
	}
	invalid := []string{
		"",         // empty
		"Example",  // uppercase
		"my_view",  // underscore
		"my view",  // space
		"a/b",      // slash — would break the URL mount
		"a.b",      // dot
		"ext-x\n",  // trailing newline
		"héllo",    // non-ASCII
		"<script>", // markup — would break the DOM route
	}
	for _, id := range invalid {
		if ValidConsoleViewID(id) {
			t.Errorf("ValidConsoleViewID(%q) = true, want false", id)
		}
	}
}
