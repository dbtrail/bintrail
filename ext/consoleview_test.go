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
