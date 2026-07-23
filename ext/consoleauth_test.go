package ext

import (
	"net/http"
	"testing"
	"time"
)

// The issuer contract is a plain func type — keep a compile-time pin on its
// exact shape so an accidental signature change fails here, not only in the
// console wiring.
var _ ConsoleSessionIssuer = func(string, *AccessPolicy) (string, time.Time, error) {
	return "", time.Time{}, nil
}

type stubConsoleAuth struct{}

func (stubConsoleAuth) DisplayName() string { return "Example SSO" }
func (stubConsoleAuth) Handler(prefix string, issue ConsoleSessionIssuer) http.Handler {
	return http.NotFoundHandler()
}

func TestConsoleAuthDefaultsNil(t *testing.T) {
	if ConsoleAuth() != nil {
		t.Fatal("ConsoleAuth() != nil by default — the OSS build must have no provider installed")
	}
}

func TestSetConsoleAuthRoundTrip(t *testing.T) {
	SetConsoleAuth(stubConsoleAuth{})
	t.Cleanup(func() { SetConsoleAuth(nil) })

	p := ConsoleAuth()
	if p == nil {
		t.Fatal("ConsoleAuth() = nil after SetConsoleAuth")
	}
	if got := p.DisplayName(); got != "Example SSO" {
		t.Errorf("DisplayName() = %q, want %q", got, "Example SSO")
	}

	SetConsoleAuth(nil)
	if ConsoleAuth() != nil {
		t.Error("SetConsoleAuth(nil) did not clear the provider")
	}
}
