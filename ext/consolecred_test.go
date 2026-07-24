package ext

import "testing"

type stubCredBackend struct{ id string }

func (s stubCredBackend) Verify(username, password string) *Credential {
	if username == "known" && password == "pw" {
		return &Credential{Identity: s.id}
	}
	return nil
}

func TestConsoleCredentialDefaultsNil(t *testing.T) {
	if ConsoleCredential() != nil {
		t.Fatal("ConsoleCredential() != nil by default — the OSS build must have no backend installed")
	}
}

func TestSetConsoleCredentialRoundTrip(t *testing.T) {
	SetConsoleCredentialProvider(stubCredBackend{id: "alice"})
	t.Cleanup(func() { SetConsoleCredentialProvider(nil) })

	b := ConsoleCredential()
	if b == nil {
		t.Fatal("ConsoleCredential() = nil after SetConsoleCredentialProvider")
	}
	if cred := b.Verify("known", "pw"); cred == nil || cred.Identity != "alice" {
		t.Errorf("Verify(known,pw) = %+v, want an admitted alice credential", cred)
	}
	if cred := b.Verify("known", "wrong"); cred != nil {
		t.Errorf("Verify(known,wrong) = %+v, want nil (uniform rejection)", cred)
	}

	SetConsoleCredentialProvider(nil)
	if ConsoleCredential() != nil {
		t.Error("SetConsoleCredentialProvider(nil) did not clear the backend")
	}
}
