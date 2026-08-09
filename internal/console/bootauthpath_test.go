package console

import (
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// recordingCredBackend is a credential provider that also implements
// ext.ConsoleBootAuthPathReceiver, so it records what the console published.
type recordingCredBackend struct {
	fakeCredBackend
	got   string
	calls int
}

func (r *recordingCredBackend) SetBootAuthPath(p string) { r.got = p; r.calls++ }

// Pin the shape an embedding build has to implement: if this signature moves,
// every out-of-tree provider silently stops receiving the path (a failed type
// assertion is not a compile error at the call site).
var _ ext.ConsoleBootAuthPathReceiver = (*recordingCredBackend)(nil)

func TestPublishesResolvedAuthPathToBackend(t *testing.T) {
	explicit := filepath.Join(t.TempDir(), "elsewhere", "console-auth.yaml")
	b := &recordingCredBackend{}
	installCredBackend(t, b)

	if _, err := New(Config{Listen: "127.0.0.1:8090", AuthPath: explicit}); err != nil {
		t.Fatal(err)
	}
	if b.got != explicit {
		t.Errorf("SetBootAuthPath(%q), want the configured %q", b.got, explicit)
	}
	if b.calls != 1 {
		t.Errorf("SetBootAuthPath called %d times, want exactly 1", b.calls)
	}
}

// With no path configured the backend must learn the DEFAULT the console fell
// back to, not an empty string — the point is that both sides open one file.
func TestPublishesDefaultAuthPathWhenUnset(t *testing.T) {
	b := &recordingCredBackend{}
	installCredBackend(t, b)

	if _, err := New(Config{Listen: "127.0.0.1:8090"}); err != nil {
		t.Fatal(err)
	}
	if want := DefaultAuthPath(); b.got != want {
		t.Errorf("SetBootAuthPath(%q), want the default %q", b.got, want)
	}
}

// A provider that does not implement the optional interface must still work:
// the type assertion is the whole feature, and a failed one cannot panic.
func TestBackendWithoutReceiverStillBuilds(t *testing.T) {
	installCredBackend(t, fakeCredBackend{user: "u", pass: "p"})
	if _, err := New(Config{Listen: "127.0.0.1:8090", AuthPath: filepath.Join(t.TempDir(), "absent.yaml")}); err != nil {
		t.Fatal(err)
	}
}

// No backend installed at all — the OSS build — must not trip the assertion
// against a nil interface.
func TestNoBackendInstalledIsFine(t *testing.T) {
	if _, err := New(Config{Listen: "127.0.0.1:8090", Token: "tok", AuthPath: filepath.Join(t.TempDir(), "absent.yaml")}); err != nil {
		t.Fatal(err)
	}
}
