package baseline

import "testing"

// TestDefaultLockModeIsPointConsistent is the guard the whole change exists
// for. A baseline is the seed state reconstruct merges deltas onto, so a
// default that can hand back a torn snapshot makes every downstream answer
// quietly untrustworthy. If someone flips this back, this test says why it
// matters rather than just that a constant moved.
func TestDefaultLockModeIsPointConsistent(t *testing.T) {
	if !DefaultLockMode.PointConsistent() {
		t.Fatalf("DefaultLockMode = %s, which can emit a torn snapshot; a baseline that may be stitched from several instants is not a backup", DefaultLockMode)
	}
	if DefaultLockMode != LockModeFTWRL {
		t.Errorf("DefaultLockMode = %s, want %s: safe-no-lock aborts on any write-active source, so it cannot be the default", DefaultLockMode, LockModeFTWRL)
	}
}

func TestLockModeMydumperValue(t *testing.T) {
	for _, tc := range []struct {
		mode LockMode
		want string
	}{
		{LockModeFTWRL, "FTWRL"},
		{LockModeLockAll, "LOCK_ALL"},
		{LockModeSafeNoLock, "SAFE_NO_LOCK"},
		{LockModeNoLock, "NO_LOCK"},
	} {
		if got := tc.mode.MydumperValue(); got != tc.want {
			t.Errorf("%s.MydumperValue() = %q, want %q", tc.mode, got, tc.want)
		}
	}
}

// TestLockModePointConsistent: safe-no-lock counts as point-consistent because
// it ABORTS rather than write skew, so every snapshot it produces represents
// one instant. Only no-lock can hand back skew without saying so.
func TestLockModePointConsistent(t *testing.T) {
	for mode, want := range map[LockMode]bool{
		LockModeFTWRL:      true,
		LockModeLockAll:    true,
		LockModeSafeNoLock: true,
		LockModeNoLock:     false,
	} {
		if got := mode.PointConsistent(); got != want {
			t.Errorf("%s.PointConsistent() = %v, want %v", mode, got, want)
		}
	}
}

// TestLockModeNeedsElevatedPrivileges pins the empirical finding: measured
// against mydumper v1.0.3-1 and MySQL 8.0, FTWRL fails for a SELECT +
// REPLICATION CLIENT user on LOCK INSTANCE FOR BACKUP, while SAFE_NO_LOCK and
// NO_LOCK both succeed. Callers use this to probe privileges BEFORE launching
// mydumper, which segfaults on a half-granted state.
func TestLockModeNeedsElevatedPrivileges(t *testing.T) {
	for mode, want := range map[LockMode]bool{
		LockModeFTWRL:      true,
		LockModeLockAll:    true,
		LockModeSafeNoLock: false,
		LockModeNoLock:     false,
	} {
		if got := mode.NeedsElevatedPrivileges(); got != want {
			t.Errorf("%s.NeedsElevatedPrivileges() = %v, want %v", mode, got, want)
		}
	}
}

// TestParseLockModeRejectsUnknown: a typo must not land on a weaker mode. The
// difference between these values is whether a wrong answer can ship
// unannounced, so "close enough" parsing is a correctness bug, not a UX one.
func TestParseLockModeRejectsUnknown(t *testing.T) {
	for _, in := range []string{"FTWRL", "ftwrl ", "no_lock", "nolock", "safe", "none", "yes"} {
		if got, err := ParseLockMode(in); err == nil {
			t.Errorf("ParseLockMode(%q) = %s with no error; a near-miss must be refused, not guessed", in, got)
		}
	}
}

func TestParseLockMode(t *testing.T) {
	// Empty means "operator said nothing", which must resolve to the default
	// rather than to the zero value of the type (which is not a valid mode).
	if got, err := ParseLockMode(""); err != nil || got != DefaultLockMode {
		t.Errorf(`ParseLockMode("") = %s, %v; want %s, nil`, got, err, DefaultLockMode)
	}
	for _, want := range LockModeValues {
		got, err := ParseLockMode(string(want))
		if err != nil || got != want {
			t.Errorf("ParseLockMode(%q) = %s, %v; want %s, nil", want, got, err, want)
		}
	}
}

// TestLockModeZeroValueIsTreatedAsFTWRL: the three methods must agree on a
// value ParseLockMode never produced. MydumperValue's default arm already
// sends FTWRL, so a NeedsElevatedPrivileges that answered false would send
// FTWRL to mydumper while skipping the privilege preflight — reopening the
// segfault path internal/mydumperlock exists to make unreachable. No caller
// passes the zero value today; this keeps that from becoming load-bearing.
func TestLockModeZeroValueIsTreatedAsFTWRL(t *testing.T) {
	for _, m := range []LockMode{"", "bogus"} {
		if got := m.MydumperValue(); got != "FTWRL" {
			t.Errorf("%q.MydumperValue() = %q, want FTWRL", m, got)
		}
		if !m.NeedsElevatedPrivileges() {
			t.Errorf("%q sends FTWRL to mydumper but reports it needs no privileges; the preflight would be skipped and the crash becomes reachable", m)
		}
		if !m.PointConsistent() {
			t.Errorf("%q.PointConsistent() = false while it sends FTWRL; the three methods must agree", m)
		}
	}
}

// TestLockAllIsPointConsistent pins the property that makes lock-all worth
// having: it is the point-consistent mode reachable on managed MySQL, where
// BACKUP_ADMIN cannot be granted and ftwrl therefore cannot run at all.
// Demoting it to "weak" would leave RDS with no consistent option.
func TestLockAllIsPointConsistent(t *testing.T) {
	if !LockModeLockAll.PointConsistent() {
		t.Fatal("lock-all reported as able to emit a torn snapshot; it locks the exported tables to synchronize workers")
	}
	if !LockModeLockAll.NeedsElevatedPrivileges() {
		t.Error("lock-all reported as needing no privileges; it needs LOCK TABLES, and skipping the preflight would let mydumper fail mid-dump instead of refusing up front")
	}
	if got := LockModeLockAll.MydumperValue(); got != "LOCK_ALL" {
		t.Errorf("MydumperValue = %q, want LOCK_ALL", got)
	}
}
