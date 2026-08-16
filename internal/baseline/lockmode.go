package baseline

import "fmt"

// LockMode selects how mydumper synchronizes its worker threads onto a single
// point in time. It is shared by `bintrail dump` and the console's in-process
// baseline trigger so the two cannot drift: a snapshot's trustworthiness must
// not depend on which surface produced it.
//
// The distinction that matters is NOT "does it lock" but "can it hand back a
// TORN snapshot without saying so". A baseline is the seed state reconstruct
// merges deltas onto, so a snapshot stitched from several instants yields a
// table that never existed — and every downstream answer inherits it silently.
//
// Measured against mydumper v1.0.3-1 and a real MySQL 8.0 with a
// least-privilege user (SELECT + REPLICATION CLIENT):
//
//   - FTWRL succeeds and is genuinely point-consistent, but needs RELOAD /
//     FLUSH_TABLES plus BACKUP_ADMIN on MySQL 8.0+ (it issues LOCK INSTANCE
//     FOR BACKUP first). The least-privilege user got
//     "ERROR 1227: Access denied" on that statement.
//   - SAFE_NO_LOCK runs at least-privilege, but it does not PREVENT skew — it
//     DETECTS it: mydumper compares the binlog position before and after
//     syncing threads and, on any difference, stops with "we cannot guarantee
//     the backup to be consistent. Stopping backup due to the use of
//     SAFE_NO_LOCK." Verified: under sustained concurrent writes it aborts.
//     So it never lies, but on a write-active source it mostly refuses.
//   - NO_LOCK always succeeds and is the only mode that can silently produce a
//     torn snapshot. mydumper's own docs describe it as the choice for when
//     "you don't need a consistent backup", and DEPRECATED it in v0.18.1.
//
// Hence the default is FTWRL: SAFE_NO_LOCK is unusable as a default on the
// sources that most need a baseline, and NO_LOCK is not a backup.
type LockMode string

const (
	// LockModeFTWRL holds one global read lock only long enough for every
	// worker to open its snapshot at the same instant, then releases it.
	LockModeFTWRL LockMode = "ftwrl"
	// LockModeSafeNoLock needs no elevated privilege and refuses rather than
	// emit a torn snapshot. Expect it to abort on a write-active source.
	LockModeSafeNoLock LockMode = "safe-no-lock"
	// LockModeNoLock accepts a torn snapshot. Reachable only by explicit
	// choice; never a default and never a fallback.
	LockModeNoLock LockMode = "no-lock"
)

// DefaultLockMode is what every surface uses when the operator says nothing.
const DefaultLockMode = LockModeFTWRL

// LockModeValues lists the accepted spellings, for flag help and error text.
var LockModeValues = []LockMode{LockModeFTWRL, LockModeSafeNoLock, LockModeNoLock}

// ParseLockMode maps an operator-supplied string to a LockMode. It is
// deliberately strict: a typo must not silently land on a weaker mode, because
// the difference between them is whether a wrong answer can ship unannounced.
func ParseLockMode(s string) (LockMode, error) {
	switch LockMode(s) {
	case LockModeFTWRL:
		return LockModeFTWRL, nil
	case LockModeSafeNoLock:
		return LockModeSafeNoLock, nil
	case LockModeNoLock:
		return LockModeNoLock, nil
	case "":
		return DefaultLockMode, nil
	}
	return "", fmt.Errorf("unknown lock mode %q: want one of %s, %s, %s",
		s, LockModeFTWRL, LockModeSafeNoLock, LockModeNoLock)
}

// MydumperValue is the --sync-thread-lock-mode argument for this mode.
func (m LockMode) MydumperValue() string {
	switch m {
	case LockModeSafeNoLock:
		return "SAFE_NO_LOCK"
	case LockModeNoLock:
		return "NO_LOCK"
	default:
		return "FTWRL"
	}
}

// PointConsistent reports whether a snapshot this mode PRODUCES can be trusted
// to represent one instant. SAFE_NO_LOCK counts: it aborts instead of writing
// a torn snapshot, so anything it does write is consistent. Only NO_LOCK can
// hand back skew without saying so — which is what the footer marker and the
// downstream warnings key off.
func (m LockMode) PointConsistent() bool { return m != LockModeNoLock }

// NeedsElevatedPrivileges reports whether the mode issues statements a
// least-privilege replication user cannot run (verified empirically — see the
// type comment). Callers use this to check privileges BEFORE launching
// mydumper: the pinned build segfaults on a half-granted state rather than
// failing cleanly.
func (m LockMode) NeedsElevatedPrivileges() bool { return m == LockModeFTWRL }
