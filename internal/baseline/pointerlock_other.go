//go:build !unix

package baseline

// pointerLockName is still defined off unix so isPointerArtifact and the
// discovery notes describe one filename everywhere.
const pointerLockName = "." + CurrentLinkName + ".lock"

// lockPointer is a no-op where flock is unavailable. bintrail ships on Linux
// and macOS; this keeps the package compiling elsewhere, at the cost of the
// concurrent-publisher guarantee, which needs a kernel lock to hold.
func lockPointer(string) (func(), error) { return func() {}, nil }
