//go:build unix

package baseline

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// pointerLockName is the lock file PublishCurrentPointer serializes on. It sits
// beside the pointer it guards, is dot-prefixed and does not parse as a
// timestamp (so discovery skips it), and is a REGULAR file, so isPointerArtifact
// must name it explicitly or the S3 upload would publish it as snapshot data.
const pointerLockName = "." + CurrentLinkName + ".lock"

// pointerLockTimeout bounds the wait. A publish is a symlink and a rename, so a
// peer holds this for microseconds; anything approaching this bound is a stuck
// process, and refusing beats blocking a baseline run behind it.
const pointerLockTimeout = 5 * time.Second

// lockPointer serializes pointer publication within a baselines root.
//
// The rule it protects is read-then-write: decide whether this snapshot
// outranks the newest complete one, then rename the link. Without the lock both
// halves are separately correct and the pair is not, because a peer can
// complete a newer snapshot inside the gap and lose to a rename that started
// earlier. Narrowing that gap is not closing it: measured at 118 in 400
// concurrent pairs comparing against the pointer, and still 12 in 400 under CPU
// load comparing against the directory listing.
//
// flock rather than an O_EXCL sentinel, for the reason the rest of this project
// picks it: the kernel releases it when the process dies, so a crash mid-publish
// needs no staleness rule and leaves nothing for an operator to clear.
func lockPointer(root string) (func(), error) {
	path := filepath.Join(root, pointerLockName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open pointer lock %s: %w", path, err)
	}
	deadline := time.Now().Add(pointerLockTimeout)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() {
				_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
				f.Close()
			}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			f.Close()
			return nil, fmt.Errorf("lock %s: %w", path, err)
		}
		if time.Now().After(deadline) {
			f.Close()
			return nil, fmt.Errorf("another baseline held the pointer lock %s for longer than %s", path, pointerLockTimeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
