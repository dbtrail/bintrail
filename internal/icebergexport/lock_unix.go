//go:build unix

package icebergexport

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockWarehouse takes a non-blocking flock on path. The kernel releases it
// when the process dies, so no staleness rule is needed.
func lockWarehouse(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open warehouse lock %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("another Iceberg export is running on this warehouse (lock %s is held): %w", path, err)
		}
		return nil, fmt.Errorf("lock warehouse %s: %w", path, err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}
