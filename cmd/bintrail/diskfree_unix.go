//go:build darwin || linux

package main

import "syscall"

// diskFree returns the bytes available to non-root users on the filesystem
// containing path. Stdlib syscall, not golang.org/x/sys (a transitive dep we
// must not import directly).
func diskFree(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}
