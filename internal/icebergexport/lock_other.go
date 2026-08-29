//go:build !unix

package icebergexport

// lockWarehouse is a no-op where flock is unavailable; bintrail ships on
// Linux, and this keeps the package compiling elsewhere.
func lockWarehouse(string) (func(), error) { return func() {}, nil }
