package pgbaseline

// SetTestHookAfterSnapshot installs the after-anchor seam for the external
// integration tests (boundary test); returns a restore func. Test binary only.
func SetTestHookAfterSnapshot(f func()) (restore func()) {
	prev := testHookAfterSnapshot
	testHookAfterSnapshot = f
	return func() { testHookAfterSnapshot = prev }
}
