package reconstruct

// StubLinkFileForTest swaps the hard-link primitive carryForward tries first,
// so an external test can force the copy fallback through the REAL fold. Every
// test machine has one filesystem, so without the stub os.Link always succeeds
// and the fold-level copy arm never executes anywhere (the same blindness that
// let `return nil` replace the copy and pass both tiers — see linkFile's doc).
// Test-only by construction: _test.go files never build into the shipped
// package.
func StubLinkFileForTest(fn func(oldname, newname string) error) (restore func()) {
	prev := linkFile
	linkFile = fn
	return func() { linkFile = prev }
}
