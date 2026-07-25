package mcpext

// ResetForTest clears the provider registry so tests in other packages can
// register fixtures without polluting later tests in the same binary. Mirrors
// ext.ResetForTest. Test helper ONLY — the registry is startup-only by
// contract (set once from main(), never mutated during command execution), and
// production code must never call this.
func ResetForTest() { providers = nil }
