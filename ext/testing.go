package ext

// ResetForTest clears every extension registry (doctor checks, source jobs,
// agent commands, console views and settings panels) so tests in other packages
// can register fixtures without polluting later tests in the same binary. Test
// helper ONLY — the registries are startup-only by contract (set once from
// main(), never mutated during command execution), and production code must
// never call this.
//
// It does NOT touch the single-provider slots (SetConsoleView, SetConsoleAuth,
// SetAuditSink): those have their own setters a test undoes by passing nil, and
// clearing them here would silently uninstall a fixture a test installed with
// the setter it is exercising.
func ResetForTest() {
	doctorChecks = nil
	sourceJobs = nil
	agentCommands = map[string]AgentCommandFunc{}
	registeredConsoleViews = nil
	registeredConsoleSettings = nil
}
