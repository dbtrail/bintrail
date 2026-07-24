package ext

// ResetForTest clears every extension registry (doctor checks, source jobs,
// agent commands) so tests in other packages can register fixtures without
// polluting later tests in the same binary. Test helper ONLY — the
// registries are startup-only by contract (set once from main(), never
// mutated during command execution), and production code must never call
// this.
func ResetForTest() {
	doctorChecks = nil
	sourceJobs = nil
	agentCommands = map[string]AgentCommandFunc{}
}
