package ext

import (
	"context"
	"encoding/json"
	"testing"
)

// TestRegisterNilPanics pins the fail-at-startup contract: registering a nil
// handler/function/job panics immediately in main(), instead of surfacing
// hours later as a nil dereference at first dispatch.
func TestRegisterNilPanics(t *testing.T) {
	cases := map[string]func(){
		"agent command": func() { RegisterAgentCommand("ext_test_nil", nil) },
		"doctor check":  func() { RegisterDoctorCheck(nil) },
		"source job":    func() { RegisterSourceJob(nil) },
	}
	for name, register := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("registering a nil %s did not panic", name)
				}
			}()
			register()
		})
	}
}

// TestResetForTestClearsRegistries pins the test helper other packages'
// tests rely on for cleanup (the registries have no unregister by design).
func TestResetForTestClearsRegistries(t *testing.T) {
	origJobs, origChecks, origCmds := sourceJobs, doctorChecks, agentCommands
	t.Cleanup(func() { sourceJobs, doctorChecks, agentCommands = origJobs, origChecks, origCmds })

	RegisterSourceJob(func(context.Context, SourceJobInfo) {})
	RegisterDoctorCheck(func(context.Context, string, string) []DoctorCheck { return nil })
	RegisterAgentCommand("ext_test_reset", func(context.Context, AgentDeps, json.RawMessage) (any, error) {
		return nil, nil
	})

	ResetForTest()

	if len(sourceJobs) != 0 || len(doctorChecks) != 0 || len(agentCommands) != 0 {
		t.Fatalf("ResetForTest left registrations behind: %d jobs, %d checks, %d commands",
			len(sourceJobs), len(doctorChecks), len(agentCommands))
	}
}
