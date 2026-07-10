package cliapp

import (
	"testing"

	"github.com/spf13/cobra"
)

// #820: --proxysql-admin is opt-in. It must exist on doctor and never be
// required — the check it enables is advisory, and doctor must keep working
// for deployments with no ProxySQL at all.
func TestDoctorProxySQLAdminFlagOptional(t *testing.T) {
	f := doctorCmd.Flags().Lookup("proxysql-admin")
	if f == nil {
		t.Fatal("doctor is missing the --proxysql-admin flag")
	}
	if f.DefValue != "" {
		t.Errorf("--proxysql-admin default = %q, want empty (opt-in)", f.DefValue)
	}
	if req, ok := f.Annotations[cobra.BashCompOneRequiredFlag]; ok && len(req) > 0 && req[0] == "true" {
		t.Error("--proxysql-admin must not be marked required")
	}
}
