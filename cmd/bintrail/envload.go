package main

import (
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
)

// bindCommandEnv forwards to cli.BindCommandEnv, kept as a package-local helper
// so every command's init() can keep calling bindCommandEnv(cmd) unchanged.
// The env-loading logic and the flag-to-env table (cli.EnvBindings) moved to
// internal/cli (#529) so a second binary can reuse them.
func bindCommandEnv(cmd *cobra.Command) {
	cli.BindCommandEnv(cmd)
}
