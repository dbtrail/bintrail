package main

import "testing"

// TestRunUp_explicitRetentionWiring pins the runUp call site — the literal
// Changed("rotate-retain") string. A typo there would make the upgrade guard
// engage even when the operator explicitly set a retention, silently ignoring
// their choice (and never dropping deep history they asked to drop). The loop
// itself (ParseSettings/StartLoop) is tested in internal/rotation; this guards
// the cmd-layer wiring into rotation.ParseSettings.
func TestRunUp_explicitRetentionWiring(t *testing.T) {
	flag := upCmd.Flags().Lookup("rotate-retain")
	if flag == nil {
		t.Fatal("--rotate-retain not registered on upCmd")
	}
	savedChanged, savedValue := flag.Changed, flag.Value.String()
	savedCfg := upRotationCfg
	savedRetain, savedInterval, savedAdd := upRotateRetain, upRotateInterval, upRotateAddFuture
	savedSource, savedConsole, savedFormat := upSourceDSN, upConsole, upFormat
	t.Cleanup(func() {
		flag.Changed = savedChanged
		_ = flag.Value.Set(savedValue)
		upRotationCfg = savedCfg
		upRotateRetain, upRotateInterval, upRotateAddFuture = savedRetain, savedInterval, savedAdd
		upSourceDSN, upConsole, upFormat = savedSource, savedConsole, savedFormat
	})

	// Make runUp exit early at the source-dsn check — AFTER the rotation
	// block has populated upRotationCfg, BEFORE any phase touches a DB.
	upSourceDSN, upConsole, upFormat = "", false, "text"
	upRotateInterval, upRotateAddFuture = "1h", 3

	// Implicit: flag never set.
	flag.Changed = false
	upRotateRetain = "30d"
	_ = runUp(upCmd, nil) // returns the source-dsn error; irrelevant here
	if upRotationCfg.Explicit {
		t.Error("Explicit must be false when --rotate-retain was never set")
	}

	// Explicit: set through the flag set, exactly like CLI/env would.
	if err := upCmd.Flags().Set("rotate-retain", "7d"); err != nil {
		t.Fatalf("Set(rotate-retain): %v", err)
	}
	_ = runUp(upCmd, nil)
	if !upRotationCfg.Explicit {
		t.Error("Explicit must be true when --rotate-retain was set — the Changed(\"rotate-retain\") call site is broken")
	}
	if upRotationCfg.RetainRaw != "7d" {
		t.Errorf("RetainRaw = %q, want 7d", upRotationCfg.RetainRaw)
	}
}

// TestUpRotateFlagsRegistered pins the flag names to the envBindings strings:
// bindCommandEnv silently skips bindings whose flag doesn't exist on the
// command, so a renamed flag would make BINTRAIL_ROTATE_RETAIN=off silently
// fail to disable rotation.
func TestUpRotateFlagsRegistered(t *testing.T) {
	for _, name := range []string{"rotate-retain", "rotate-interval", "rotate-add-future"} {
		if upCmd.Flags().Lookup(name) == nil {
			t.Errorf("upCmd is missing --%s — its BINTRAIL_* env binding would silently no-op", name)
		}
	}
}
