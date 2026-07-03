package forensics

// Enabled reports whether the forensics surface is available in this build.
//
// dbtrail OSS ships with forensics enabled. This indirection is the single
// entitlement seam a future enterprise-license build closes; policy lives at
// surface entry points (CLI, console, MCP, agent, poller wiring) — never
// inside this library, so closing the gate never touches mechanism code.
var Enabled = func() bool { return true }
