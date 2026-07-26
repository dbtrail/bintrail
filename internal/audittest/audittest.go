// Package audittest holds the canonical list of audit emissions the core
// must produce, plus the recording sink the contract tests drive it with.
//
// Why a shared package instead of one list per test: the audit seam
// (ext.AuditSink) is the one thing an embedding distribution relies on to
// know WHO read WHICH historical rows. Before #945 the wiring covered the
// CLI's query/recover and the MCP tools only, while ext/audit.go's own
// contract named the shim, reconstruct and the console — and no test pinned
// any of it, so a refactor could drop an emission with CI green. Required
// below is that contract, machine-checked.
//
// The list is split by Owner rather than exercised from one place because
// each surface is only drivable from inside its own package (unexported
// handlers, in-package fakes) or needs a live MySQL. Every owner's
// contract test calls CheckCoverage with the pairs it actually observed,
// so a dropped emission fails that owner's test and an emission nobody
// exercises fails as uncovered.
//
// This package is imported by tests only; it is never linked into a binary.
package audittest

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// Pair identifies one audit emission: the ext.AuditEvent Surface and Action
// an operator (or an embedding distribution's sink) can rely on seeing.
type Pair struct {
	Surface string
	Action  string
}

func (p Pair) String() string { return p.Surface + "/" + p.Action }

// Owner names the contract test responsible for exercising a requirement.
// It is the test's location, not a taxonomy of its own — the surface
// vocabulary lives in ext.AuditEvent.Surface.
type Owner string

const (
	// OwnerCLI: internal/cli/audit_contract_integration_test.go. The CLI
	// commands connect through a real DSN (config.Connect), so their contract
	// test needs the integration MySQL — which CI runs on every pull request
	// as a required check, not only on tagged refs.
	OwnerCLI Owner = "internal/cli (integration)"
	// OwnerConsoleUnit: internal/console/audit_contract_test.go — the HTTP
	// handlers that a sqlmock index or a stub controller can serve.
	OwnerConsoleUnit Owner = "internal/console (unit)"
	// OwnerConsoleIntegration: internal/console/audit_contract_integration_test.go
	// — the endpoints that need a real index plus a baseline snapshot.
	OwnerConsoleIntegration Owner = "internal/console (integration)"
	// OwnerShim: internal/shim/audit_contract_test.go — Handler.HandleQuery
	// driven with the package's injected fetcher/resolver fakes.
	OwnerShim Owner = "internal/shim (unit)"
	// OwnerMCP: internal/mcptools/audit_contract_test.go — the tool handlers
	// over a sqlmock index.
	OwnerMCP Owner = "internal/mcptools (unit)"
)

// Requirement is one emission the core must make, and where it is pinned.
type Requirement struct {
	Pair
	Owner Owner
	// Why states what the operator loses if this emission disappears.
	Why string
}

// Required is the exact set of audit emissions the core guarantees. Adding a
// surface means adding a row here AND exercising it in that owner's contract
// test; removing one means deleting the row and correcting the ext/audit.go
// docstring, which enumerates the same set in prose.
var Required = []Requirement{
	{
		Pair:  Pair{Surface: "cli", Action: "query.run"},
		Owner: OwnerCLI,
		Why:   "reading indexed row images (before/after) from the command line",
	},
	{
		Pair:  Pair{Surface: "cli", Action: "recover.generate"},
		Owner: OwnerCLI,
		Why:   "producing a reversal script — the mutation artifact recover exists to record",
	},
	{
		Pair:  Pair{Surface: "cli", Action: "recover.cascade"},
		Owner: OwnerCLI,
		Why:   "producing a reversal script that also SYNTHESIZES cascade-deleted child rows",
	},
	{
		Pair:  Pair{Surface: "cli", Action: "reconstruct.run"},
		Owner: OwnerCLI,
		Why:   "reading a baseline snapshot's rows and folding deltas onto them (point-in-time row state)",
	},
	{
		Pair:  Pair{Surface: "cli", Action: "verify.explain"},
		Owner: OwnerCLI,
		Why:   "the only verify output that prints row-level data (the differing rows of a mismatch)",
	},
	{
		Pair:  Pair{Surface: "mcp", Action: "query.run"},
		Owner: OwnerMCP,
		Why:   "an LLM client reading indexed row images",
	},
	{
		Pair:  Pair{Surface: "mcp", Action: "recover.generate"},
		Owner: OwnerMCP,
		Why:   "an LLM client generating a reversal script",
	},
	{
		Pair:  Pair{Surface: "shim", Action: "timetravel.query"},
		Owner: OwnerShim,
		Why:   "network time-travel reads (_flashback/_snapshot/_diff) by an authenticated tenant",
	},
	{
		Pair:  Pair{Surface: "console", Action: "query.run"},
		Owner: OwnerConsoleUnit,
		Why:   "reading indexed row images from the web console",
	},
	{
		Pair:  Pair{Surface: "console", Action: "recover.generate"},
		Owner: OwnerConsoleUnit,
		Why:   "generating a reversal script from the web console",
	},
	{
		Pair:  Pair{Surface: "console", Action: "verify.explain"},
		Owner: OwnerConsoleUnit,
		Why:   "the console's row-level mismatch drill-down",
	},
	{
		Pair:  Pair{Surface: "console", Action: "authz.denied"},
		Owner: OwnerConsoleUnit,
		Why:   "an authorization refusal — one of the two console events that are not data reads",
	},
	{
		Pair:  Pair{Surface: "console", Action: "profile.denied"},
		Owner: OwnerConsoleUnit,
		Why:   "a data-profile refusal: an unknown profile, or a surface that cannot honor redaction",
	},
	{
		Pair:  Pair{Surface: "console", Action: "reconstruct.run"},
		Owner: OwnerConsoleIntegration,
		Why:   "point-in-time row state served by the console's Time-travel tab",
	},
	{
		Pair:  Pair{Surface: "console", Action: "recover.cascade"},
		Owner: OwnerConsoleIntegration,
		Why:   "cascade reversal (with synthesized child rows) served by the console",
	},
}

// RequiredFor returns the pairs owner must exercise.
func RequiredFor(owner Owner) []Pair {
	var out []Pair
	for _, r := range Required {
		if r.Owner == owner {
			out = append(out, r.Pair)
		}
	}
	return out
}

// Recorder is an ext.AuditSink that keeps every event for assertions. Safe
// for concurrent use, as the AuditSink contract demands.
type Recorder struct {
	mu     sync.Mutex
	events []ext.AuditEvent
}

// Record implements ext.AuditSink.
func (r *Recorder) Record(_ context.Context, ev ext.AuditEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

// Events returns a copy of everything recorded so far.
func (r *Recorder) Events() []ext.AuditEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ext.AuditEvent{}, r.events...)
}

// Pairs returns the (surface, action) pairs recorded so far, in order.
func (r *Recorder) Pairs() []Pair {
	evs := r.Events()
	out := make([]Pair, 0, len(evs))
	for _, ev := range evs {
		out = append(out, Pair{Surface: ev.Surface, Action: ev.Action})
	}
	return out
}

// Reset drops recorded events so one recorder can serve a whole table of
// subtests.
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

// Install registers a Recorder as the process-wide audit sink and removes it
// when the test ends.
//
// The sink is process-wide, so a test that installs one must NOT call
// t.Parallel() — two parallel tests would observe each other's events. The
// swap itself is atomic (ext keeps the sink behind an atomic.Pointer), so
// driving a surface from another goroutine is race-safe.
func Install(t *testing.T) *Recorder {
	t.Helper()
	rec := &Recorder{}
	ext.SetAuditSink(rec)
	t.Cleanup(func() { ext.SetAuditSink(nil) })
	return rec
}

// CheckCoverage fails t unless the observed pairs are exactly the pairs owner
// is required to emit: every requirement seen at least once (a dropped
// emission site fails here), and nothing observed that Required does not
// list (a NEW audited surface has to be declared, not smuggled in).
func CheckCoverage(t *testing.T, owner Owner, observed []Pair) {
	t.Helper()
	want := map[Pair]bool{}
	for _, p := range RequiredFor(owner) {
		want[p] = false
	}
	if len(want) == 0 {
		t.Fatalf("audittest: no requirements registered for owner %q — did the owner constant drift?", owner)
	}
	known := map[Pair]bool{}
	for _, r := range Required {
		known[r.Pair] = true
	}
	var unexpected []string
	for _, p := range observed {
		if _, ok := want[p]; ok {
			want[p] = true
			continue
		}
		if !known[p] {
			unexpected = append(unexpected, p.String())
		}
	}
	var missing []string
	for p, seen := range want {
		if !seen {
			missing = append(missing, p.String())
		}
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	for _, m := range missing {
		t.Errorf("audit contract: %s must emit %s, but exercising it recorded nothing — "+
			"either the ext.Record call was dropped, or audittest.Required is stale", owner, m)
	}
	for _, u := range unexpected {
		t.Errorf("audit contract: %s emitted %s, which audittest.Required does not list — "+
			"add it there (and to the ext/audit.go docstring) so the seam's contract stays honest", owner, u)
	}
}
