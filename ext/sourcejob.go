package ext

import (
	"context"
	"log/slog"
)

// SourceJobInfo describes one capture source at daemon startup, handed to
// every registered source job. Flavor names the source family; core wiring
// currently only ever carries "mysql" or "mariadb" (the flavor the daemon
// streams with; `bintrail agent` has no flavor flag and always reports
// "mysql"). Both DSNs are populated by every core wiring point: a job that
// needs somewhere to persist what it observes can rely on IndexDSN being set.
type SourceJobInfo struct {
	SourceDSN string
	IndexDSN  string
	Flavor    string
}

// sourceJobs is empty in the OSS build — RunSourceJobs is a no-op.
var sourceJobs []func(ctx context.Context, src SourceJobInfo)

// RegisterSourceJob registers a background job to run alongside the daemon
// lifecycle. Core wiring points: `bintrail stream` (which `bintrail up`
// delegates to), `bintrail agent` in BYOS mode, and the console daemon's
// `watch` main source plus every monitor-supervised source. Same
// startup-only contract as the
// other seams: call from main() before command dispatch; not safe for
// concurrent use with command execution. Registering a nil job panics
// immediately so the misuse fails at startup, not at daemon boot.
func RegisterSourceJob(job func(ctx context.Context, src SourceJobInfo)) {
	if job == nil {
		panic("ext: nil source job")
	}
	sourceJobs = append(sourceJobs, job)
}

// RunSourceJobs launches every registered source job, each on its own
// goroutine, and returns immediately. Called by the core once per capture
// source at daemon startup, with a context bound to the daemon lifetime —
// the passed ctx
// bounds the jobs' lifetime. Jobs are secondary and must never be fatal to
// the daemon; the core enforces that here: a panicking job is recovered and
// logged, never propagated to the stream, and a slow job cannot block
// startup. Safe to call with nothing registered.
func RunSourceJobs(ctx context.Context, src SourceJobInfo) {
	for _, job := range sourceJobs {
		go func() {
			defer func() {
				if p := recover(); p != nil {
					slog.Error("ext: source job panicked", "panic", p)
				}
			}()
			job(ctx, src)
		}()
	}
}
