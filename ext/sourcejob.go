package ext

import "context"

// SourceJobInfo describes one capture source at daemon startup, handed to
// every registered source job. Flavor names the source family ("mysql",
// "mariadb", "postgres").
type SourceJobInfo struct {
	SourceDSN string
	IndexDSN  string
	Flavor    string
}

// sourceJobs is empty in the OSS build — RunSourceJobs is a no-op.
var sourceJobs []func(ctx context.Context, src SourceJobInfo)

// RegisterSourceJob registers a background job to run alongside each capture
// source's daemon lifecycle. Same startup-only contract as the other seams:
// call from main() before command dispatch; not safe for concurrent use with
// command execution. Jobs are invoked in registration order.
func RegisterSourceJob(job func(ctx context.Context, src SourceJobInfo)) {
	sourceJobs = append(sourceJobs, job)
}

// RunSourceJobs invokes every registered source job. Called by the core at
// daemon startup wiring points, with a context bound to the daemon lifecycle;
// jobs are secondary and must never be fatal — a job that needs to keep
// working spawns its own goroutine and returns promptly, and a job that fails
// logs rather than panics. Safe to call with nothing registered.
func RunSourceJobs(ctx context.Context, src SourceJobInfo) {
	for _, job := range sourceJobs {
		job(ctx, src)
	}
}
