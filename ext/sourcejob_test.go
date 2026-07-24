package ext

import (
	"context"
	"testing"
	"time"
)

func TestRunSourceJobsNoRegistrationIsNoop(t *testing.T) {
	orig := sourceJobs
	sourceJobs = nil
	t.Cleanup(func() { sourceJobs = orig })

	// Must not panic.
	RunSourceJobs(context.Background(), SourceJobInfo{SourceDSN: "s", IndexDSN: "i", Flavor: "mysql"})
}

func TestRunSourceJobsRunsEveryJobWithInfo(t *testing.T) {
	orig := sourceJobs
	sourceJobs = nil
	t.Cleanup(func() { sourceJobs = orig })

	// Jobs run on their own goroutines (no ordering guarantee), so collect
	// through a channel and assert both fire with the exact SourceJobInfo.
	got := make(chan SourceJobInfo, 2)
	RegisterSourceJob(func(_ context.Context, src SourceJobInfo) { got <- src })
	RegisterSourceJob(func(_ context.Context, src SourceJobInfo) { got <- src })

	want := SourceJobInfo{SourceDSN: "user:pass@tcp(h:3306)/db", IndexDSN: "idx-dsn", Flavor: "mariadb"}
	RunSourceJobs(context.Background(), want)

	for i := range 2 {
		select {
		case src := <-got:
			if src != want {
				t.Errorf("job received %+v, want %+v", src, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for registered job %d to run", i+1)
		}
	}
}

// TestRunSourceJobsPanicDoesNotPropagate pins the H-contract the core
// enforces: a panicking job is recovered on its own goroutine (never
// propagated to the daemon) and sibling jobs still run.
func TestRunSourceJobsPanicDoesNotPropagate(t *testing.T) {
	orig := sourceJobs
	sourceJobs = nil
	t.Cleanup(func() { sourceJobs = orig })

	survivorRan := make(chan struct{})
	RegisterSourceJob(func(context.Context, SourceJobInfo) { panic("source job boom") })
	RegisterSourceJob(func(context.Context, SourceJobInfo) { close(survivorRan) })

	// Must not panic the caller — the panic is contained per-goroutine. An
	// uncontained goroutine panic would crash the whole test binary, so the
	// package passing at all is the real assertion.
	RunSourceJobs(context.Background(), SourceJobInfo{Flavor: "mysql"})

	select {
	case <-survivorRan:
	case <-time.After(5 * time.Second):
		t.Fatal("surviving job did not run after a sibling panicked")
	}
}
