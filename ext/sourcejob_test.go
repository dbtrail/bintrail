package ext

import (
	"context"
	"testing"
)

func TestRunSourceJobsNoRegistrationIsNoop(t *testing.T) {
	orig := sourceJobs
	sourceJobs = nil
	t.Cleanup(func() { sourceJobs = orig })

	// Must not panic.
	RunSourceJobs(context.Background(), SourceJobInfo{SourceDSN: "s", IndexDSN: "i", Flavor: "mysql"})
}

func TestRunSourceJobsInvokesInRegistrationOrder(t *testing.T) {
	orig := sourceJobs
	sourceJobs = nil
	t.Cleanup(func() { sourceJobs = orig })

	var order []string
	var gotSrc SourceJobInfo
	RegisterSourceJob(func(_ context.Context, src SourceJobInfo) {
		order = append(order, "first")
		gotSrc = src
	})
	RegisterSourceJob(func(_ context.Context, _ SourceJobInfo) {
		order = append(order, "second")
	})

	want := SourceJobInfo{SourceDSN: "user:pass@tcp(h:3306)/db", IndexDSN: "idx-dsn", Flavor: "mysql"}
	RunSourceJobs(context.Background(), want)

	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("jobs ran %v, want [first second]", order)
	}
	if gotSrc != want {
		t.Errorf("job received %+v, want %+v", gotSrc, want)
	}
}
