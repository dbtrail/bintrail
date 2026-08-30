package console

import (
	"context"
	"runtime"
	"testing"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

// The view DDL binds a Parquet footer per file in the layout, which is latency,
// not CPU (#1535). The panel widens DuckDB's thread count across that bind and
// the sandbox puts the daemon budget back before the user's statement runs.
//
// Both halves need a guard, and they fail in opposite directions: without the
// widen the bind runs at the daemon budget and a large archive times out;
// without the restore the user's statement executes on a wide pool in a process
// that may also be capturing. The restore half is readable off the finished
// session. The widen half is NOT — it is deliberately overwritten a few
// statements later — which is why sqlPanelBindThreads is a func var: consulting
// it is the only observable trace that the widen ran at all.

// TestSQLPanelBindWidensThreadsThenRestoresTheDaemonBudget drives the real
// openSandboxedSession over a real local baseline and asserts both halves.
func TestSQLPanelBindWidensThreadsThenRestoresTheDaemonBudget(t *testing.T) {
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)

	consulted := 0
	orig := sqlPanelBindThreads
	sqlPanelBindThreads = func() int { consulted++; return 9 }
	t.Cleanup(func() { sqlPanelBindThreads = orig })

	db, cleanup, err := openSandboxedSession(context.Background(),
		panelInput(nil, baselineRoot, baselinePath), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	if consulted != 1 {
		t.Errorf("bind-thread count consulted %d times, want exactly 1: the widen "+
			"before the view DDL is what keeps a large archive inside the setup "+
			"timeout", consulted)
	}

	// The value the SESSION ends on, which is the one the user's statement runs
	// under. Asserted against the daemon budget rather than against 9, so the
	// test fails if the widen leaks past the sandbox.
	var got int
	if err := db.QueryRow(`SELECT current_setting('threads')`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if want := duckdbutil.DefaultTuning().Threads; got != want {
		t.Errorf("session executes statements with threads = %d, want the daemon "+
			"budget %d: the bind widening must not survive into the query", got, want)
	}
}

// TestSQLPanelBindThreadsUntouchedWithNoDDL pins the widen INSIDE the
// build-the-views branch. A statement that names no view (SELECT 1) builds
// nothing, so there is no footer to read and no reason to touch the pool.
func TestSQLPanelBindThreadsUntouchedWithNoDDL(t *testing.T) {
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)

	consulted := 0
	orig := sqlPanelBindThreads
	sqlPanelBindThreads = func() int { consulted++; return 9 }
	t.Cleanup(func() { sqlPanelBindThreads = orig })

	_, cleanup, err := openSandboxedSession(context.Background(),
		panelInput(nil, baselineRoot, baselinePath), views.ViewSet{})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	if consulted != 0 {
		t.Errorf("bind-thread count consulted %d times for a session that builds "+
			"no view, want 0", consulted)
	}
}

// TestDefaultBindThreads pins the two properties the ceiling exists for: a
// one-core container still overlaps more than one S3 round trip, and no host
// opens an unbounded number of them.
func TestDefaultBindThreads(t *testing.T) {
	got := defaultBindThreads()
	if got < 2 {
		t.Errorf("defaultBindThreads() = %d on a %d-core host; below 2 the bind "+
			"overlaps nothing and is the state #1535 measured at 124.9s",
			got, runtime.NumCPU())
	}
	if got > 16 {
		t.Errorf("defaultBindThreads() = %d, want at most 16: these threads wait "+
			"on object storage, and the ceiling is what keeps a large host from "+
			"opening an unbounded number of reads", got)
	}
}
