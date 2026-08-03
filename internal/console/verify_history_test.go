package console

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyHistory_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-verify-history.json")
	h, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := h.List("srv1"); len(got) != 0 {
		t.Fatalf("fresh history not empty: %v", got)
	}
	for i := range 2 {
		err := h.Append(VerifyRunRecord{
			ServerID: "srv1", ServerName: "wp", Trigger: "scheduled",
			VerifyStatus: VerifyStatus{State: "succeeded", Since: fmt.Sprintf("2026-08-02T0%d:00:00Z", i),
				Summary: VerifySummary{Match: i, Total: i}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Reopen from disk: the records survive the process, newest first.
	h2, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	got := h2.List("srv1")
	if len(got) != 2 {
		t.Fatalf("want 2 records after reopen, got %d", len(got))
	}
	if got[0].Summary.Match != 1 || got[1].Summary.Match != 0 {
		t.Fatalf("List is not newest-first: %+v", got)
	}
	if got[0].Trigger != "scheduled" || got[0].ServerName != "wp" {
		t.Fatalf("record fields lost in round-trip: %+v", got[0])
	}
	if got := h2.List("other"); len(got) != 0 {
		t.Fatalf("history leaked across servers: %v", got)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("history file mode = %o, want 0600", perm)
	}
}

func TestVerifyHistory_CapDropsOldest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.json")
	h, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range VerifyHistoryCap + 5 {
		if err := h.Append(VerifyRunRecord{ServerID: "s", VerifyStatus: VerifyStatus{Summary: VerifySummary{Total: i}}}); err != nil {
			t.Fatal(err)
		}
	}
	got := h.List("s")
	if len(got) != VerifyHistoryCap {
		t.Fatalf("want cap of %d records, got %d", VerifyHistoryCap, len(got))
	}
	// Newest first: got[0] is the last appended, the tail is the oldest kept.
	if got[0].Summary.Total != VerifyHistoryCap+4 || got[len(got)-1].Summary.Total != 5 {
		t.Fatalf("cap dropped the wrong end: newest=%d oldest=%d", got[0].Summary.Total, got[len(got)-1].Summary.Total)
	}
}

// TestVerifyHistory_AppendRollsBackOnSaveFailure: a failed save must not
// leave the record in memory — List would then serve "history" that a restart
// silently rewinds, masking a permanent write failure behind a healthy panel.
func TestVerifyHistory_AppendRollsBackOnSaveFailure(t *testing.T) {
	base := t.TempDir()
	sub := filepath.Join(base, "sub")
	path := filepath.Join(sub, "h.json")
	h, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Append(VerifyRunRecord{ServerID: "s", VerifyStatus: VerifyStatus{State: "succeeded"}}); err != nil {
		t.Fatal(err)
	}

	// Make the next save fail: replace the parent directory with a plain file
	// so MkdirAll/CreateTemp cannot succeed.
	if err := os.RemoveAll(sub); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sub, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := h.Append(VerifyRunRecord{ServerID: "s", VerifyStatus: VerifyStatus{State: "failed"}}); err == nil {
		t.Fatal("Append with an unwritable path reported success")
	}
	got := h.List("s")
	if len(got) != 1 || got[0].State != "succeeded" {
		t.Fatalf("failed Append leaked into memory: %+v", got)
	}
}

func TestOpenVerifyHistory_RefusesCorruptAndNewer(t *testing.T) {
	dir := t.TempDir()
	corrupt := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenVerifyHistory(corrupt); err == nil {
		t.Fatal("corrupt history opened without error; a later save would silently truncate it")
	}

	newer := filepath.Join(dir, "newer.json")
	if err := os.WriteFile(newer, fmt.Appendf(nil, `{"version": %d, "servers": {}}`, verifyHistoryVersion+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenVerifyHistory(newer); err == nil {
		t.Fatal("newer-versioned history opened without error")
	}
}

// Found is about the FILE, not the records: "this deployment never ran the
// scheduled verifier" and "it ran and nothing failed" are both an empty
// List, and a consumer that reports on verification activity has to tell them
// apart or it states a clean record for a period nothing ever verified.
func TestVerifyHistory_FoundSeparatesAbsentFromEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-verify-history.json")

	h, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if h.Found() {
		t.Fatal("Found() is true for a history file that does not exist")
	}
	if ids := h.ServerIDs(); len(ids) != 0 {
		t.Fatalf("absent history has server ids: %v", ids)
	}

	// An existing file with zero records still counts as found — the history
	// is present, it simply has nothing in this window.
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	empty, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if !empty.Found() {
		t.Fatal("Found() is false for an existing (empty) history file")
	}

	if err := h.Append(VerifyRunRecord{
		ServerID: "srv1", Trigger: VerifyTriggerScheduled,
		VerifyStatus: VerifyStatus{State: "succeeded"},
	}); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.Found() {
		t.Fatal("Found() is false after a run was recorded")
	}
}

func TestVerifyHistory_ServerIDsEnumeratesSorted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.json")
	h, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"srv2", "srv1", "srv2"} {
		if err := h.Append(VerifyRunRecord{ServerID: id, VerifyStatus: VerifyStatus{State: "succeeded"}}); err != nil {
			t.Fatal(err)
		}
	}
	got := h.ServerIDs()
	if len(got) != 2 || got[0] != "srv1" || got[1] != "srv2" {
		t.Fatalf("ServerIDs() = %v, want [srv1 srv2]", got)
	}
}

func TestDefaultVerifyHistoryPath_SiblingOfRegistry(t *testing.T) {
	got := DefaultVerifyHistoryPath("/etc/bintrail/console-servers.yaml")
	if got != "/etc/bintrail/console-verify-history.json" {
		t.Fatalf("got %q", got)
	}
}
