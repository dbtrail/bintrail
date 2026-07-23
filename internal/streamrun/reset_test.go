package streamrun

import (
	"strings"
	"testing"
)

// TestResetJumpDetail_positionNoop: discarding a checkpoint and restarting at
// the exact same file:pos skips nothing — no loss record must be written.
func TestResetJumpDetail_positionNoop(t *testing.T) {
	old := &streamState{mode: "position", binlogFile: "mysql-bin.000043", binlogPos: 1000}
	noop, detail := resetJumpDetail(old, "position", "mysql-bin.000043", 1000, "")
	if !noop {
		t.Fatalf("same-position reset must be a noop, got detail %q", detail)
	}
}

// TestResetJumpDetail_positionJump: a reset that lands anywhere else is a jump
// and the detail must name both coordinates so gap_lost_detail is actionable.
func TestResetJumpDetail_positionJump(t *testing.T) {
	old := &streamState{mode: "position", binlogFile: "mysql-bin.000043", binlogPos: 1000}
	noop, detail := resetJumpDetail(old, "position", "mysql-bin.000044", 4, "")
	if noop {
		t.Fatal("jump to a different position must not be a noop")
	}
	for _, want := range []string{"--reset", "mysql-bin.000043:1000", "mysql-bin.000044:4", "permanently lost"} {
		if !strings.Contains(detail, want) {
			t.Errorf("detail %q missing %q", detail, want)
		}
	}
}

// TestResetJumpDetail_gtidNoop: GTID sets are compared structurally, so
// formatting differences (UUID case) must not fake a jump.
func TestResetJumpDetail_gtidNoop(t *testing.T) {
	old := &streamState{mode: "gtid", gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"}
	noop, _ := resetJumpDetail(old, "gtid", "", 0, "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5")
	if !noop {
		t.Fatal("structurally equal GTID sets must be a noop")
	}
}

// TestResetJumpDetail_gtidJump: a different executed set is a jump and the
// detail carries both sets.
func TestResetJumpDetail_gtidJump(t *testing.T) {
	old := &streamState{mode: "gtid", gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"}
	noop, detail := resetJumpDetail(old, "gtid", "", 0, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9000")
	if noop {
		t.Fatal("different GTID sets must not be a noop")
	}
	for _, want := range []string{"gtid_set 3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "gtid_set 3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9000"} {
		if !strings.Contains(detail, want) {
			t.Errorf("detail %q missing %q", detail, want)
		}
	}
}

// TestResetJumpDetail_crossModeIsJump: position→gtid (and the reverse) have no
// comparable coordinates; conservatively treat the discard as a jump so the
// loss is recorded rather than silently assumed away.
func TestResetJumpDetail_crossModeIsJump(t *testing.T) {
	old := &streamState{mode: "position", binlogFile: "mysql-bin.000043", binlogPos: 1000}
	if noop, _ := resetJumpDetail(old, "gtid", "", 0, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"); noop {
		t.Fatal("cross-mode reset must be treated as a jump")
	}
	oldGTID := &streamState{mode: "gtid", gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"}
	if noop, _ := resetJumpDetail(oldGTID, "position", "mysql-bin.000050", 4, ""); noop {
		t.Fatal("cross-mode reset must be treated as a jump")
	}
}
