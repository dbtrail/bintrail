package pgcapture

import "testing"

// TestAckCommitted_AdvanceOnly pins the monotonicity of lastAcked: a stale or
// out-of-order ack must never regress the durable cursor (which would make the next
// standby update report a lower confirmed_flush_lsn). Internal test — it reads the
// unexported lastAcked directly; no live PostgreSQL needed.
func TestAckCommitted_AdvanceOnly(t *testing.T) {
	c := New(Config{}) // New opens no connections; lastAcked starts at 0
	if got := c.lastAcked.Load(); got != 0 {
		t.Fatalf("fresh Capturer: lastAcked=%d, want 0", got)
	}

	c.AckCommitted(100)
	if got := c.lastAcked.Load(); got != 100 {
		t.Fatalf("after Ack(100): lastAcked=%d, want 100", got)
	}

	c.AckCommitted(50) // stale / regressing — must be ignored
	if got := c.lastAcked.Load(); got != 100 {
		t.Errorf("after stale Ack(50): lastAcked=%d, want 100 (advance-only)", got)
	}

	c.AckCommitted(100) // equal — no-op
	if got := c.lastAcked.Load(); got != 100 {
		t.Errorf("after equal Ack(100): lastAcked=%d, want 100", got)
	}

	c.AckCommitted(150) // advances
	if got := c.lastAcked.Load(); got != 150 {
		t.Errorf("after Ack(150): lastAcked=%d, want 150", got)
	}
}
