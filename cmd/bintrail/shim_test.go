package main

import (
	"net"
	"testing"
	"time"
)

// TestIsLoopbackAddr locks in the security-relevant guard that
// determines whether the shim emits the "non-loopback bind" warning
// at startup. A regression that classified 0.0.0.0 as loopback would
// silently degrade the auth model.
func TestIsLoopbackAddr(t *testing.T) {
	cases := []struct {
		name string
		addr net.Addr
		want bool
	}{
		{"IPv4 loopback", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3308}, true},
		{"IPv4 loopback alt", &net.TCPAddr{IP: net.ParseIP("127.0.0.5"), Port: 3308}, true},
		{"IPv6 loopback", &net.TCPAddr{IP: net.ParseIP("::1"), Port: 3308}, true},
		{"unspecified IPv4 (0.0.0.0)", &net.TCPAddr{IP: net.IPv4zero, Port: 3308}, false},
		{"unspecified IPv6 (::)", &net.TCPAddr{IP: net.IPv6unspecified, Port: 3308}, false},
		{"private IPv4", &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 3308}, false},
		{"public IPv4", &net.TCPAddr{IP: net.ParseIP("8.8.8.8"), Port: 3308}, false},
		{"nil IP", &net.TCPAddr{IP: nil, Port: 3308}, false},
		{"non-TCP addr", &net.UnixAddr{Name: "/tmp/sock", Net: "unix"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isLoopbackAddr(tc.addr); got != tc.want {
				t.Errorf("isLoopbackAddr(%v) = %v, want %v", tc.addr, got, tc.want)
			}
		})
	}
}

// TestNextAcceptBackoff pins the doubling-with-cap behaviour. A
// regression here matters because the backoff is what stops a wedged
// listener from filling the log at ~10 lines/sec — and a buggy reset
// (e.g. always returning initial) would silently re-spin.
func TestNextAcceptBackoff(t *testing.T) {
	cases := []struct {
		name    string
		current time.Duration
		want    time.Duration
	}{
		{"zero seeds at initial", 0, initialAcceptBackoff},
		{"negative seeds at initial", -1, initialAcceptBackoff},
		{"100ms doubles to 200ms", 100 * time.Millisecond, 200 * time.Millisecond},
		{"200ms doubles to 400ms", 200 * time.Millisecond, 400 * time.Millisecond},
		{"2s doubles to 4s", 2 * time.Second, 4 * time.Second},
		{"4s doubles to cap", 4 * time.Second, maxAcceptBackoff},
		{"at cap stays at cap", maxAcceptBackoff, maxAcceptBackoff},
		{"above cap clamps to cap", 30 * time.Second, maxAcceptBackoff},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextAcceptBackoff(tc.current); got != tc.want {
				t.Errorf("nextAcceptBackoff(%v) = %v, want %v", tc.current, got, tc.want)
			}
		})
	}
}

// TestAcceptBackoffSequence walks the steady-state usage: starting
// from zero (post-success), each call models another consecutive
// failure. Verifies the cap is reached in a bounded number of steps —
// today's constants reach the 5s cap on the 7th failure (100, 200,
// 400, 800, 1600, 3200, 5000ms).
func TestAcceptBackoffSequence(t *testing.T) {
	var d time.Duration
	steps := 0
	for d < maxAcceptBackoff {
		d = nextAcceptBackoff(d)
		steps++
		if steps > 20 {
			t.Fatalf("backoff did not reach cap after %d steps; got %v", steps, d)
		}
	}
	if d != maxAcceptBackoff {
		t.Errorf("after %d steps, got %v; want exactly %v", steps, d, maxAcceptBackoff)
	}
}
