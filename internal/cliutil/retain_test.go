package cliutil

import (
	"testing"
	"time"
)

func TestParseRetain_days(t *testing.T) {
	d, err := ParseRetain("7d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 7*24*time.Hour {
		t.Errorf("expected 168h, got %v", d)
	}
}

func TestParseRetain_hours(t *testing.T) {
	d, err := ParseRetain("24h")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 24*time.Hour {
		t.Errorf("expected 24h, got %v", d)
	}
}

func TestParseRetain_largeDays(t *testing.T) {
	d, err := ParseRetain("365d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 365*24*time.Hour {
		t.Errorf("expected 365*24h, got %v", d)
	}
}

func TestParseRetain_invalid(t *testing.T) {
	// badDurations lives in interval_test.go and is deliberately shared: the
	// two entry points are allowed to disagree about UNITS and must not
	// disagree about how the NUMBER is read. This test used to carry its own
	// copy of that list, which is exactly the drift the shared
	// corpus exists to prevent.
	for _, c := range badDurations {
		if _, err := ParseRetain(c); err == nil {
			t.Errorf("expected error for %q, got nil", c)
		}
	}
}

func TestParseRetain_minimumHour(t *testing.T) {
	d, err := ParseRetain("1h")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != time.Hour {
		t.Errorf("expected 1h, got %v", d)
	}
}

func TestParseRetain_minimumDay(t *testing.T) {
	d, err := ParseRetain("1d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 24*time.Hour {
		t.Errorf("expected 24h, got %v", d)
	}
}
