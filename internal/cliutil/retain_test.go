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
	cases := []string{
		"",    // too short
		"d",   // no number
		"7x",  // unknown unit
		"7",   // no unit
		"-1d", // negative
		"0d",  // zero
		"0h",  // zero hours
	}
	for _, c := range cases {
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
