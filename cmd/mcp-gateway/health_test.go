package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthChecker_healthyBackend(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("expected /health, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	checker := NewHealthChecker(50*time.Millisecond, 2)
	checker.RegisterBackend(backend.URL + "/mcp")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Wait for at least one check.
	time.Sleep(100 * time.Millisecond)

	if !checker.IsHealthy(backend.URL + "/mcp") {
		t.Error("expected backend to be healthy")
	}
}

func TestHealthChecker_unhealthyBackend(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer backend.Close()

	checker := NewHealthChecker(50*time.Millisecond, 2)
	checker.RegisterBackend(backend.URL + "/mcp")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Wait for threshold failures (2 checks × 50ms + buffer).
	time.Sleep(200 * time.Millisecond)

	if checker.IsHealthy(backend.URL + "/mcp") {
		t.Error("expected backend to be unhealthy after consecutive failures")
	}
}

func TestHealthChecker_recovery(t *testing.T) {
	healthy := true
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthy {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer backend.Close()

	checker := NewHealthChecker(50*time.Millisecond, 2)
	checker.RegisterBackend(backend.URL + "/mcp")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Start healthy.
	time.Sleep(100 * time.Millisecond)
	if !checker.IsHealthy(backend.URL + "/mcp") {
		t.Fatal("expected healthy initially")
	}

	// Go unhealthy.
	healthy = false
	time.Sleep(200 * time.Millisecond)
	if checker.IsHealthy(backend.URL + "/mcp") {
		t.Fatal("expected unhealthy")
	}

	// Recover.
	healthy = true
	time.Sleep(200 * time.Millisecond)
	if !checker.IsHealthy(backend.URL + "/mcp") {
		t.Error("expected healthy after recovery")
	}
}

func TestHealthChecker_disabled(t *testing.T) {
	checker := NewHealthChecker(0, 3) // interval=0 disables

	// Should always return healthy when disabled.
	if !checker.IsHealthy("http://nonexistent:9999") {
		t.Error("disabled checker should always return healthy")
	}

	// RegisterBackend is a no-op.
	checker.RegisterBackend("http://nonexistent:9999")
	status := checker.Status()
	if len(status) != 0 {
		t.Errorf("expected empty status map, got %d entries", len(status))
	}
}

func TestHealthChecker_unknownBackend(t *testing.T) {
	checker := NewHealthChecker(30*time.Second, 3)

	// Unknown backends are assumed healthy (let the proxy handle errors).
	if !checker.IsHealthy("http://unknown:8080") {
		t.Error("unknown backend should be assumed healthy")
	}
}

func TestHealthChecker_unreachableBackend(t *testing.T) {
	// Use a port that's definitely not listening.
	checker := NewHealthChecker(50*time.Millisecond, 2)
	checker.RegisterBackend("http://127.0.0.1:1/mcp")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	time.Sleep(200 * time.Millisecond)

	if checker.IsHealthy("http://127.0.0.1:1/mcp") {
		t.Error("unreachable backend should be marked unhealthy")
	}
}

func TestHealthEndpoint(t *testing.T) {
	tests := []struct {
		backend string
		want    string
	}{
		{"http://host:8080/mcp", "http://host:8080/health"},
		{"http://host:8080", "http://host:8080/health"},
		{"http://10.0.1.5:8080/mcp", "http://10.0.1.5:8080/health"},
		{"https://backend.example.com/mcp", "https://backend.example.com/health"},
	}

	for _, tt := range tests {
		got := healthEndpoint(tt.backend)
		if got != tt.want {
			t.Errorf("healthEndpoint(%q) = %q, want %q", tt.backend, got, tt.want)
		}
	}
}

func TestHealthChecker_statusSnapshot(t *testing.T) {
	checker := NewHealthChecker(30*time.Second, 3)
	checker.RegisterBackend("http://a:8080")
	checker.RegisterBackend("http://b:8080")

	status := checker.Status()
	if len(status) != 2 {
		t.Fatalf("expected 2 backends, got %d", len(status))
	}
	// New backends start as healthy.
	if !status["http://a:8080"] || !status["http://b:8080"] {
		t.Error("new backends should start healthy")
	}
}
