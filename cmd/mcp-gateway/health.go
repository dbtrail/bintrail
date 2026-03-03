package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// HealthChecker periodically probes backend /health endpoints and tracks
// whether each backend is healthy. The proxy consults this before routing.
//
// A backend is marked unhealthy after [threshold] consecutive failed checks
// and returns to healthy as soon as a check passes.
type HealthChecker struct {
	client    *http.Client
	interval  time.Duration
	threshold int // consecutive failures before marking unhealthy

	mu     sync.RWMutex
	status map[string]*backendStatus // keyed by backend URL
}

type backendStatus struct {
	healthy    bool
	failures   int
	lastCheck  time.Time
	lastError  string
}

// NewHealthChecker creates a health checker that probes backends every interval.
// If interval is 0, health checking is disabled (IsHealthy always returns true).
func NewHealthChecker(interval time.Duration, threshold int) *HealthChecker {
	if threshold <= 0 {
		threshold = 3
	}
	return &HealthChecker{
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		interval:  interval,
		threshold: threshold,
		status:    make(map[string]*backendStatus),
	}
}

// RegisterBackend adds a backend URL to be health-checked.
// Safe to call multiple times with the same URL.
func (h *HealthChecker) RegisterBackend(backend string) {
	if h.interval == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.status[backend]; !ok {
		h.status[backend] = &backendStatus{healthy: true}
	}
}

// IsHealthy returns whether a backend is considered healthy.
// Returns true if health checking is disabled or the backend is unknown.
func (h *HealthChecker) IsHealthy(backend string) bool {
	if h.interval == 0 {
		return true
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	s, ok := h.status[backend]
	if !ok {
		// Unknown backend — let the proxy try; it will fail with its own error.
		return true
	}
	return s.healthy
}

// Status returns a snapshot of all backend health states.
func (h *HealthChecker) Status() map[string]bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]bool, len(h.status))
	for url, s := range h.status {
		out[url] = s.healthy
	}
	return out
}

// Run starts the periodic health check loop. It blocks until ctx is cancelled.
func (h *HealthChecker) Run(ctx context.Context) {
	if h.interval == 0 {
		<-ctx.Done()
		return
	}

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	// Run an initial check immediately.
	h.checkAll(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.checkAll(ctx)
		}
	}
}

func (h *HealthChecker) checkAll(ctx context.Context) {
	h.mu.RLock()
	backends := make([]string, 0, len(h.status))
	for url := range h.status {
		backends = append(backends, url)
	}
	h.mu.RUnlock()

	for _, backend := range backends {
		h.checkOne(ctx, backend)
	}
}

func (h *HealthChecker) checkOne(ctx context.Context, backend string) {
	healthURL := healthEndpoint(backend)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		h.recordFailure(backend, fmt.Sprintf("build request: %v", err))
		return
	}

	resp, err := h.client.Do(req)
	if err != nil {
		h.recordFailure(backend, fmt.Sprintf("request failed: %v", err))
		return
	}
	resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		h.recordSuccess(backend)
	} else {
		h.recordFailure(backend, fmt.Sprintf("status %d", resp.StatusCode))
	}
}

func (h *HealthChecker) recordSuccess(backend string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.status[backend]
	if s == nil {
		return
	}
	wasUnhealthy := !s.healthy
	s.healthy = true
	s.failures = 0
	s.lastCheck = time.Now()
	s.lastError = ""
	if wasUnhealthy {
		slog.Info("backend recovered", "backend", backend)
	}
}

func (h *HealthChecker) recordFailure(backend string, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.status[backend]
	if s == nil {
		return
	}
	s.failures++
	s.lastCheck = time.Now()
	s.lastError = reason
	if s.failures >= h.threshold && s.healthy {
		s.healthy = false
		slog.Error("backend marked unhealthy",
			"backend", backend,
			"consecutive_failures", s.failures,
			"last_error", reason,
		)
	} else if !s.healthy {
		slog.Warn("backend still unhealthy",
			"backend", backend,
			"consecutive_failures", s.failures,
			"last_error", reason,
		)
	} else {
		slog.Warn("backend health check failed",
			"backend", backend,
			"consecutive_failures", s.failures,
			"threshold", h.threshold,
			"last_error", reason,
		)
	}
}

// healthEndpoint derives the health check URL from a backend URL.
// It replaces the path with /health (e.g. http://host:8080/mcp → http://host:8080/health).
func healthEndpoint(backend string) string {
	u, err := url.Parse(backend)
	if err != nil {
		return backend + "/health"
	}
	u.Path = "/health"
	u.RawQuery = ""
	return u.String()
}
