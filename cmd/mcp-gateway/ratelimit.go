package main

import (
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// RateLimiter tracks per-IP request counts in fixed one-minute windows.
// Expired entries are cleaned up periodically.
type RateLimiter struct {
	mu      sync.Mutex
	entries map[string]*rateLimitEntry
	window  time.Duration
	done    chan struct{}
}

type rateLimitEntry struct {
	count    int
	windowAt time.Time // start of the current window
}

// NewRateLimiter creates a rate limiter with the given window duration.
// Call Stop when done to release the cleanup goroutine.
func NewRateLimiter(window time.Duration) *RateLimiter {
	rl := &RateLimiter{
		entries: make(map[string]*rateLimitEntry),
		window:  window,
		done:    make(chan struct{}),
	}
	go rl.cleanup()
	return rl
}

// Allow checks whether the given key has remaining capacity. If so it
// increments the counter and returns true. Otherwise it returns false.
func (rl *RateLimiter) Allow(key string, limit int) bool {
	if limit <= 0 {
		return true // disabled
	}

	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()

	e, ok := rl.entries[key]
	if !ok || now.Sub(e.windowAt) >= rl.window {
		rl.entries[key] = &rateLimitEntry{count: 1, windowAt: now}
		return true
	}

	if e.count >= limit {
		return false
	}
	e.count++
	return true
}

// Stop shuts down the background cleanup goroutine.
func (rl *RateLimiter) Stop() {
	close(rl.done)
}

func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-rl.done:
			return
		case <-ticker.C:
			rl.evict()
		}
	}
}

func (rl *RateLimiter) evict() {
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for key, e := range rl.entries {
		if now.Sub(e.windowAt) >= 2*rl.window {
			delete(rl.entries, key)
		}
	}
}

// RateLimitMiddleware wraps an http.Handler with per-IP rate limiting.
// When the limit is exceeded, it returns 429 Too Many Requests with a
// Retry-After header.
func RateLimitMiddleware(rl *RateLimiter, limit int, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if limit <= 0 {
			next.ServeHTTP(w, r)
			return
		}

		ip := clientIP(r)
		if !rl.Allow(ip, limit) {
			retryAfter := int(rl.window.Seconds())
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			jsonError(w, "rate_limit_exceeded", "too many requests, try again later", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientIP extracts the client IP from the request. It prefers
// X-Forwarded-For (set by ALB/reverse proxy) and falls back to RemoteAddr.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// X-Forwarded-For can contain multiple IPs; the first is the client.
		for i := range len(xff) {
			if xff[i] == ',' {
				return xff[:i]
			}
		}
		return xff
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
