package main

import (
	"testing"

	"github.com/dbtrail/bintrail/internal/agent"
)

func TestExitCodeFor(t *testing.T) {
	tests := []struct {
		name     string
		reason   agent.FatalReason
		wantCode int
		wantMsg  string
	}{
		{"missing credentials", agent.FatalMissingCredentials, 64, "missing credentials — set --api-key or BINTRAIL_API_KEY"},
		{"invalid key", agent.FatalInvalidKey, 64, "invalid or revoked API key"},
		{"wrong tenant mode", agent.FatalWrongTenantMode, 64, "tenant is not in BYOS mode — WebSocket channel unavailable"},
		{"rate limited", agent.FatalRateLimited, 65, "agent rate-limited by server — contact support"},
		{"unknown reason falls back to 64", agent.NotFatal, 64, "agent rejected by server — fix credentials/config and restart manually"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, msg := exitCodeFor(tt.reason)
			if code != tt.wantCode {
				t.Errorf("code = %d, want %d", code, tt.wantCode)
			}
			if msg != tt.wantMsg {
				t.Errorf("msg = %q, want %q", msg, tt.wantMsg)
			}
		})
	}
}
